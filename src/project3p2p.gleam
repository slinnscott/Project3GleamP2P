import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Option}
import gleam/otp/actor
import gleam/otp/supervisor
import gleam/otp/timer
import gleam/string

// Chord protocol implementation using actor model

// Core data types
pub type NodeId =
  Int

pub type Key =
  Int

pub type Value =
  String

pub type Finger {
  Finger(start: Int, node: Option(NodeId))
}

pub type FingerTable =
  List(Finger)

pub type NodeState {
  NodeState(
    id: NodeId,
    successor: Option(NodeId),
    predecessor: Option(NodeId),
    finger_table: FingerTable,
    keys: List(#(Key, Value)),
    m: Int,
    // number of bits for identifiers
  )
}

// Message types for actor communication
pub type Message {
  FindSuccessor(key: Key, requester: process.Pid)
  FindPredecessor(key: Key, requester: process.Pid)
  Notify(node_id: NodeId)
  GetPredecessor(requester: process.Pid)
  SetPredecessor(node_id: Option(NodeId))
  SetSuccessor(node_id: Option(NodeId))
  LookupKey(key: Key, requester: process.Pid)
  StoreKey(key: Key, value: Value, requester: process.Pid)
  GetKey(key: Key, requester: process.Pid)
  Stabilize
  FixFingers
  HopCount(count: Int)
}

// Response types
pub type Response {
  Successor(node_id: NodeId)
  Predecessor(node_id: Option(NodeId))
  KeyValue(key: Key, value: Option(Value))
  Stored
  Hops(count: Int)
}

// Chord algorithm implementations
pub fn find_successor(state: NodeState, key: Key) -> NodeId {
  case state.successor {
    Ok(successor) -> {
      case is_between(state.id, key, successor, True) {
        True -> successor
        False -> {
          let _closest = closest_preceding_finger(state, key)
          // In a real implementation, this would send a message to the closest node
          // For now, we'll return the successor as a fallback
          successor
        }
      }
    }
    Error(_) -> state.id
  }
}

pub fn find_predecessor(state: NodeState, key: Key) -> NodeId {
  case state.predecessor {
    Ok(predecessor) -> {
      case is_between(predecessor, key, state.id, False) {
        True -> predecessor
        False -> {
          // In a real implementation, this would send a message to find the predecessor
          state.id
        }
      }
    }
    Error(_) -> state.id
  }
}

pub fn closest_preceding_finger(state: NodeState, key: Key) -> NodeId {
  closest_rec(state.finger_table, state.id, key)
}

fn closest_rec(fingers: List(Finger), best: NodeId, key: Key) -> NodeId {
  case fingers {
    [] -> best
    [Finger(_start, Ok(node)), ..rest] -> {
      case is_between(best, node, key, False) {
        True -> closest_rec(rest, node, key)
        False -> closest_rec(rest, best, key)
      }
    }
    [Finger(_, Error(_)), ..rest] -> closest_rec(rest, best, key)
  }
}

pub fn is_between(start: Int, middle: Int, end: Int, inclusive: Bool) -> Bool {
  case start < end, inclusive {
    True, True -> start < middle && middle <= end
    True, False -> start < middle && middle < end
    False, True -> start < middle || middle <= end
    False, False -> start < middle || middle < end
  }
}

pub fn create_finger_table(node_id: NodeId, m: Int) -> FingerTable {
  create_fingers(m, [], node_id, m)
}

fn create_fingers(
  i: Int,
  acc: List(Finger),
  node_id: NodeId,
  m: Int,
) -> List(Finger) {
  case i {
    0 -> acc
    _ -> {
      let start = int.rem(node_id + int.power(2, i - 1), int.power(2, m))
      create_fingers(i - 1, [Finger(start, Error(Nil)), ..acc], node_id, m)
    }
  }
}

// Actor implementation
pub fn node_actor_init() -> NodeState {
  NodeState(
    id: 0,
    successor: Error(Nil),
    predecessor: Error(Nil),
    finger_table: [],
    keys: [],
    m: 32,
  )
}

pub fn node_actor_handle(
  state: NodeState,
  message: Message,
) -> #(NodeState, List(actor.Message(Response))) {
  case message {
    FindSuccessor(key, requester) -> {
      let successor_id = find_successor(state, key)
      let response = actor.Message(requester, Successor(successor_id))
      #(state, [response])
    }

    FindPredecessor(key, requester) -> {
      let predecessor_id = find_predecessor(state, key)
      let response = actor.Message(requester, Predecessor(Ok(predecessor_id)))
      #(state, [response])
    }

    Notify(node_id) -> {
      case state.predecessor {
        Error(_) -> {
          let new_state = NodeState(..state, predecessor: Ok(node_id))
          #(new_state, [])
        }
        Ok(current_predecessor) -> {
          case is_between(current_predecessor, node_id, state.id, False) {
            True -> {
              let new_state = NodeState(..state, predecessor: Ok(node_id))
              #(new_state, [])
            }
            False -> #(state, [])
          }
        }
      }
    }

    GetPredecessor(requester) -> {
      let response = actor.Message(requester, Predecessor(state.predecessor))
      #(state, [response])
    }

    SetPredecessor(node_id) -> {
      let new_state = NodeState(..state, predecessor: node_id)
      #(new_state, [])
    }

    SetSuccessor(node_id) -> {
      let new_state = NodeState(..state, successor: node_id)
      #(new_state, [])
    }

    LookupKey(key, requester) -> {
      let _successor_id = find_successor(state, key)
      // In a real implementation, this would forward the request to the successor
      // For now, we'll just return the key if we have it
      let value =
        list.find(state.keys, fn(kv) {
          case kv {
            #(k, _) -> k == key
          }
        })
      let response = actor.Message(requester, KeyValue(key, value))
      #(state, [response])
    }

    StoreKey(key, value, requester) -> {
      let _successor_id = find_successor(state, key)
      // In a real implementation, this would forward to the successor
      // For now, we'll store it locally
      let new_keys = [#(key, value), ..state.keys]
      let new_state = NodeState(..state, keys: new_keys)
      let response = actor.Message(requester, Stored)
      #(new_state, [response])
    }

    GetKey(key, requester) -> {
      let value =
        list.find(state.keys, fn(kv) {
          case kv {
            #(k, _) -> k == key
          }
        })
      let response = actor.Message(requester, KeyValue(key, value))
      #(state, [response])
    }

    Stabilize -> {
      // Implement stabilization protocol
      #(state, [])
    }

    FixFingers -> {
      // Implement finger table fix
      #(state, [])
    }

    HopCount(_count) -> {
      // Handle hop counting
      #(state, [])
    }
  }
}

pub fn main() -> Nil {
  io.println("Chord P2P Protocol Implementation")
  io.println("Usage: gleam run numNodes numRequests")
}
