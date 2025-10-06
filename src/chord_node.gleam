import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor

// Chord protocol constants
pub const m = 16

// 16-bit identifier space (0 to 65535)
pub const ring_size = 65_536

// 2^16

// State for a chord node
pub type ChordNodeState {
  ChordNodeState(
    id: Int,
    successor: Option(Subject(Message)),
    predecessor: Option(Subject(Message)),
    finger_table: Dict(Int, Subject(Message)),
    node_registry: Dict(Int, Subject(Message)),
    // Map of all node IDs to their subjects
  )
}

// Messages that chord nodes can receive
pub type Message {
  // Lookup operations
  FindSuccessor(id: Int, reply: Subject(FindSuccessorResponse))
  FindPredecessor(id: Int, reply: Subject(Int))
  ClosestPrecedingFinger(id: Int, reply: Subject(Option(Subject(Message))))

  // Node information
  GetId(reply: Subject(Int))
  GetSuccessor(reply: Subject(Option(Subject(Message))))
  SetSuccessor(successor: Subject(Message))
  SetPredecessor(predecessor: Subject(Message))

  // Finger table operations
  InitFingerTable(nodes: Dict(Int, Subject(Message)), reply: Subject(InitAck))
  GetFingerEntry(index: Int, reply: Subject(Option(Subject(Message))))

  // Stabilization (not required for basic implementation)
  Notify(node: Subject(Message))
  Stabilize
}

pub type FindSuccessorResponse {
  FoundSuccessor(id: Int, subject: Subject(Message))
}

pub type InitAck {
  InitializationComplete
}

// Helper function to check if a value is in the range (start, end] on the ring
fn in_range(value: Int, start: Int, end: Int) -> Bool {
  case start < end {
    True -> value > start && value <= end
    False -> value > start || value <= end
  }
}

// Helper function to check if a value is in the range (start, end) on the ring
fn in_range_exclusive(value: Int, start: Int, end: Int) -> Bool {
  case start < end {
    True -> value > start && value < end
    False -> value > start || value < end
  }
}

// Create a new chord node state
pub fn new_state(id: Int, nodes: Dict(Int, Subject(Message))) -> ChordNodeState {
  ChordNodeState(
    id: id,
    successor: None,
    predecessor: None,
    finger_table: dict.new(),
    node_registry: nodes,
  )
}

// Handler for chord node messages
pub fn handle_message(
  state: ChordNodeState,
  message: Message,
) -> actor.Next(ChordNodeState, Message) {
  case message {
    // Get this node's ID
    GetId(reply) -> {
      process.send(reply, state.id)
      actor.continue(state)
    }

    // Get this node's successor
    GetSuccessor(reply) -> {
      process.send(reply, state.successor)
      actor.continue(state)
    }

    // Set this node's successor
    SetSuccessor(successor) -> {
      actor.continue(ChordNodeState(..state, successor: Some(successor)))
    }

    // Set this node's predecessor
    SetPredecessor(predecessor) -> {
      actor.continue(ChordNodeState(..state, predecessor: Some(predecessor)))
    }

    // Find the successor of a given ID
    FindSuccessor(id, reply) -> {
      case state.successor {
        Some(succ) -> {
          // Get successor's ID
          let succ_id_subject = process.new_subject()
          process.send(succ, GetId(succ_id_subject))
          let succ_id = process.receive(succ_id_subject, 1000)

          case succ_id {
            Ok(succ_id_value) -> {
              // If id is in (n, successor], successor is the answer
              case in_range(id, state.id, succ_id_value) {
                True -> {
                  process.send(reply, FoundSuccessor(succ_id_value, succ))
                  actor.continue(state)
                }
                False -> {
                  // Find the closest preceding finger and forward
                  let closest_subject = process.new_subject()
                  process.send(
                    succ,
                    ClosestPrecedingFinger(id, closest_subject),
                  )

                  case process.receive(closest_subject, 1000) {
                    Ok(Some(closest_node)) -> {
                      // Forward the request
                      process.send(closest_node, FindSuccessor(id, reply))
                      actor.continue(state)
                    }
                    _ -> {
                      // Return successor as fallback
                      process.send(reply, FoundSuccessor(succ_id_value, succ))
                      actor.continue(state)
                    }
                  }
                }
              }
            }
            Error(_) -> {
              // Timeout, just return the successor
              process.send(reply, FoundSuccessor(state.id, succ))
              actor.continue(state)
            }
          }
        }
        None -> {
          // No successor, return self
          process.send(reply, FoundSuccessor(state.id, process.new_subject()))
          actor.continue(state)
        }
      }
    }

    // Find the closest preceding finger for a given ID
    ClosestPrecedingFinger(id, reply) -> {
      // Search finger table from highest to lowest
      let closest = find_closest_finger(state.finger_table, id, state.id, m - 1)
      process.send(reply, closest)
      actor.continue(state)
    }

    // Initialize finger table
    InitFingerTable(nodes, reply) -> {
      let new_state = ChordNodeState(..state, node_registry: nodes)
      let finger_table = build_finger_table(state.id, nodes)

      // Set successor to first finger table entry
      let successor = case dict.get(finger_table, 0) {
        Ok(node) -> Some(node)
        Error(_) -> None
      }

      // Send acknowledgment that initialization is complete
      process.send(reply, InitializationComplete)

      actor.continue(
        ChordNodeState(
          ..new_state,
          finger_table: finger_table,
          successor: successor,
        ),
      )
    }

    // Get a specific finger table entry
    GetFingerEntry(index, reply) -> {
      let entry = dict.get(state.finger_table, index)
      case entry {
        Ok(node) -> process.send(reply, Some(node))
        Error(_) -> process.send(reply, None)
      }
      actor.continue(state)
    }

    FindPredecessor(_, _) | Notify(_) | Stabilize -> {
      // Not implemented for basic simulation
      actor.continue(state)
    }
  }
}

// Find the closest preceding finger in the finger table
fn find_closest_finger(
  finger_table: Dict(Int, Subject(Message)),
  target_id: Int,
  node_id: Int,
  index: Int,
) -> Option(Subject(Message)) {
  case index >= 0 {
    True -> {
      case dict.get(finger_table, index) {
        Ok(finger_node) -> {
          let finger_id_subject = process.new_subject()
          process.send(finger_node, GetId(finger_id_subject))

          case process.receive(finger_id_subject, 100) {
            Ok(finger_id) -> {
              case in_range_exclusive(finger_id, node_id, target_id) {
                True -> Some(finger_node)
                False ->
                  find_closest_finger(
                    finger_table,
                    target_id,
                    node_id,
                    index - 1,
                  )
              }
            }
            Error(_) ->
              find_closest_finger(finger_table, target_id, node_id, index - 1)
          }
        }
        Error(_) ->
          find_closest_finger(finger_table, target_id, node_id, index - 1)
      }
    }
    False -> None
  }
}

// Build finger table for a node
fn build_finger_table(
  node_id: Int,
  nodes: Dict(Int, Subject(Message)),
) -> Dict(Int, Subject(Message)) {
  let node_ids = dict.keys(nodes) |> list.sort(int.compare)

  list.range(0, m - 1)
  |> list.fold(dict.new(), fn(table, i) {
    let start = { node_id + pow2(i) } % ring_size

    // Find the successor of start
    case find_successor_in_list(start, node_ids, nodes) {
      Some(successor) -> dict.insert(table, i, successor)
      None -> table
    }
  })
}

// Find successor in a sorted list of node IDs
fn find_successor_in_list(
  id: Int,
  node_ids: List(Int),
  nodes: Dict(Int, Subject(Message)),
) -> Option(Subject(Message)) {
  // Find the first node >= id, wrapping around if necessary
  let successor_id = case list.find(node_ids, fn(nid) { nid >= id }) {
    Ok(nid) -> nid
    Error(_) -> {
      // Wrap around to first node
      case list.first(node_ids) {
        Ok(nid) -> nid
        Error(_) -> id
      }
    }
  }

  dict.get(nodes, successor_id) |> option.from_result
}

// Calculate 2^i
fn pow2(i: Int) -> Int {
  case i {
    0 -> 1
    _ -> 2 * pow2(i - 1)
  }
}
