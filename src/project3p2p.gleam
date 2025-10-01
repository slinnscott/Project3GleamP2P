import argv
import gleam/int
import gleam/io
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
  Finger(start: Int, node: NodeId)
}

pub type FingerTable =
  List(Finger)

pub type NodeState {
  NodeState(
    id: NodeId,
    successor: NodeId,
    predecessor: NodeId,
    finger_table: FingerTable,
    keys: List(#(Key, Value)),
    m: Int,
    // number of bits for identifiers
  )
}

// Chord algorithm implementations
pub fn find_successor(state: NodeState, key: Key) -> NodeId {
  case is_between(state.id, key, state.successor, True) {
    True -> state.successor
    False -> {
      let _closest = closest_preceding_finger(state, key)
      // In a real implementation, this would send a message to the closest node
      // For now, we'll return the successor as a fallback
      state.successor
    }
  }
}

pub fn find_predecessor(state: NodeState, key: Key) -> NodeId {
  case is_between(state.predecessor, key, state.id, False) {
    True -> state.predecessor
    False -> {
      // In a real implementation, this would send a message to find the predecessor
      state.id
    }
  }
}

pub fn closest_preceding_finger(state: NodeState, key: Key) -> NodeId {
  closest_rec(state.finger_table, state.id, key)
}

fn closest_rec(fingers: List(Finger), best: NodeId, key: Key) -> NodeId {
  case fingers {
    [] -> best
    [Finger(_start, node), ..rest] -> {
      case is_between(best, node, key, False) {
        True -> closest_rec(rest, node, key)
        False -> closest_rec(rest, best, key)
      }
    }
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
      let power_2_i_minus_1 = power_of_2(i - 1)
      let power_2_m = power_of_2(m)
      let start = case int.remainder(node_id + power_2_i_minus_1, power_2_m) {
        Ok(value) -> value
        Error(_) -> 0
      }
      create_fingers(i - 1, [Finger(start, node_id), ..acc], node_id, m)
    }
  }
}

fn power_of_2(n: Int) -> Int {
  case n {
    0 -> 1
    1 -> 2
    2 -> 4
    3 -> 8
    4 -> 16
    5 -> 32
    6 -> 64
    7 -> 128
    8 -> 256
    9 -> 512
    10 -> 1024
    11 -> 2048
    12 -> 4096
    13 -> 8192
    14 -> 16_384
    15 -> 32_768
    16 -> 65_536
    17 -> 131_072
    18 -> 262_144
    19 -> 524_288
    20 -> 1_048_576
    21 -> 2_097_152
    22 -> 4_194_304
    23 -> 8_388_608
    24 -> 16_777_216
    25 -> 33_554_432
    26 -> 67_108_864
    27 -> 134_217_728
    28 -> 268_435_456
    29 -> 536_870_912
    30 -> 1_073_741_824
    31 -> 2_147_483_648
    32 -> 4_294_967_296
    _ -> 1
  }
}

// Simple node implementation for testing
pub fn create_node(id: NodeId, m: Int) -> NodeState {
  NodeState(
    id: id,
    successor: id,
    predecessor: id,
    finger_table: create_finger_table(id, m),
    keys: [],
    m: m,
  )
}

// Simulate a simple Chord ring for testing
pub fn simulate_chord_ring(num_nodes: Int, num_requests: Int) -> Nil {
  let m = 32
  let nodes = create_test_nodes(num_nodes, m)
  let total_hops = simulate_requests(nodes, num_requests, m)
  let average_hops =
    int.to_float(total_hops) /. int.to_float(num_requests * num_nodes)

  io.println("Chord P2P Protocol Simulation Results:")
  io.println("Number of nodes: " <> int.to_string(num_nodes))
  io.println("Number of requests per node: " <> int.to_string(num_requests))
  io.println("Total requests: " <> int.to_string(num_requests * num_nodes))
  io.println("Total hops: " <> int.to_string(total_hops))
  io.println("Average hops per request: " <> string.inspect(average_hops))
}

fn create_test_nodes(num_nodes: Int, m: Int) -> List(NodeState) {
  create_nodes(num_nodes, [], m)
}

fn create_nodes(i: Int, acc: List(NodeState), m: Int) -> List(NodeState) {
  case i {
    0 -> acc
    _ -> {
      let power_2_m = power_of_2(m)
      let node_id = case int.remainder(i * 1000, power_2_m) {
        Ok(value) -> value
        Error(_) -> 0
      }
      let node = create_node(node_id, m)
      create_nodes(i - 1, [node, ..acc], m)
    }
  }
}

fn simulate_requests(nodes: List(NodeState), num_requests: Int, m: Int) -> Int {
  simulate_all_nodes(nodes, 0, num_requests, m)
}

fn simulate_node_requests(
  node: NodeState,
  remaining_requests: Int,
  total_hops: Int,
  m: Int,
  all_nodes: List(NodeState),
) -> Int {
  case remaining_requests {
    0 -> total_hops
    _ -> {
      let power_2_m = power_of_2(m)
      let key = case int.remainder(remaining_requests * 500, power_2_m) {
        Ok(value) -> value
        Error(_) -> 0
      }
      let hops = simulate_key_lookup(node, key, all_nodes)
      simulate_node_requests(
        node,
        remaining_requests - 1,
        total_hops + hops,
        m,
        all_nodes,
      )
    }
  }
}

fn simulate_all_nodes(
  nodes: List(NodeState),
  total_hops: Int,
  num_requests: Int,
  m: Int,
) -> Int {
  case nodes {
    [] -> total_hops
    [node, ..rest] -> {
      let node_hops = simulate_node_requests(node, num_requests, 0, m, nodes)
      simulate_all_nodes(rest, total_hops + node_hops, num_requests, m)
    }
  }
}

fn simulate_key_lookup(
  start_node: NodeState,
  key: Key,
  all_nodes: List(NodeState),
) -> Int {
  // Simple simulation: count hops based on the distance in the identifier space
  let target_node = find_node_for_key(key, all_nodes)
  let distance = calculate_distance(start_node.id, target_node.id)

  // Estimate hops as log2 of the distance (simplified Chord behavior)
  case distance {
    0 -> 0
    _ -> int.max(1, log2_approx(distance))
  }
}

fn log2_approx(n: Int) -> Int {
  case n {
    0 -> 0
    1 -> 0
    2 -> 1
    4 -> 2
    8 -> 3
    16 -> 4
    32 -> 5
    64 -> 6
    128 -> 7
    256 -> 8
    512 -> 9
    1024 -> 10
    2048 -> 11
    4096 -> 12
    8192 -> 13
    16_384 -> 14
    32_768 -> 15
    65_536 -> 16
    131_072 -> 17
    262_144 -> 18
    524_288 -> 19
    1_048_576 -> 20
    2_097_152 -> 21
    4_194_304 -> 22
    8_388_608 -> 23
    16_777_216 -> 24
    33_554_432 -> 25
    67_108_864 -> 26
    134_217_728 -> 27
    268_435_456 -> 28
    536_870_912 -> 29
    1_073_741_824 -> 30
    2_147_483_648 -> 31
    _ -> 32
  }
}

fn find_node_for_key(key: Key, nodes: List(NodeState)) -> NodeState {
  case nodes {
    [] ->
      NodeState(
        id: 0,
        successor: 0,
        predecessor: 0,
        finger_table: [],
        keys: [],
        m: 32,
      )
    [first, ..rest] -> find_closest(rest, first, key)
  }
}

fn find_closest(nodes: List(NodeState), best: NodeState, key: Key) -> NodeState {
  case nodes {
    [] -> best
    [node, ..rest] -> {
      case is_between(best.id, key, node.id, True) {
        True -> find_closest(rest, node, key)
        False -> find_closest(rest, best, key)
      }
    }
  }
}

fn calculate_distance(start: Int, end: Int) -> Int {
  case start <= end {
    True -> end - start
    False -> { power_of_2(32) - start } + end
  }
}

pub fn main() -> Nil {
  let args = argv.load()

  case args {
    argv.Argv(_, _, arguments) -> {
      case arguments {
        [num_nodes_str, num_requests_str] -> {
          case int.parse(num_nodes_str), int.parse(num_requests_str) {
            Ok(num_nodes), Ok(num_requests) -> {
              simulate_chord_ring(num_nodes, num_requests)
            }
            _, _ -> {
              io.println(
                "Error: Invalid arguments. Please provide two integers.",
              )
              io.println("Usage: gleam run numNodes numRequests")
            }
          }
        }
        _ -> {
          io.println("Error: Expected exactly 2 arguments.")
          io.println("Usage: gleam run numNodes numRequests")
          io.println("Example: gleam run 10 5")
        }
      }
    }
  }
}
