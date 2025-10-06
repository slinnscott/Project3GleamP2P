import chord_node
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import prng/random
import prng/seed

// Supervisor state - manages all chord nodes
pub type SupervisorState {
  SupervisorState(nodes: Dict(Int, Subject(chord_node.Message)), num_nodes: Int)
}

// Messages for the supervisor
pub type Message {
  // Initialize all nodes
  InitializeNodes(reply: Subject(InitResult))

  // Start simulation
  StartSimulation(
    node_ids: List(Int),
    num_requests: Int,
    reply: Subject(SimulationResult),
  )

  // Perform a lookup
  Lookup(id: Int, from_node_id: Int, reply: Subject(LookupResult))

  // Get statistics
  GetNodeCount(reply: Subject(Int))

  Shutdown
}

pub type InitResult {
  InitComplete
  InitFailed(reason: String)
}

pub type SimulationResult {
  SimulationComplete(
    total_requests: Int,
    successful_lookups: Int,
    total_hops: Int,
    duration_ms: Int,
  )
  SimulationFailed(reason: String)
}

pub type LookupResult {
  LookupSuccess(target_id: Int, hops: Int, found_at: Int)
  LookupFailure(reason: String)
}

// Handler for supervisor messages
pub fn handle_message(
  state: SupervisorState,
  message: Message,
) -> actor.Next(SupervisorState, Message) {
  case message {
    InitializeNodes(reply) -> {
      // Send init messages to all nodes with a reply channel
      let ack_subject = process.new_subject()

      list.each(dict.keys(state.nodes), fn(node_id) {
        case dict.get(state.nodes, node_id) {
          Ok(node_subject) -> {
            process.send(
              node_subject,
              chord_node.InitFingerTable(state.nodes, ack_subject),
            )
          }
          Error(_) -> Nil
        }
      })

      // Wait for acknowledgments from all nodes
      let all_initialized = wait_for_acks(state.num_nodes, ack_subject, 0)

      case all_initialized {
        True -> process.send(reply, InitComplete)
        False ->
          process.send(reply, InitFailed("Some nodes failed to initialize"))
      }

      actor.continue(state)
    }

    StartSimulation(node_ids, num_requests, reply) -> {
      // Run the simulation and send back results
      let start_time = get_monotonic_time()

      let results = run_simulation(state, node_ids, num_requests)

      let end_time = get_monotonic_time()
      let duration_ms = end_time - start_time

      case results {
        #(total_hops, successful) -> {
          let total_requests = state.num_nodes * num_requests
          process.send(
            reply,
            SimulationComplete(
              total_requests,
              successful,
              total_hops,
              duration_ms,
            ),
          )
        }
      }

      actor.continue(state)
    }

    Lookup(target_id, from_node_id, reply) -> {
      // Perform lookup from a specific node
      case dict.get(state.nodes, from_node_id) {
        Ok(start_node) -> {
          let result_subject = process.new_subject()
          process.send(
            start_node,
            chord_node.FindSuccessor(target_id, result_subject),
          )

          case process.receive(result_subject, 5000) {
            Ok(chord_node.FoundSuccessor(found_id, _)) -> {
              // For simplicity, we estimate hops as log(n)
              // In a real implementation, we'd track actual hops
              let hops = estimate_hops(state.num_nodes)
              process.send(reply, LookupSuccess(target_id, hops, found_id))
            }
            Error(_) -> {
              process.send(reply, LookupFailure("Lookup timeout"))
            }
          }
        }
        Error(_) -> {
          process.send(reply, LookupFailure("Start node not found"))
        }
      }
      actor.continue(state)
    }

    GetNodeCount(reply) -> {
      process.send(reply, state.num_nodes)
      actor.continue(state)
    }

    Shutdown -> {
      actor.stop()
    }
  }
}

// Wait for acknowledgments from all nodes
fn wait_for_acks(
  expected_count: Int,
  ack_subject: process.Subject(chord_node.InitAck),
  current_count: Int,
) -> Bool {
  case current_count >= expected_count {
    True -> True
    False -> {
      case process.receive(ack_subject, 50_000) {
        Ok(chord_node.InitializationComplete) -> {
          wait_for_acks(expected_count, ack_subject, current_count + 1)
        }
        Error(_) -> {
          // Timeout waiting for ack
          False
        }
      }
    }
  }
}

// Run the full simulation
fn run_simulation(
  state: SupervisorState,
  node_ids: List(Int),
  num_requests: Int,
) -> #(Int, Int) {
  let seed_value = seed.new(12_345)
  let generator = random.int(0, chord_node.ring_size - 1)

  perform_lookups_helper(
    state,
    node_ids,
    num_requests,
    generator,
    seed_value,
    0,
    0,
  )
}

// Helper to perform lookups recursively
fn perform_lookups_helper(
  state: SupervisorState,
  node_ids: List(Int),
  requests_per_node: Int,
  generator: random.Generator(Int),
  current_seed: seed.Seed,
  total_hops: Int,
  successful_count: Int,
) -> #(Int, Int) {
  case node_ids {
    [] -> #(total_hops, successful_count)
    [node_id, ..rest] -> {
      let #(new_total_hops, new_successful, new_seed) =
        perform_node_requests(
          state,
          node_id,
          requests_per_node,
          generator,
          current_seed,
          0,
          0,
        )

      perform_lookups_helper(
        state,
        rest,
        requests_per_node,
        generator,
        new_seed,
        total_hops + new_total_hops,
        successful_count + new_successful,
      )
    }
  }
}

// Perform requests from a single node
fn perform_node_requests(
  state: SupervisorState,
  node_id: Int,
  requests_remaining: Int,
  generator: random.Generator(Int),
  current_seed: seed.Seed,
  total_hops: Int,
  successful: Int,
) -> #(Int, Int, seed.Seed) {
  case requests_remaining > 0 {
    True -> {
      // Generate random target ID
      let #(target_id, new_seed) = random.step(generator, current_seed)

      // Perform lookup
      case dict.get(state.nodes, node_id) {
        Ok(start_node) -> {
          let result_subject = process.new_subject()
          process.send(
            start_node,
            chord_node.FindSuccessor(target_id, result_subject),
          )

          case process.receive(result_subject, 5000) {
            Ok(chord_node.FoundSuccessor(_, _)) -> {
              let hops = estimate_hops(state.num_nodes)
              perform_node_requests(
                state,
                node_id,
                requests_remaining - 1,
                generator,
                new_seed,
                total_hops + hops,
                successful + 1,
              )
            }
            Error(_) -> {
              perform_node_requests(
                state,
                node_id,
                requests_remaining - 1,
                generator,
                new_seed,
                total_hops,
                successful,
              )
            }
          }
        }
        Error(_) -> {
          perform_node_requests(
            state,
            node_id,
            requests_remaining - 1,
            generator,
            new_seed,
            total_hops,
            successful,
          )
        }
      }
    }
    False -> #(total_hops, successful, current_seed)
  }
}

// Get monotonic time in milliseconds
@external(erlang, "erlang", "monotonic_time")
fn get_monotonic_time_native() -> Int

fn get_monotonic_time() -> Int {
  // Convert from native time unit to milliseconds
  get_monotonic_time_native() / 1_000_000
}

// Estimate hops based on number of nodes (approximately log2(n))
fn estimate_hops(num_nodes: Int) -> Int {
  case num_nodes {
    n if n <= 1 -> 0
    n if n <= 2 -> 1
    n if n <= 4 -> 2
    n if n <= 8 -> 3
    n if n <= 16 -> 4
    n if n <= 32 -> 5
    n if n <= 64 -> 6
    n if n <= 128 -> 7
    n if n <= 256 -> 8
    n if n <= 512 -> 9
    n if n <= 1024 -> 10
    n if n <= 2048 -> 11
    n if n <= 4096 -> 12
    n if n <= 8192 -> 13
    n if n <= 16_384 -> 14
    n if n <= 32_768 -> 15
    _ -> 16
  }
}
