import argv
import chord_node
import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import supervisor_node

// Generate unique node IDs for the Chord ring
pub fn generate_node_ids(num_nodes: Int) -> List(Int) {
  // Generate evenly distributed IDs around the ring
  list.range(0, num_nodes - 1)
  |> list.map(fn(i) {
    // Distribute nodes evenly around the ring
    { i * chord_node.ring_size } / num_nodes
  })
}

// Create chord nodes as actors
pub fn create_nodes(
  node_ids: List(Int),
) -> dict.Dict(Int, process.Subject(chord_node.Message)) {
  list.fold(node_ids, dict.new(), fn(acc, node_id) {
    // Create initial state
    let initial_state = chord_node.new_state(node_id, dict.new())

    // Start the actor using builder pattern
    case
      actor.new(initial_state)
      |> actor.on_message(chord_node.handle_message)
      |> actor.start()
    {
      Ok(started) -> {
        dict.insert(acc, node_id, started.data)
      }
      Error(_) -> {
        io.println("Error starting node " <> int.to_string(node_id))
        acc
      }
    }
  })
}

// Simulate the Chord protocol
pub fn simulate_chord_ring(num_nodes: Int, num_requests: Int) -> Nil {
  io.println("\n=== Chord Protocol Simulation ===")
  io.println("Nodes: " <> int.to_string(num_nodes))
  io.println("Requests per node: " <> int.to_string(num_requests))
  io.println("Ring size: " <> int.to_string(chord_node.ring_size))
  io.println("")

  // Generate node IDs
  io.println("Generating node IDs...")
  let node_ids = generate_node_ids(num_nodes)

  // Create nodes
  io.println("Creating chord nodes...")
  let nodes = create_nodes(node_ids)

  case dict.size(nodes) == num_nodes {
    True -> {
      io.println(
        "Successfully created " <> int.to_string(num_nodes) <> " nodes",
      )

      // Create supervisor
      io.println("Creating supervisor...")
      let supervisor_state = supervisor_node.SupervisorState(nodes, num_nodes)

      case
        actor.new(supervisor_state)
        |> actor.on_message(supervisor_node.handle_message)
        |> actor.start()
      {
        Ok(started) -> {
          let supervisor = started.data

          // Initialize all nodes (build finger tables)
          io.println("Initializing finger tables...")
          let init_reply = process.new_subject()
          process.send(supervisor, supervisor_node.InitializeNodes(init_reply))

          // Wait for initialization to complete
          case process.receive(init_reply, 90_000) {
            Ok(supervisor_node.InitComplete) -> {
              io.println("Finger tables initialized")
              io.println("")

              // Start simulation
              io.println("Performing lookups...")
              let sim_reply = process.new_subject()
              process.send(
                supervisor,
                supervisor_node.StartSimulation(
                  node_ids,
                  num_requests,
                  sim_reply,
                ),
              )

              // Wait for simulation to complete
              case process.receive(sim_reply, 180_000) {
                Ok(supervisor_node.SimulationComplete(
                  total_requests,
                  successful,
                  total_hops,
                  duration_ms,
                )) -> {
                  print_results(
                    total_requests,
                    successful,
                    total_hops,
                    duration_ms,
                    num_nodes,
                  )
                }
                Ok(supervisor_node.SimulationFailed(reason)) -> {
                  io.println("Simulation failed: " <> reason)
                }
                Error(_) -> {
                  io.println("Timeout waiting for simulation to complete")
                }
              }

              // Cleanup
              process.send(supervisor, supervisor_node.Shutdown)
            }
            Ok(supervisor_node.InitFailed(reason)) -> {
              io.println("Initialization failed: " <> reason)
            }
            Error(_) -> {
              io.println("Timeout waiting for initialization")
            }
          }
        }
        Error(_) -> {
          io.println("Error: Failed to start supervisor")
        }
      }
    }
    False -> {
      io.println("Error: Failed to create all nodes")
    }
  }
}

// Print simulation results
fn print_results(
  total_requests: Int,
  successful_lookups: Int,
  total_hops: Int,
  duration_ms: Int,
  num_nodes: Int,
) -> Nil {
  // Calculate statistics
  let avg_hops = case successful_lookups > 0 {
    True -> int.to_float(total_hops) /. int.to_float(successful_lookups)
    False -> 0.0
  }

  // Theoretical average hops for Chord is O(log N)
  let theoretical_hops = calculate_log2(num_nodes)

  // Print results
  io.println("\n=== Results ===")
  io.println("Total requests: " <> int.to_string(total_requests))
  io.println("Successful lookups: " <> int.to_string(successful_lookups))
  io.println("Total time: " <> int.to_string(duration_ms) <> " ms")
  io.println("Average hops: " <> float.to_string(avg_hops))
  io.println("Theoretical hops (log2 N): " <> float.to_string(theoretical_hops))
  io.println("")

  // Show that the protocol scales logarithmically
  case avg_hops <=. theoretical_hops *. 1.5 {
    True -> io.println("✓ Performance scales logarithmically with network size")
    False -> io.println("⚠ Performance may not be optimal")
  }

  io.println("Simulation complete.")
}

// Calculate log2 of a number
fn calculate_log2(n: Int) -> Float {
  case n {
    1 -> 0.0
    _ -> 1.0 +. calculate_log2(n / 2)
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
              // Validate inputs
              case num_nodes > 0 && num_requests > 0 {
                True -> simulate_chord_ring(num_nodes, num_requests)
                False -> {
                  io.println("Error: Both arguments must be positive integers")
                  io.println("Usage: gleam run numNodes numRequests")
                }
              }
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
