#!/bin/bash

# Create results folder and output CSV
mkdir -p results
csv_file="results/chord_results.csv"

# Write CSV header
echo "nodes,requests,total_requests,successful_lookups,total_time_ms,avg_hops,theoretical_hops" > "$csv_file"

nodes_list=(5 10 20 50 100 1000 5000)
requests_list=(1 5 10 20 50 100 1000)

for nodes in "${nodes_list[@]}"
do
  for reqs in "${requests_list[@]}"
  do
    # Run the Gleam simulation and capture output
    output=$(gleam run "$nodes" "$reqs")
    
    # Extract values from output
    total_requests=$(echo "$output" | grep "Total requests:" | awk '{print $3}')
    successful_lookups=$(echo "$output" | grep "Successful lookups:" | awk '{print $3}')
    total_time=$(echo "$output" | grep "Total time:" | awk '{print $3}')
    avg_hops=$(echo "$output" | grep "Average hops:" | awk '{print $3}')
    theoretical_hops=$(echo "$output" | grep "Theoretical hops" | awk '{print $5}')
    
    # Append extracted values as a row in the CSV
    echo "$nodes,$reqs,$total_requests,$successful_lookups,$total_time,$avg_hops,$theoretical_hops" >> "$csv_file"
  done
done

echo "All tests complete. Results saved to $csv_file."
