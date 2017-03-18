package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.Graph;
import graph.Node;
import graph.NodesPerSecond;
import query.Query;

public class Cet extends Transaction {
	
	Query query;
	
	public Cet (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		super(str,d,time,mem);
		query = q;
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		latency.set(latency.get() + duration);
		done.countDown();
	}
	
	public void computeResults() {	

		Set<String> substream_ids = stream.substreams.keySet();		
		for (String substream_id : substream_ids) {					
		 
			// Construct the graph for each sub-stream
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);			
			Graph graph = new Graph();
			graph = graph.getCompleteGraphForPercentage(events, query);
			
			// Traverse the pointers from each last event in the graph
			NodesPerSecond last_nodes = graph.all_nodes.get(graph.all_nodes.size()-1);
			int count = traversePointers(last_nodes.nodes_per_second, 0);			
			System.out.println("Sub-stream id: " + substream_id + " with count " + count);
			
			memory.set(memory.get() + graph.nodeNumber * 12 + graph.edgeNumber * 4 + count);		
		}			
	}
	
	// BFS storing intermediate results in all nodes at the current level
	public static int traversePointers (ArrayList<Node> current_level, int count) { 
		
		// Array for recursive call of this method
		ArrayList<Node> next_level_array = new ArrayList<Node>();
			
		// Hash for quick lookup of saved nodes
		HashMap<Integer,Integer> next_level_hash = new HashMap<Integer,Integer>();
			
		for (Node this_node : current_level) {
				
			/*** Base case: Create the results for the first nodes ***/
			if (this_node.results.isEmpty()) {
				EventTrend new_trend = new EventTrend(this_node, this_node, this_node.toString());
				this_node.results.add(new_trend);				
			}
				
			/*** Recursive case: Copy results from the current node to its previous node and  
			 *** append this previous node to each copied result ***/
			if (!this_node.previous.isEmpty()) {			
					
				// System.out.println(this_node.event.id + ": " + this_node.previous.toString());
					
				for (Node next_node : this_node.previous) {
					
					for (EventTrend old_trend : this_node.results) {
						String new_seq = next_node.toString() + ";" + old_trend.sequence;
						EventTrend new_trend = new EventTrend(next_node, old_trend.last_node, new_seq);
						next_node.results.add(new_trend);					
					}														
					
					// Check that following is not in next_level
					if (!next_level_hash.containsKey(next_node.event.id)) {
						next_level_array.add(next_node); 
						next_level_hash.put(next_node.event.id,1);
					}
				}
				// Delete intermediate results
				this_node.results.clear();
			} else {
				// Count all results from a first node
				count += this_node.results.size();
				//System.out.print(this_node.resultsToString());					
			}
		}
					
		// Call this method recursively
		if (!next_level_array.isEmpty()) count += traversePointers(next_level_array, count);
			
		return count;
	}
}
