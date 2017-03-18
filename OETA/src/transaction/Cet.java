package transaction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import query.*;

public class Cet extends Transaction {
	
	Query query;
	public BigInteger final_count;
	
	public Cet (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		super(str,d,time,mem);
		query = q;
		final_count = BigInteger.ZERO;
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
			memory.set(memory.get() + graph.nodeNumber * 12 + graph.edgeNumber * 4);
			
			// Traverse the pointers from each event in the graph
			NodesPerSecond nodes = graph.all_nodes.get(graph.all_nodes.size()-1);		
			final_count = traversePointers(nodes.nodes_per_second);
			
			System.out.println("Sub-stream id: " + substream_id + " with count " + final_count.intValue());
						
			final_count = BigInteger.ZERO;
		}			
	}
	
	// BFS storing intermediate results in all nodes at the current level
	public BigInteger traversePointers (ArrayList<Node> current_level) { 
		
		// Array for recursive call of this method
		ArrayList<Node> next_level_array = new ArrayList<Node>();
			
		// Hash for quick lookup of saved nodes
		HashMap<Integer,Integer> next_level_hash = new HashMap<Integer,Integer>();
			
		for (Node this_node : current_level) {
				
			/*** Base case: Create the results for the first nodes ***/
			if (this_node.results.isEmpty()) {
				
				EventTrend new_trend = new EventTrend(this_node, this_node, this_node.toString());
				this_node.results.add(new_trend);
				System.out.println(new_trend.sequence);
				final_count = final_count.add(BigInteger.ONE);
				memory.set(memory.get() + new_trend.getEventNumber() * 4);
				
			} 				
			/*** Recursive case: Copy results from the current node to its previous node and  
			 *** append this previous node to each copied result ***/			
			if (!this_node.previous.isEmpty()) {
				for (Node next_node : this_node.previous) {
					
					for (EventTrend old_trend : this_node.results) {
						String new_seq = next_node.toString() + ";" + old_trend.sequence;
						EventTrend new_trend = new EventTrend(next_node, old_trend.last_node, new_seq);
						next_node.results.add(new_trend);
						System.out.println(new_trend.sequence);
						final_count = final_count.add(BigInteger.ONE);
						memory.set(memory.get() + new_trend.getEventNumber() * 4);
					}														
					
					// Check that following is not in next_level
					if (!next_level_hash.containsKey(next_node.event.id)) {
						next_level_array.add(next_node); 
						next_level_hash.put(next_node.event.id,1);
					}
				}
				// Delete intermediate results
				this_node.results.clear();
			} 		
		}
					
		// Call this method recursively
		if (!next_level_array.isEmpty()) final_count = traversePointers(next_level_array);
			
		return final_count;
	}
}
