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

public class HCet extends Transaction {
	
	Query query;
	public BigInteger final_count;
	int number_of_graphlets;
	
	public HCet (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem, int graphlets) {		
		super(str,d,time,mem);
		query = q;
		final_count = BigInteger.ZERO;
		number_of_graphlets = graphlets;
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
			
			// Compute and store trends per graphlet
			ArrayList<Graph> graphlets = graph.partition(number_of_graphlets);
			for (Graph graphlet : graphlets) {
				for(NodesPerSecond nodes : graphlet.all_nodes) {		
					final_count = withinGraphlets(graphlet, nodes.nodes_per_second);
			}}
			// Compute and output trends across graphlets
			final_count = acrossGraphlets(graphlets, graphlets);
			
			//System.out.println("Sub-stream id: " + substream_id + " with count " + final_count.intValue());
						
			final_count = BigInteger.ZERO;
		}			
	}
	
	public BigInteger withinGraphlets (Graph graphlet, ArrayList<Node> current_level) { 
		
		// Array for recursive call of this method
		ArrayList<Node> next_level_nodes = new ArrayList<Node>();
			
		// Hash for quick lookup of saved nodes
		HashMap<Integer,Integer> next_level_hash = new HashMap<Integer,Integer>();
			
		for (Node this_node : current_level) {
				
			/*** Base case: Create the results for the first nodes ***/
			if (this_node.results.isEmpty()) {
				
				EventTrend new_trend = new EventTrend(this_node, this_node, this_node.toString());
				this_node.results.add(new_trend);
				graphlet.trends.add(new_trend);
				//System.out.println(new_trend.sequence);
				final_count = final_count.add(BigInteger.ONE);
				memory.set(memory.get() + new_trend.getEventNumber() * 4);				
			} 				
			/*** Recursive case: Copy results from the current node to its previous node and  
			 *** append this previous node to each copied result ***/			
			if (!this_node.previous.isEmpty()) {
				for (Node next_node : this_node.previous) {
					if (next_node.event.sec >= graphlet.start) {
					
						for (EventTrend old_trend : this_node.results) {
							String new_seq = next_node.toString() + ";" + old_trend.sequence;
							EventTrend new_trend = new EventTrend(next_node, old_trend.last_node, new_seq);
							next_node.results.add(new_trend);
							graphlet.trends.add(new_trend);
							//System.out.println(new_trend.sequence);
							final_count = final_count.add(BigInteger.ONE);
							memory.set(memory.get() + new_trend.getEventNumber() * 4);
						}														
					
						// Check that following is not in next_level
						if (!next_level_hash.containsKey(next_node.event.id)) {
							next_level_nodes.add(next_node); 
							next_level_hash.put(next_node.event.id,1);
						}
					}
				}
				// Delete intermediate results
				this_node.results.clear();				
			} 		
		}					
		// Call this method recursively
		if (!next_level_nodes.isEmpty()) final_count = withinGraphlets(graphlet, next_level_nodes);			
		return final_count;
	}
	
	public BigInteger acrossGraphlets (ArrayList<Graph> new_graphlets, ArrayList<Graph> original_graphlets) {
		
		ArrayList<Graph> next_level_graphlets = new ArrayList<Graph>(); 
		
		for (Graph graphlet1 : new_graphlets) {			
			for (Graph graphlet2 : original_graphlets) {
				if (graphlet1.start < graphlet2.start && graphlet1.end < graphlet2.start) {
					
					//System.out.println("Graphlet1 " + graphlet1.start + "," + graphlet1.end +
					//		" Graphlet2 " + graphlet2.start + "," + graphlet2.end);
					
					Graph next_level_graphlet = new Graph();
					next_level_graphlet.start = graphlet1.start;
					next_level_graphlet.end = graphlet2.end;
										
					for (EventTrend trend1 : graphlet1.trends) {
						for (EventTrend trend2 : graphlet2.trends) {
							
							if (trend2.first_node.previous.contains(trend1.last_node)) {
							
								String next_level_sequence = trend1.sequence + " " + trend2.sequence;
								EventTrend next_level_trend = new EventTrend(trend1.first_node, trend2.last_node, next_level_sequence);
								next_level_graphlet.trends.add(next_level_trend);
							 
								//System.out.println(next_level_sequence);
								final_count = final_count.add(BigInteger.ONE);
								memory.set(memory.get() + next_level_trend.getEventNumber() * 4);
							} 
						}
					}
					if (!next_level_graphlet.trends.isEmpty()) next_level_graphlets.add(next_level_graphlet);
				}
			}
		}
		if (!next_level_graphlets.isEmpty()) final_count = acrossGraphlets(next_level_graphlets, original_graphlets);			
		return final_count;
	}	
}
