package transaction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
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
	int negated_events_per_window;
	
	public HCet (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem, int graphlets, int nepw) {		
		super(str,d,time,mem);
		query = q;
		final_count = BigInteger.ZERO;
		number_of_graphlets = graphlets;
		negated_events_per_window = nepw;
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
			graph = graph.getCompleteGraphForPercentage(events, query, negated_events_per_window);
			memory.set(memory.get() + graph.nodeNumber * 12 + graph.edgeNumber * 4);
			
			// Compute and store trends per graphlet
			ArrayList<Graph> graphlets = graph.partition(number_of_graphlets);
			for (Graph graphlet : graphlets) {
				for(NodesPerSecond nodes : graphlet.all_nodes) {		
					final_count = withinGraphlets(graphlet, nodes.nodes_per_second);
			}}
			// Compute and output trends across graphlets
			final_count = acrossGraphlets(graphlets);
			
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
		// Call this method recursively
		if (!next_level_nodes.isEmpty()) final_count = withinGraphlets(graphlet, next_level_nodes);			
		return final_count;
	}
	
	public BigInteger acrossGraphlets (ArrayList<Graph> all_graphlets) {
				
		ArrayList<String> bitCombinations = bitCombinations(all_graphlets.size());
		
		for (String bitCombination : bitCombinations) {	
			
			ArrayList<Graph> graphlets2combine = new ArrayList<Graph>();
			String[] bits = bitCombination.split(";");
			
			for (int i=0; i<bits.length; i++) {			
				
				if (bits[i].equals("1")) {		
					
					Graph graphlet = all_graphlets.get(i);					
					graphlets2combine.add(graphlet);									
				}
			}
			int maxLength = 0;
			int index = graphlets2combine.size()-1;
			for (EventTrend partial_trend : graphlets2combine.get(index).trends) {
				Stack<EventTrend> final_trend = new Stack<EventTrend>();
				maxLength = computeFinalTrends(partial_trend, final_trend, maxLength, index, graphlets2combine);
			}
		}					
		return final_count;
	}	
	
	// DFS computing final trends
	public int computeFinalTrends (EventTrend partial_trend, Stack<EventTrend> final_trend, int maxLength, int index, ArrayList<Graph> graphlets) {       
				
		final_trend.push(partial_trend);
		//System.out.println("pushed " + event_trend.sequence);
		        
		/*** Base case: We hit the end of the graph. Output the current final trend. ***/
		if (index == 0) {   
		    String result = "";        	
		    Iterator<EventTrend> iter = final_trend.iterator();
		    int eventNumber = 0;
		    while(iter.hasNext()) {
		    	EventTrend n = iter.next();
		    	result = n.sequence.toString() + ";" + result;
		    	eventNumber += n.getEventNumber();
		    }
		    if (maxLength < eventNumber) maxLength = eventNumber;	
		    String s = (!result.isEmpty()) ? result : partial_trend.sequence;
		   	//results.add(s);  
		   	//System.out.println(s);
		   	final_count = final_count.add(BigInteger.ONE);
		} else {
		   	/*** Recursive case: Traverse the following graphlets. ***/ 
			// get previous graphlet
			// call this method recursively on each of its compatible trends
			Graph previous_graphlet = graphlets.get(index-1);	       		
			for (EventTrend previous_trend : previous_graphlet.trends) {   
				if (partial_trend.first_node.previous.contains(previous_trend.last_node)) {
					maxLength = computeFinalTrends(previous_trend, final_trend, maxLength, index-1, graphlets);
			   	}			
		    }	            	
		}
		EventTrend top = final_trend.pop();
		//System.out.println("popped " + top.sequence);
		   
		return maxLength;
	}
	
	// Compute all bit combinations of n bits excluding powers of 2
	ArrayList<String> bitCombinations(int n) {		
		ArrayList<String> results = new ArrayList<String>();
		String B;
	    int temp;		
        for(int i = 0; i < Math.pow(2,n); i++) {
            B = "";
            temp = i;
            if ((temp & (temp - 1)) != 0) {
            	for (int j = 0; j < n; j++) {
            		if (temp%2 == 1)
            			B = "1;"+B;
            		else
            			B = "0;"+B;
                    temp = temp/2;
            	}
            	results.add(B);
            }
        }
        return results;
    } 
}
