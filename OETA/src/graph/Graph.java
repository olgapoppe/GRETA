package graph;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import event.*;
import query.*;

public class Graph {
	
	// Events per second 
	public ArrayList<NodesPerSecond> all_nodes;
	
	// Memory requirement
	public int nodeNumber;
	public int edgeNumber;	
	
	// Counts
	public BigInteger final_count;
	public BigInteger count_for_current_second;
	
	// Last node
	Node last_node;
		
	public Graph () {
		all_nodes = new ArrayList<NodesPerSecond>();
		nodeNumber = 0;
		edgeNumber = 0;		
		final_count = new BigInteger("0");
		count_for_current_second = new BigInteger("0");
		last_node = null;
	}
	
	public Graph getCompleteGraph (ConcurrentLinkedQueue<Event> events, Query query) {		
		
		int curr_sec = -1;	
		Event event = events.peek();			
		while (event != null) {
			
			event = events.poll();
			//System.out.println("--------------" + event.id);
			
			// Update the current second and all_nodes
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				NodesPerSecond nodes_in_new_second = new NodesPerSecond(curr_sec);
				all_nodes.add(nodes_in_new_second);				
			}
			
			// Create and store a new node
			Node new_node = new Node(event);
			NodesPerSecond nodes_in_current_second = all_nodes.get(all_nodes.size()-1);
			
			if (query.event_selection_strategy.equals("any") || nodes_in_current_second.nodes_per_second.isEmpty()) {
				nodes_in_current_second.nodes_per_second.add(new_node);		
				nodeNumber++;
						
				// Connect this event to all previous compatible events and compute the count of this node
				for (NodesPerSecond nodes_per_second : all_nodes) {
					if (nodes_per_second.second < curr_sec && !nodes_per_second.marked) {
						for (Node previous_node : nodes_per_second.nodes_per_second) {
							
							int id_of_last_predecessor = nodeNumber - nodes_in_current_second.nodes_per_second.size();
							int id_of_last_compatible_predecessor = (id_of_last_predecessor * query.getPercentage())/100;
														
							if(query.compatible(previous_node.event,new_node.event,id_of_last_compatible_predecessor)) {
								new_node.previous.add(previous_node);
								//System.out.println(previous_node.event.id + " is predecessor of " + new_node.event.id);
								new_node.count = new_node.count.add(previous_node.count);							
								edgeNumber++;						
				}}}}				
					
				// Update the final count
				final_count = final_count.add(new BigInteger(new_node.count+""));
				//System.out.println(new_node.toString());
			} else {
				// Mark all previous events as incompatible with all future events under the contiguous strategy
				if (query.event_selection_strategy.equals("cont")) {
					for (NodesPerSecond nodes_per_second : all_nodes) {
						nodes_per_second.marked = true;
			}}}
			event = events.peek();
		}		
		return this;
	}	
	
	public Graph getCompressedGraph (ConcurrentLinkedQueue<Event> events, Query query) {		
		
		if (query.event_selection_strategy.equals("any")) {
		
			int curr_sec = -1;
			Event event = events.peek();			
			while (event != null) {
					
				event = events.poll();
				
				// Update the current second and intermediate counts
				if (curr_sec < event.sec) {
					curr_sec = event.sec;
					final_count = final_count.add(count_for_current_second);	
					count_for_current_second = new BigInteger("0");
				} 
				BigInteger event_count = new BigInteger("1").add(final_count);
				count_for_current_second = count_for_current_second.add(event_count);				
							
				/*if (event != null && event.getSubstreamid().equals("10_7"))
					System.out.println(event.sec + " : " + event.id + " with count " + event_count);*/	
				
				event = events.peek();
			}
			// Add the count for last second to the final count
			final_count = final_count.add(count_for_current_second);
		
		} else {
			
			Event event = events.peek();			
			while (event != null) {
				
				event = events.poll();
				// If the event can be inserted, update the final count and last node
				Node new_node = new Node(event);
				
				if (last_node == null || (last_node.event.sec < event.sec && query.compatible(last_node.event,new_node.event,0))) {
					
					if (last_node != null && !last_node.marked) 
						new_node.count = new_node.count.add(last_node.count);
					final_count = final_count.add(new_node.count);
					last_node = new_node;
					
					//System.out.println(event.id + " with count " + new_node.count + " and final count " + final_count);		
					
				} else {
					// If the event cannot be inserted and the event selection strategy is contiguous, delete the last node
					if (query.event_selection_strategy.equals("cont")) 
						last_node.marked = true;
				}
				event = events.peek();
			}			
		}
		return this;
	}	
}
