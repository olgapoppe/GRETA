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
	public int start;
	public int end;
	
	// Counts
	public BigInteger final_count;
	public BigInteger count_for_current_second;
	
	// Last node
	Node last_node;
	
	// Trends
	public ArrayList<EventTrend> trends;
			
	public Graph () {
		all_nodes = new ArrayList<NodesPerSecond>();
		nodeNumber = 0;
		edgeNumber = 0;
		start = 0;
		end = 0;
		final_count = new BigInteger("0");
		count_for_current_second = new BigInteger("0");
		last_node = null;	
		trends = new ArrayList<EventTrend>();
	}	
	
	public Graph getCompleteGraphForPercentage (ConcurrentLinkedQueue<Event> events, Query query, int negated_events_per_window) {		
		
		int number_of_events_per_window = events.size();
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
			nodes_in_current_second.nodes_per_second.add(new_node);
			
			// Compute the number of predecessors
			int numberOfPredecessors = (nodeNumber * query.getPercentage())/100;
			nodeNumber++;
						
			// Every n'th event marks all previous events as incompatible with all future events
			if (negated_events_per_window+1 > 1) {
			if (nodeNumber%(number_of_events_per_window/(negated_events_per_window+1)) == 0) {
			
				System.out.println("Negated event at " + event.sec);
				for (NodesPerSecond nodes_per_second : all_nodes) {
					if (nodes_per_second.second < curr_sec) {
						nodes_per_second.marked = true;						
					}
				}
			}
			}
			
			// Connect this event to all previous compatible events and compute the count of this node
			for (NodesPerSecond nodes_per_second : all_nodes) {
				if (nodes_per_second.second < curr_sec && !nodes_per_second.marked && event.actual_count < numberOfPredecessors) {
					for (Node predecessor : nodes_per_second.nodes_per_second) {																				
						if (event.actual_count < numberOfPredecessors) {		
							new_node.connect(predecessor);
							new_node.count = new_node.count.add(predecessor.count);						
							event.actual_count++;
							edgeNumber++;
							// System.out.println(new_node.event.id + " : " + previous_node.event.id + ", ");
						} else { break; }
					}
				} 
			}	
									
			// Update the final count
			final_count = final_count.add(new BigInteger(new_node.count+""));
			//System.out.println(new_node.toString());
			 
			event = events.peek();
		}	
		return this;
	}	
	
	public ArrayList<Graph> partition (int number_of_graphlets) {
		
		ArrayList<Graph> graphlets = new ArrayList<Graph>();
		
		int balanced_graphlet_size = nodeNumber/number_of_graphlets;
		//System.out.println("Balanced partition size: " + balanced_graphlet_size + " Graph size: " + nodeNumber);
		Graph graphlet = new Graph();
		graphlet.start = all_nodes.get(0).second;
		graphlet.end = all_nodes.get(0).second;
				
		for (NodesPerSecond nodes : all_nodes) {
			if (graphlet.nodeNumber + nodes.nodes_per_second.size() <= balanced_graphlet_size) {
				// Add nodes per second to the current graphlet 
				graphlet.all_nodes.add(nodes);
				graphlet.nodeNumber += nodes.nodes_per_second.size();
				graphlet.end = nodes.second;
			} else {
				// Add previous graphlet to the result
				if (graphlet.nodeNumber > 0) graphlets.add(graphlet);
				// Create a new graphlet and add nodes per second to it
				graphlet = new Graph();
				graphlet.start = nodes.second;
				graphlet.end = nodes.second;
				graphlet.all_nodes.add(nodes);
				graphlet.nodeNumber = nodes.nodes_per_second.size();
			}
		}
		// Add last graphlet to the result
		if (graphlet.nodeNumber > 0) graphlets.add(graphlet);
		// Print resulting graphlets
		//for (Graph g : graphlets) System.out.println("--->" + g.nodeNumber);
				
		return graphlets;		
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
							
							new_node.count = new_node.count.add(previous_node.count);																					
							//System.out.println(previous_node.event.id + " , " + new_node.event.id);
				}}}				
					
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
	
	/*public Graph getCompleteGraphForPercentage (ConcurrentLinkedQueue<Event> events, Query query) {		
		
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
						
			ArrayList<Node> previous_nodes = new ArrayList<Node>();
			boolean marked = false;
			int count = 0;
			if (all_nodes.size()-2 >= 0) {
				NodesPerSecond nodes_in_previous_second = all_nodes.get(all_nodes.size()-2);
				marked = nodes_in_previous_second.marked;
				previous_nodes = nodes_in_previous_second.nodes_per_second;
				count = previous_nodes.size();
			}
			int required_count = (count * query.getPercentage())/100;
			//System.out.println(event.toString() + " -> " + count);
			
			if (query.event_selection_strategy.equals("any") || nodes_in_current_second.nodes_per_second.isEmpty()) {
				nodes_in_current_second.nodes_per_second.add(new_node);		
				nodeNumber++;			
						
				// Connect this event to all previous compatible events and compute the count of this node
				if (!marked) {
					for (Node previous_node : previous_nodes) {																				
						if (event.actual_count<required_count) {								
							
							new_node.count = new_node.count.add(previous_node.count);						
							event.actual_count++;
							System.out.println(new_node.event.id + " : " + previous_node.event.id + ", ");
				}}}	
									
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
	}*/		
	
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
