package graph;

import java.util.ArrayList;
import event.*;
import query.*;

public class Graph {
	
	// Events per second 
	public ArrayList<NodesPerSecond> all_nodes;
	public int nodeNumber;
	public int edgeNumber;	
	public int final_count;
		
	public Graph () {
		all_nodes = new ArrayList<NodesPerSecond>();
		nodeNumber = 0;
		edgeNumber = 0;		
		final_count = 0;
	}
	
	public void connect (Node old_event, Node new_event) {
		if (!new_event.previous.contains(old_event)) {
			new_event.connect(old_event);
			edgeNumber++;
		}
	}
	
	public Graph getCompleteGraphUnderSkipTillAnyMatch (ArrayList<Event> events, Query query) {		
		
		int curr_sec = -1;				
		for (Event event : events) {
			
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
			nodeNumber++;
			
			// Connect this event to all previous compatible events and compute the count of this node
			for (NodesPerSecond nodes_per_second : all_nodes) {
				if (nodes_per_second.second < curr_sec) {
					for (Node previous_node : nodes_per_second.nodes_per_second) {
						if(query.compatible(previous_node,new_node)) {
							new_node.previous.add(previous_node);
							new_node.count += previous_node.count;							
							edgeNumber++;						
			}}}}
			
			// Update the final count
			final_count += new_node.count;
			System.out.println(new_node.toString());
		}		
		return this;
	}	
}
