package graph;

import java.util.ArrayList;
import java.util.HashMap;

public class Graph {
	
	public ArrayList<Node> nodes;
	// Arrays of events per event type. Each array is sorted by time stamp. 
	public HashMap<Integer,ArrayList<Node>> events_per_second;
	public int edgeNumber;	
		
	public Graph () {
		nodes = new ArrayList<Node>();
		events_per_second = new HashMap<Integer,ArrayList<Node>>();
		edgeNumber = 0;		
	}
	
	public void connect (Node old_event, Node new_event) {
		if (!new_event.previous.contains(old_event)) {
			new_event.connect(old_event);
			edgeNumber++;
		}
	}
}
