package graph;

import java.util.ArrayList;
import java.util.HashMap;

public class Graph {
	
	public ArrayList<Node> nodes;
	// Events per second
	public HashMap<Integer,ArrayList<Node>> events_per_second;
	public int edgeNumber;	
		
	public Graph () {
		nodes = new ArrayList<Node>();
		events_per_second = new HashMap<Integer,ArrayList<Node>>();
		edgeNumber = 0;		
	}
	
	public void connect (Node first, Node second) {
		if (!first.following.contains(second)) {
			first.connect(second);
			edgeNumber++;
		}
	}
}
