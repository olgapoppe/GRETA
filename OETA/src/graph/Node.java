package graph;

import java.math.BigInteger;
import java.util.ArrayList;
import event.*;

public class Node {
	
	public Event event;
	public ArrayList<Node> previous;
	BigInteger count;
	public boolean marked;
			
	public Node (Event e) {
		event = e;
		previous = new ArrayList<Node>();
		count = new BigInteger("1");
		marked = false;
	}
	
	public boolean equals (Node other) {
		return event.equals(other.event);
	}
	
	public void connect (Node old_event) {
		if (!previous.contains(old_event)) previous.add(old_event);
	}	
	
	public String toString() {
		return event.toString() + " with count: " + count; 
	}
}
