package graph;

import java.util.ArrayList;
import event.*;

public class Node {
	
	public StockEvent event;
	public ArrayList<Node> previous;
			
	public Node (StockEvent e) {
		event = e;
		previous = new ArrayList<Node>();
	}
	
	public boolean equals (Node other) {
		return this.event.equals(other.event);
	}
	
	public void connect (Node old_event) {
		if (!this.previous.contains(old_event)) this.previous.add(old_event);
	}	
	
	public String toString() {
		return event.toString(); 
	}
}
