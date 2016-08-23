package graph;

import java.util.ArrayList;
import event.*;

public class Node {
	
	public StockEvent event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
		
	public Node (StockEvent e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();		
	}
	
	public void connect (Node other) {
		if (!this.following.contains(other)) this.following.add(other);
		if (!other.previous.contains(this)) other.previous.add(this);
	}	
	
	public String toString() {
		return event.toString(); 
	}
}
