package graph;

import java.io.IOException;
import java.util.ArrayList;
import event.*;
import iogenerator.*;

public class Node {
	
	public StockEvent event;
	public ArrayList<Node> previous;
	public ArrayList<Node> following;
		
	public Node (StockEvent e) {
		event = e;
		previous = new ArrayList<Node>();
		following = new ArrayList<Node>();		
	}
	
	public String toString() {
		return event.toString(); 
	}
}
