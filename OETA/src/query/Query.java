package query;

import graph.*;

public class Query {
	
	//public String event_selection_strategy; 
	
	// Predicate can be up, down, none 
	public String predicate_on_adjacent_events;
	
	public Query (String pred) {
		predicate_on_adjacent_events = pred;		
	}
	
	public boolean compatible (Node previous, Node following) {
		if (predicate_on_adjacent_events.equals("up")) {
			return previous.event.up(following.event);
		} else {
		if (predicate_on_adjacent_events.equals("down")) {
			return previous.event.down(following.event);	
		} else {
			return true;
		}}
	}

}
