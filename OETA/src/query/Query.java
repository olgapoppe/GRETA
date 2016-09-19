package query;

import graph.*;

public class Query {
	
	// any, next, cont
	public String event_selection_strategy; 
	// up, down, none 
	public String predicate_on_adjacent_events;
	
	public Query (String ess, String pred) {
		event_selection_strategy = ess;
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
	
	public boolean compressible () {
		return !event_selection_strategy.equals("any") || predicate_on_adjacent_events.equals("none");
	}

}
