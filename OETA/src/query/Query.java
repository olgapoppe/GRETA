package query;

import event.*;

public class Query {
	
	// any, next, cont
	public String event_selection_strategy; 
	// up, down, none, required_percentage% 
	public String predicate_on_adjacent_events;
	
	public Query (String ess, String pred) {
		event_selection_strategy = ess;
		predicate_on_adjacent_events = pred;
	}
	
	public boolean compatible (Event previous, Event following, int current_percentage) {
		
		if (predicate_on_adjacent_events.endsWith("%")) {
			String str= predicate_on_adjacent_events.replaceAll("%", "");
			int required_percentage = Integer.parseInt(str);
			
			//System.out.println(current_percentage + " < " + required_percentage);
			
			return current_percentage < required_percentage;
		} else {		
		if (predicate_on_adjacent_events.equals("up")) {
			return previous.up(following);
		} else {
		if (predicate_on_adjacent_events.equals("down")) {
			return previous.down(following);	
		} else {
			return true;
		}}} 		
	}
	
	public boolean compressible () {
		return !event_selection_strategy.equals("any") || predicate_on_adjacent_events.equals("none");
	}

}
