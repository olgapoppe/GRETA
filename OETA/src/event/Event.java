package event;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Event {
	
	public String type;
	public int id;
	public int sec;
	// Mapping of window identifier to the pointers of this event within this window
	public HashMap<String,ArrayList<Event>> pointers;
	
	public Event (String t, int i, int s) {
		id = i;
		type = t;
		sec = s;
		pointers = new HashMap<String,ArrayList<Event>>();
	}
	
	public int getStart() {
		return sec;
	}
	
	public int getEnd() {
		return sec;
	}	
	
	public static Event parse (String line, String type) {
		Event event;
		if (type == "raw") { 
			event = RawEvent.parse(line); 
		} else { 
			event = StockEvent.parse(line); 
		}
		return event;
	}
	
	public abstract boolean isRelevant();
	public abstract String toString();
	
	/** Print this event with pointers to console */
	public String toStringWithPointers(String widnow_id) {
		ArrayList<Event> predecessors = pointers.get(widnow_id);
		String s = id + " : ";
		for (Event predecessor : predecessors) {
			s += predecessor.id + ",";
		}
		return s;
	}
}
