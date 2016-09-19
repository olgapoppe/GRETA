package event;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Event {
	
	public int id;
	public int sec;
	// Mapping of window identifier to the pointers of this event within this window
	public HashMap<String,ArrayList<Event>> pointers;
	public boolean flagged;
	public boolean marked;
		
	public Event (int i, int s) {
		id = i;		
		sec = s;
		pointers = new HashMap<String,ArrayList<Event>>();
		flagged = false;
		marked = false;
	}
	
	public int getStart() {
		return sec;
	}
	
	public int getEnd() {
		return sec;
	}	
	
	public static Event parse (String line, String type) {
		Event event;
		if (type.equals("raw")) { 
			event = RawEvent.parse(line); 
		} else {
		if (type.equals("cluster")) { 
			event = ClusterEvent.parse(line); 
		} else {
		if (type.equals("stock")) { 
			event = StockEvent.parse(line); 
		} else  {
			event = PositionReport.parse(line);
		}}}
		return event;
	}
	
	public abstract String getSubstreamid();
	public abstract boolean isRelevant();
	public abstract boolean up(Event next);
	public abstract boolean down(Event next);
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
