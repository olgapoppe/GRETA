package event;

import java.util.ArrayList;

public class EventSequence {
	
	public ArrayList<RawEvent> events;
	
	public EventSequence (ArrayList<RawEvent> es) {
		events = es;
	}
	
	public int getStart() {
		return events.get(0).getStart();
	}
	
	public int getEnd() {
		return events.get(events.size()-1).getEnd();
	}

	public String toString() {
		
		String result = "";
		
		for (RawEvent e : events) {
			result += e.toString();
		}
		
		return result;
	}
}