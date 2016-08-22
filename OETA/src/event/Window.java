package event;

import java.util.ArrayList;

public class Window {
	
	public String id;
	public int start;
	public int end;
	public ArrayList<StockEvent> events;
	public int event_number;
		
	public Window (int s, int e) {
		id = s + "-" + e;
		start = s;
		end = e;
		events = new ArrayList<StockEvent>();
		event_number = 0;
	}

}
