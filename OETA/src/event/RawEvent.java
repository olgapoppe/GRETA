package event;

public class RawEvent {
	
	String type;
	int time;
	
	public RawEvent (String t1, int t2) {
		type = t1;
		time = t2;
	}
	
	public int getStart() {
		return time;
	}
	
	public int getEnd() {
		return time;
	}
	
	public String toString() {
		return type + time;
	}
}