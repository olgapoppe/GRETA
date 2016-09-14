package event;

public class RawEvent extends Event {
	
	public RawEvent (String t, int i, int s) {
		super(t,i,s);
	}	
	
	public static RawEvent parse (String line) {
		
		String[] values = line.split(":");
		
		String t = values[0];
		int i = Integer.parseInt(values[1]);
		int s = Integer.parseInt(values[1]);
	           	    	    	
    	RawEvent event = new RawEvent(t,i,s);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean up(Event next) {
		return true;
	}
	
	public boolean down(Event next) {
		return true;
	}
	
	public String toString() {
		return type + sec;
	}
}