package event;

public class ActivityEvent extends Event {
	
	int pid;
	int activity;
	int heartRate;
		
	public ActivityEvent (int id, int sec, int p, int a, int h) {
		super(id, sec);
		pid = p;
		activity = a;
		heartRate = h;			
	}
	
	/**
	 * Parse the given line and construct an activity report.
	 * @param line	
	 * @return activity report
	 */
	public static ActivityEvent parse (int id, String line) {
		
		String[] values = line.split(",");
		
		int sec = Integer.parseInt(values[1]);
		int pid = Integer.parseInt(values[2]);
		int hr = Integer.parseInt(values[3]);
		int a = Integer.parseInt(values[8]);
		
        ActivityEvent event = new ActivityEvent(id,sec,pid,a,hr); 
        
    	//System.out.println(event.toString());    	
        return event;
	}	
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean equals (StockEvent other) {
		return id == other.id;
	}
	
	public String getSubstreamid() {
		return pid + "";
	}
	
	public boolean up(Event next) {
		return  heartRate < ((ActivityEvent)next).heartRate;
	}
	
	public boolean down(Event next) {
		return heartRate > ((ActivityEvent)next).heartRate;
	}
	
	public String toString() {
		return "" + id;
	}
	
	public boolean isCompatible (Event other) {
		if (other instanceof ActivityEvent) {
			ActivityEvent o = (ActivityEvent) other;
			return this.id == o.id && this.heartRate < o.heartRate && this.activity == o.activity && this.sec < other.sec;
		}
		return false;
	}

}
