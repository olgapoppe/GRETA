package event;

public class PositionReport extends Event {

	public int vid; 
	public int spd; 
	public int xway; 
	public int lane;
	public int dir; 
	public int seg;
	public int pos;	
	
	public PositionReport (int sec, int v, int s, int x, int l, int d, int s1, int p) {
		super(0, sec);
		vid = v;
		spd = s;
		xway = x;
		lane = l;
		dir = d;
		seg = s1;
		pos = p;		
	}
	
	/**
	 * Parse the given line and construct a position report.
	 * @param line	
	 * @return position report
	 */
	public static PositionReport parse (String line) {
		
		String[] values = line.split(",");
		
		int new_sec = Integer.parseInt(values[1]);
        int new_vid = Integer.parseInt(values[2]);          	
    	int new_spd = Integer.parseInt(values[3]);
    	int new_xway = Integer.parseInt(values[4]);
    	int new_lane = Integer.parseInt(values[5]);
    	int new_dir = Integer.parseInt(values[6]);
    	int new_seg = Integer.parseInt(values[7]);
    	int new_pos = Integer.parseInt(values[8]);    
    	    	    	
    	PositionReport event = new PositionReport(new_sec, new_vid, new_spd, new_xway, new_lane, new_dir, new_seg, new_pos);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	/**
	 * Determine whether this position report is equal to the given position report. 
	 * @param e	position report
	 * @return boolean
	 */	
	public boolean equals (PositionReport e) {
		return 	sec == e.sec &&				
				vid == e.vid &&
				spd == e.spd &&
				xway == e.xway &&
				lane == e.lane &&
				dir == e.dir &&
				seg == e.seg &&
				pos == e.pos;
	}	
	
	public String getSubstreamid() {
		return vid + "";
	}
	
	/** 
	 * Return true if this position report is correct.
	 * Return false otherwise.
	 */
	public boolean isRelevant () {
		return id>=0 && sec>=0 && spd>=0 && xway>=0 && lane>=0 && dir>=0 && seg>=0 && pos>=0;
	}
	
	public boolean up(Event next) {
		return true;
	}
	
	public boolean down(Event next) {
		return true;
	}
	
	public String toString() {
		return "" + id;
	}
	
	/** 
	 * Print this position report to file 
	 */
	public String toFile() {
		return id + "," + sec + "," + spd + "," + xway + "," + lane + "," + dir + "," + seg + "," + pos + "\n";		
	}	
}
