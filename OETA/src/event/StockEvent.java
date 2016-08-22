package event;

public class StockEvent {
	
	public int sec;
	public int sector;
	public int company;
	public int price;
	
	public StockEvent (int t, int s, int c, int p) {
		sec = t;
		sector = s;
		company = c;
		price = p;
	}
	
	public String toString() {
		return "sec " + sec + " sector " + sector + " company " + company + " price " + price;
	}
	
	public static StockEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int t = Integer.parseInt(values[0]);
        int s = Integer.parseInt(values[1]);
        int c = Integer.parseInt(values[2]);          	
        int p = Integer.parseInt(values[2]);
    	    	    	
    	StockEvent event = new StockEvent(t,s,c,p);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
}
