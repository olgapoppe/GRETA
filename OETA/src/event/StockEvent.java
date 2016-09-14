package event;

public class StockEvent extends Event {
	
	public int sector;
	public int company;
	public int price;
	
	public StockEvent (int i, int sec, int s, int c, int p) {
		super("stock", i, sec);
		sector = s;
		company = c;
		price = p;
	}
	
	public boolean equals (StockEvent other) {
		return id == other.id;
	}
	
	public static StockEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int i = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
        int s = Integer.parseInt(values[2]);
        int c = Integer.parseInt(values[3]);          	
        int p = Integer.parseInt(values[4]);
    	    	    	
    	StockEvent event = new StockEvent(i,sec,s,c,p);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public boolean isRelevant() {
		return sector > 0;
	}
	
	public boolean up(Event next) {
		return price < ((StockEvent)next).price;
	}
	
	public boolean down(Event next) {
		return price > ((StockEvent)next).price;
	}
	
	public String toString() {
		return "" + id;
	}
	
	public String toStringComplete() {
		return "id " + id + "sec " + sec + " sector " + sector + " company " + company + " price " + price;
	}
}
