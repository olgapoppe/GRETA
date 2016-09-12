package event;

public class StockEvent extends Event {
	
	public int sector;
	public int company;
	public int price;
	
	public StockEvent (int i, int t, int s, int c, int p) {
		super("stock", i, t);
		sector = s;
		company = c;
		price = p;
	}
	
	public boolean equals(StockEvent other) {
		return this.sec == other.sec && this.sector == other.sector && this.company == other.company && this.price == other.price;
	}
	
	public static StockEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int i = Integer.parseInt(values[0]);
		int t = Integer.parseInt(values[0]);
        int s = Integer.parseInt(values[1]);
        int c = Integer.parseInt(values[2]);          	
        int p = Integer.parseInt(values[2]);
    	    	    	
    	StockEvent event = new StockEvent(i,t,s,c,p);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public boolean isRelevant() {
		return sector > 0;
	}
	
	public String toString() {
		return "sec " + sec + " sector " + sector + " company " + company + " price " + price;
	}
}
