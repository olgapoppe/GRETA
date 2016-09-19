package event;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Stream {
	
	public HashMap<String,ConcurrentLinkedQueue<Event>> substreams;
	public AtomicInteger driverProgress;
				
	public Stream (AtomicInteger dp) {		
		substreams = new HashMap<String,ConcurrentLinkedQueue<Event>>();
		driverProgress = dp;		
	}
	
	public synchronized void setDriverProgress (int sec) {
		driverProgress.set(sec);			
		notifyAll();		
	}

	public synchronized boolean getDriverProgress (int sec) {	
		//System.out.println("Scheduler is waiting for: " + sec);
		try {			
			while (driverProgress.get() < sec) {				
				wait(); 						
			}	
		} catch (InterruptedException e) { e.printStackTrace(); }
		return true;		
	}
}
