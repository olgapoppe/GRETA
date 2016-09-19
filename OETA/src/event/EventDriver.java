package event;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class EventDriver implements Runnable {	
	
	String type;
	String filename;
	final Stream stream;			
	long startOfSimulation;
	AtomicInteger drProgress;
	AtomicInteger eventNumber;
	int events_per_window;
		
	public EventDriver (String ty, String f, Stream str, long start, AtomicInteger dp, AtomicInteger eN, int epw) {
		
		type = ty;
		filename = f;
		stream = str;			
		startOfSimulation = start;
		drProgress = dp;
		eventNumber = eN;
		events_per_window = epw;
	}

	/** 
	 * Read the input file, parse the events, and put events into the event queue
	 */
	public void run() {	
		try {
			
			// Input file
			Scanner scanner = new Scanner(new File(filename));
			// First event
			String line = scanner.nextLine();
	 		Event event = Event.parse(line,type);
	 		// Current second
	 		int curr_sec = -1;			 		
 			
 			/*** Put events within the current batch into the event queue ***/		
	 		while (event != null && eventNumber.get() <= events_per_window) {	 	
	 			
	 			System.out.println("Driver: " + event.toString());
	 				
	 			/*** Put the event into the event queue and increment the counter ***/						
	 			if (event.isRelevant()) {
	 				String substream_id = event.getSubstreamid();
	 				ConcurrentLinkedQueue<Event> substream = (stream.substreams.containsKey(substream_id)) ? 
	 						stream.substreams.get(substream_id) : 
	 						new ConcurrentLinkedQueue<Event>();
					substream.add(event);
					stream.substreams.put(substream_id,substream);
	 				eventNumber.set(eventNumber.get()+1);
	 			}
	 					
	 			/*** Set distributer progress ***/	
	 			if (curr_sec < event.sec) {		
	 					
	 				// Avoid null run exception when the stream is read too fast
	 				if (curr_sec>300) stream.setDriverProgress(curr_sec);	 				
	 				curr_sec = event.sec;
	 			}
	 			
	 			/*** Reset event ***/
	 			if (scanner.hasNextLine()) {		 				
	 				line = scanner.nextLine();   
	 				event = Event.parse(line,type);		 				
	 			} else {
	 				event = null;		 				
	 			}
	 			
	 			/*** Sleep one second every 100th event ***/
	 			if (eventNumber.get() % 100 == 0) {
	 				try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
	 			}
	 		}		 			
	 		/*** Set distributor progress ***/		 					
	 		stream.setDriverProgress(curr_sec);					
	 		System.out.println("Driver progress: " + curr_sec);
	 				
	 		/*** Clean-up ***/		
			scanner.close();				
			System.out.println("Driver is done.");	
 		
		} catch (FileNotFoundException e) { e.printStackTrace(); }
	}	
}
