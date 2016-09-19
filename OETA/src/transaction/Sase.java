package transaction;

import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;
import iogenerator.*;
import query.Query;

public class Sase extends Transaction {
	
	Query query;
	
	public Sase (Stream str, int l, Query q, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(str,l,o,tn,time,mem);
		query = q;
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);			
		transaction_number.countDown();
	}
	
	public void computeResults () {
		
		Set<String> substream_ids = stream.substreams.keySet();
		
		for (String substream_id : substream_ids) {					
		 
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			computeResults(events);
		}
	}
	
	public void computeResults (ConcurrentLinkedQueue<Event> events) {
		
		// Initiate data structures: stack, last events
		Stack<Event> stack = new Stack<Event>();
		ArrayList<Event> lastEvents = new ArrayList<Event>();
		ArrayList<Event> newLastEvents = new ArrayList<Event>();
		int curr_sec = -1;
		int pointerCount = 0;		
		Event event = events.poll();
		
		while (event != null && event.sec<=limit) {
			
			boolean added = false;		
				
			// Store pointers to its predecessors
			if (event.sec == curr_sec) {
				for (Event last : lastEvents) {
					
					if (event.pointers == null) System.out.println("Pointers null");
					
					if (!event.pointers.contains(last)) {
						event.pointers.add(last);
						pointerCount++;
				}}
				if (query.event_selection_strategy.equals("any")) {
					newLastEvents.add(event);
					added = true;
				}
			} else {
				// Update last events and draw pointers from event to all last events
				lastEvents.clear();
				lastEvents.addAll(newLastEvents);
				for (Event last : lastEvents) {
					if (!event.pointers.contains(last) && !last.marked) {
						event.pointers.add(last);
						pointerCount++;
				}}
				newLastEvents.clear();
				newLastEvents.add(event);
				added = true;
				curr_sec = event.sec;
			}			
			// Store the event in a stack or mark all events as incompatible with all future events
			if (added) {
				stack.add(event);
			} else {
				if (query.event_selection_strategy.equals("cont")) 
					for (Event e : stack) 
						e.marked = true;						
			}
			event = events.poll();
		}		
		// For each new last event, traverse the pointers to extract trends
		ArrayList<EventSequence> without_duplicates = new ArrayList<EventSequence>();
		for (Event lastEvent : newLastEvents) 
			if (!lastEvent.marked)
				traversePointers(lastEvent, new Stack<Event>(), without_duplicates);
		
		System.out.println(without_duplicates);
		count += without_duplicates.size();
		total_mem.set(total_mem.get() + stack.size() + pointerCount + count);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);
	}
	
	// DFS in the stack
	public void traversePointers (Event event, Stack<Event> current_trend, ArrayList<EventSequence> without_duplicates) {       
			
		current_trend.push(event);
		//System.out.println("pushed " + event.id);
		
		/*** Count all trends with this new event ***/
		ArrayList<Event> input = new ArrayList<Event>();
	    input.addAll(current_trend);
	    without_duplicates = getIncompleteTrends(input,without_duplicates);
	    	    	
	    /*** Traverse the following nodes. ***/
		for(Event previous : event.pointers) {        		
			//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       	traversePointers(previous,current_trend,without_duplicates);        		
	    }        	
	    
	    Event top = current_trend.pop();
	    if (!top.flagged) {
	    	top.flagged = true;
	    	//System.out.println("popped and flagged " + top.id);
	    }  	    	    
	}	
	
	public ArrayList<EventSequence> getIncompleteTrends (ArrayList<Event> elements, ArrayList<EventSequence> without_duplicates) {
		
		// The new event is obligatory in all new trends
	    Event obligatory = elements.remove(elements.size()-1);
	    	    	    
	    // Get the new trends with obligatory event
	    ArrayList<EventSequence> with_duplicates = getCombinations(elements,obligatory);
	      
	    // Eliminate duplicates
	 	for (EventSequence seq : with_duplicates) {
	 		if (!seq.allFlagged() && !without_duplicates.contains(seq)) 
	 			without_duplicates.add(seq);
	 	}	   	    
	    return without_duplicates;
	}
	
	public ArrayList<EventSequence> getCombinations (ArrayList<Event> elements, Event obligatory) {
		
		ArrayList<EventSequence> with_duplicates = new ArrayList<EventSequence>();
		
	    /*** Base case: Obligatory event is a trend ***/
		if(elements.size() == 0) {
			
			ArrayList<Event> events = new ArrayList<Event>();
			events.add(obligatory);
			EventSequence seq = new EventSequence(events);		
			with_duplicates.add(seq);
						
		} else {
				
			/*** Recursive case: Combine the first event with all combinations of other events ***/
			Event first_event = elements.remove(0);  							
			ArrayList<EventSequence> rest = getCombinations(elements,obligatory);
			int limit = rest.size();
						
			for (int i = 0; i < limit; i++) {
				
				if (query.event_selection_strategy.equals("any") || 
					first_event.pointers.contains(rest.get(i).events.get(0))) { 
					// first_event has a pointer to the fist event of the rest 
				
					ArrayList<Event> events = new ArrayList<Event>();
					events.add(first_event);
					events.addAll(rest.get(i).events);		
					EventSequence seq = new EventSequence(events);
					rest.add(seq);	
				}
			}
			with_duplicates.addAll(rest);
		}		
	    return with_duplicates;
	}	
}
