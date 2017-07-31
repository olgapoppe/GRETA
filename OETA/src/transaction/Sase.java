package transaction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import query.*;

public class Sase extends Transaction {
	
	Query query;
	public BigInteger final_count;
	int negated_events_per_window;
			
	public Sase (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem, int nepw) {		
		super(str,d,time,mem);
		query = q;
		final_count = BigInteger.ZERO;
		negated_events_per_window = nepw;
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		latency.set(latency.get() + duration);			
		done.countDown();
	}
	
	public void computeResults () {
		
		Set<String> substream_ids = stream.substreams.keySet();		
		for (String substream_id : substream_ids) {					
		 
			// Construct the graph for each sub-stream
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);			
			Graph graph = new Graph();
			graph = graph.getCompleteGraph(events, query);
			
			// Traverse the pointers from each event in the graph
			int maxLength = 0;
			for (NodesPerSecond nodes : graph.all_nodes) {
				for (Node node : nodes.nodes_per_second) {
					Stack<Node> trend = new Stack<Node>();
					maxLength = traversePointers(node,trend,maxLength);
			}}			
			System.out.println("Sub-stream id: " + substream_id + " with count " + final_count.intValue());
			
			memory.set(memory.get() + graph.nodeNumber * 12 + graph.edgeNumber * 4 + maxLength);
			final_count = BigInteger.ZERO;
		}		
	}
	
	// DFS in the stack
	public int traversePointers (Node node, Stack<Node> trend, int maxLength) {       
			
		trend.push(node);
		//System.out.println("pushed " + node.event.id);
        
		/*** Output the current event trend ***/
        String result = "";        	
        Iterator<Node> iter = trend.iterator();
        while(iter.hasNext()) {
        	Node n = iter.next();
        	result += n.event.id + ";";
        }
        if (maxLength < trend.size()) maxLength = trend.size();	
        //results.add(result); 
        //System.out.println("result " + result);
        final_count = final_count.add(BigInteger.ONE);
      		
        /*** Traverse the previous nodes ***/        	
        for(Node previous : node.previous) {        		
        	//System.out.println("following of " + node.event.id + " is " + following.event.id);
        	maxLength = traversePointers(previous,trend,maxLength);        		
        }        	
        
        Node top = trend.pop();
        //System.out.println("popped " + top.event.id);
        
        return maxLength;	    	    
	}	
	
	/*public void computeResults (ConcurrentLinkedQueue<Event> events) {
		
		// Initiate data structures: stack, last events
		Stack<Event> stack = new Stack<Event>();
		ArrayList<Event> lastEvents = new ArrayList<Event>();
		ArrayList<Event> newLastEvents = new ArrayList<Event>();
		int curr_sec = -1;
		int prev_sec = -1;
		int pointerCount = 0;	
		Event event = events.peek();
		
		int number_of_events_in_prev_sec = 0;
		int number_of_events_in_curr_sec = 0;
				
		while (event != null) {
			
			event = events.poll();
			boolean added = false;	
										
			// Store pointers to its predecessors
			if (event.sec == curr_sec) {		
				
				int count = (number_of_events_at_second.containsKey(prev_sec)) ? number_of_events_at_second.get(prev_sec) : 0;
				int required_count = (count * query.getPercentage())/100;
				//System.out.println("count in " + prev_sec + " : " + count + " required: " + required_count);
						
				for (Event last : lastEvents) {
					
					if (event.pointers == null) System.out.println("Pointers null");					
					
					if (!event.pointers.contains(last) && event.actual_count<required_count) {
						event.pointers.add(last);
						pointerCount++;	
						event.actual_count++;
						//System.out.println(last.id + " , " + event.id);
				}}
				if (query.event_selection_strategy.equals("any")) {
					newLastEvents.add(event);
					added = true;
					number_of_events_in_curr_sec++;
				}
			} else {
				
				prev_sec = curr_sec;
				curr_sec = event.sec;
				number_of_events_in_prev_sec = number_of_events_in_curr_sec;
				number_of_events_in_curr_sec = 1;
				number_of_events_at_second.put(prev_sec,number_of_events_in_prev_sec);								
				
				// Update last events and draw pointers from event to all last events
				lastEvents.clear();
				lastEvents.addAll(newLastEvents);	
				
				int required_count = (number_of_events_in_prev_sec * query.getPercentage())/100;
				//System.out.println("count in " + prev_sec + " : " + count + " required: " + required_count);
				
				for (Event last : lastEvents) {
					if (!event.pointers.contains(last) && !last.marked && event.actual_count<required_count) {
						event.pointers.add(last);
						pointerCount++;
						event.actual_count++;
						//System.out.println(last.id + " , " + event.id);
				}}
				newLastEvents.clear();
				newLastEvents.add(event);
				added = true;				
			}			
			// Store the event in a stack or mark all events as incompatible with all future events
			if (added) {
				stack.add(event);
			} else {
				if (query.event_selection_strategy.equals("cont")) 
					for (Event e : stack) 
						e.marked = true;						
			}
			event = events.peek();
		}	
		number_of_events_at_second.put(curr_sec,number_of_events_in_prev_sec);
		//System.out.println(curr_sec + " " + number_of_events_in_prev_sec);
				
		// For each new last event, traverse the pointers to extract trends
		ArrayList<EventSequence> without_duplicates = new ArrayList<EventSequence>();
		for (Event lastEvent : newLastEvents) 
			if (!lastEvent.marked)
				traversePointers(lastEvent, new Stack<Event>(), without_duplicates);
		
		//System.out.println(without_duplicates);
		count = count.add(new BigInteger(without_duplicates.size()+""));
		
		// Aggregation step
		long start =  System.currentTimeMillis();
		int sum = 0;
		for (EventSequence seq : without_duplicates) {
			//System.out.println(seq.toString());
			for (Event e : seq.events) {
				sum += e.id;
			}			
		}
		long end =  System.currentTimeMillis();
		agg_time += end - start;
						
		memory.set(memory.get() + stack.size() + pointerCount + without_duplicates.size());
	}*/
	
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
	 	//System.out.println(without_duplicates.toString());
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
			Event new_event = elements.remove(0);  
			//!!! int id_of_last_predecessor = number_of_events_at_second.get(new_event.sec);
			//!!! int id_of_last_compatible_predecessor = (id_of_last_predecessor * query.getPercentage())/100;
			//System.out.println(new_event.id + " : " + id_of_last_predecessor + ", " + id_of_last_compatible_predecessor);
			
			ArrayList<EventSequence> rest = getCombinations(elements,obligatory);
			int limit = rest.size();
			
			for (int i=0; i<limit; i++) {			
				
				Event prev_event = rest.get(i).events.get(0);	
				//!!! if (prev_event.id > id_of_last_compatible_predecessor) break;
				
				boolean contained_in_pointers = new_event.pointers.contains(prev_event);
				//!!! boolean compatible = query.compatible(prev_event,new_event,id_of_last_compatible_predecessor);
								
				if ((query.event_selection_strategy.equals("any") || contained_in_pointers)) { // !!! && compatible) { 
								
					ArrayList<Event> events = new ArrayList<Event>();
					events.add(new_event);
					events.addAll(rest.get(i).events);		
					EventSequence seq = new EventSequence(events);
					rest.add(seq);
					//System.out.println(prev_event.id + " is predecessor of " + new_event.id);
				}				
			}
			with_duplicates.addAll(rest);
		}		
	    return with_duplicates;
	}	
}
