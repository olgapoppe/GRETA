package transaction;

import java.util.List;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;

public class Sase extends Transaction {
	
	public Sase (Window w,OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(w,o,tn,time,mem);
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		System.out.println("Window " + window.id + " has " + count + " results.");		
		//writeOutput2File();		
		transaction_number.countDown();
	}
	
	public void computeResults() {
		
		// Initiate data structures: stack, last events
		Stack<Event> stack = new Stack<Event>();
		ArrayList<Event> lastEvents = new ArrayList<Event>();
		ArrayList<Event> newLastEvents = new ArrayList<Event>();
		int curr_sec = -1;
		int pointerCount = 0;
		
		for (Event event : window.events) {
			
			if (!event.pointers.containsKey(window.id)) {
				ArrayList<Event> new_pointers = new ArrayList<Event>();
				event.pointers.put(window.id, new_pointers);
			}
			ArrayList<Event> pointers = event.pointers.get(window.id);
									
			// Store pointers to its predecessors
			if (event.sec == curr_sec) {
				for (Event last : lastEvents) {
					
					if (pointers == null) System.out.println("Pointers null");
					
					if (!pointers.contains(last)) {
						pointers.add(last);
						pointerCount++;
				}}
				newLastEvents.add(event);
			} else {
				lastEvents.clear();
				lastEvents.addAll(newLastEvents);
				for (Event last : lastEvents) {
					if (!pointers.contains(last)) {
						pointers.add(last);
						pointerCount++;
				}}
				newLastEvents.clear();
				newLastEvents.add(event);
				curr_sec = event.sec;
			}			
			// Store the event in a stack
			stack.add(event);
			//System.out.println(window.id + " " + event.toStringWithPointers(window.id));
		}		
		// For each new last event, traverse the pointers to extract CETs
		int maxSeqLength = 0;
		for (Event lastEvent : newLastEvents) {
			maxSeqLength = traversePointers(lastEvent, new Stack<Event>(), maxSeqLength);
		}
		int memory = stack.size() + pointerCount + maxSeqLength;
		total_mem.set(total_mem.get() + memory);
		//if (total_mem.get() < memory) total_mem.getAndAdd(memory);
	}
	
	// DFS in the stack
	public int traversePointers (Event event, Stack<Event> current_trend, int maxSeqLength) {       
			
		current_trend.push(event);
		System.out.println("pushed " + event.id);
		
		 ArrayList<Event> input = new ArrayList<Event>();
	     input.addAll(current_trend);
	     List<String> incomplete_trends = getIncompleteTrends(input);
	     count += incomplete_trends.size();
	     //System.out.println(incomplete_trends);
		
		ArrayList<Event> pointers = event.pointers.get(window.id);
	        
		/*** Base case: We hit the end of the graph. Output the current CET. ***/
	    if (pointers.isEmpty()) {  
	    	
	    	//System.out.println("complete trend " + current_trend.toString());
	        if (maxSeqLength < current_trend.size()) maxSeqLength = current_trend.size();
	        
	       /* ArrayList<Event> input = new ArrayList<Event>();
	        input.addAll(current_trend);
	        List<String> incomplete_trends = getIncompleteTrends(input);
	       	count += incomplete_trends.size();*/
				
	    } else {
	    /*** Recursive case: Traverse the following nodes. ***/     	
	       	for(Event previous : pointers) {        		
	       		//System.out.println("following of " + node.event.id + " is " + following.event.id);
	       		maxSeqLength = traversePointers(previous,current_trend,maxSeqLength);        		
	       	}        	
	    }
	    Event top = current_trend.pop();
	    //System.out.println("popped " + top.event.id);
	    	    
	    return maxSeqLength;
	}	
	
	public static List<String> getIncompleteTrends(List<Event> elements) {

	    ArrayList<String> results = new ArrayList<String>();
	    
	    Event obligatory = elements.remove(elements.size()-1);
	    results.add(obligatory.id+"");
	   
	    if (!elements.isEmpty()) {
	    List<String> rest = getCombinations(elements,obligatory);
	    results.addAll(rest);   
	    }
	    System.out.println(results.toString());
	    
	    return results;
	}
	
	public static List<String> getCombinations(List<Event> elements, Event obligatory) {

	    //return list with empty String
	    if(elements.size() == 0){
	        List<String> allLists = new ArrayList<String>();
	        allLists.add("");
	        return allLists ;
	    }

	    Event first_ele = elements.remove(0);
	    	    
	    List<String> rest = getCombinations(elements,obligatory);
	    int restsize = rest.size();
	    //Mapping the first_ele with each of the rest of the elements.
	    for (int i = 0; i < restsize; i++) {
	        String ele = first_ele.id + "," + rest.get(i) + "," + obligatory.id;
	        rest.add(ele);
	        //System.out.println(ele);
	    }

	    return rest;
	}
	
}
