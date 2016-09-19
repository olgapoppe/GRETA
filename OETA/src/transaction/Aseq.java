package transaction;

import iogenerator.OutputFileGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;

public class Aseq extends Transaction {
	
	public Aseq (Window w,OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
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
	
	public void computeResults () {
		
		Set<String> substream_ids = window.substreams.keySet();
		
		for (String substream_id : substream_ids) {					
		 
			ArrayList<Event> events = window.substreams.get(substream_id);
			computeResults(events);
		}
	}
	
	public void computeResults (ArrayList<Event> events) {
		
		// Prefix counters per prefix length 
		HashMap<Integer,Integer> prefix_counters_in_previous_second = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> prefix_counters_in_current_second = new HashMap<Integer,Integer>();		
		int curr_sec = -1;
		int curr_length = 0;
		int count_per_substream = 0;
		
		for (Event event : events) {
			
			// Update current second, current length, and prefix counters
			if (curr_sec < event.sec) {
				
				curr_sec = event.sec;
				curr_length++;
				prefix_counters_in_current_second.put(curr_length,0);
				
				// Prefix counters in current second become prefix counters in previous second
				for (int length=1; length<=curr_length; length++) 
					prefix_counters_in_previous_second.put(length,prefix_counters_in_current_second.get(length));				
			} 
			// Each event updates prefix counters for each length and the final count
			count_per_substream = 0;
			for (int length=1; length<=curr_length; length++) {
				
				int count_of_new_matches = (length-1<=0) ? 1 : prefix_counters_in_previous_second.get(length-1);				
				int count_of_old_matches = prefix_counters_in_current_second.get(length);				
				int new_count_for_length = count_of_new_matches + count_of_old_matches;
				prefix_counters_in_current_second.put(length,new_count_for_length);	
				count_per_substream += new_count_for_length;
				//System.out.println("Event " + event.id + " length: " + length + " counts: " + count_of_new_matches + " " + count_of_old_matches );	
			}				
		}
		count += count_per_substream;
		total_mem.set(total_mem.get() + prefix_counters_in_previous_second.size() + prefix_counters_in_current_second.size());	
	}
}
