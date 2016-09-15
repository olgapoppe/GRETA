package transaction;

import iogenerator.OutputFileGenerator;
import java.util.HashMap;
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
		
		// Prefix counters per prefix length 
		HashMap<Integer,Integer> prefix_counters_in_previous_second = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> prefix_counters_in_current_second = new HashMap<Integer,Integer>();		
		int curr_sec = -1;
		int curr_length = 0;
		
		for (Event event : window.events) {
			
			// Update current second, current length, and prefix counters
			if (curr_sec < event.sec) {
				
				curr_sec = event.sec;
				curr_length++;
				prefix_counters_in_current_second.put(curr_length,0);
				
				// Prefix counters in current second become prefix counters in previous second
				for (int length=1; length<=curr_length; length++) 
					prefix_counters_in_previous_second.put(length,prefix_counters_in_current_second.get(length));				
			} 
			// Update prefix counters and final count
			count = 0;
			for (int length=1; length<=curr_length; length++) {
				
				int count_of_new_matches = (length-1<=0) ? 1 : prefix_counters_in_previous_second.get(length-1);				
				int count_of_old_matches = prefix_counters_in_current_second.get(length);				
				int new_count_for_length = count_of_new_matches + count_of_old_matches;
				prefix_counters_in_current_second.put(length,new_count_for_length);	
				count += new_count_for_length;
				//System.out.println("Event " + event.id + " length: " + length + " counts: " + count_of_new_matches + " " + count_of_old_matches );	
			}				
		}
	}
}
