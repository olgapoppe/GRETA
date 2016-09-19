package transaction;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;
import iogenerator.*;

public class Echo extends Transaction {
	
	public Echo (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(w,o,tn,time,mem);
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		// writeOutput2File();		
		transaction_number.countDown();
	}
	
	public void computeResults() {	

		Set<String> substream_ids = window.substreams.keySet();
		
		for (String substream_id : substream_ids) {					
		 
			ArrayList<Event> events = window.substreams.get(substream_id);
			for (Event event : events) System.out.println(event.toString());
		}		
	}
}
