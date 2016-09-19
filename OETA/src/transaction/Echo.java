package transaction;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;
import iogenerator.*;

public class Echo extends Transaction {
	
	public Echo (Stream str, int l, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(str,l,o,tn,time,mem);
	}
	
	public void run() {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		transaction_number.countDown();
	}
	
	/*** Print all events in each sub-stream ***/
	public void computeResults() {	

		Set<String> substream_ids = stream.substreams.keySet();		
		for (String substream_id : substream_ids) {					
		 
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			Event event = events.poll();
			
			while (event != null && event.sec<=limit) {
				System.out.println("Executor: " + event.toString());
				event = events.poll();
			}
		}		
	}
}
