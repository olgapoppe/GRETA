package scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;
import transaction.*;
import query.*;

public class Scheduler implements Runnable {
	
	final Stream stream;			
	String algorithm;		
	Query query;
		
	ExecutorService executor;
	AtomicInteger drProgress;
	CountDownLatch transaction_number;
	CountDownLatch done;
	
	AtomicLong total_cpu;	
	AtomicInteger total_memory;
	OutputFileGenerator output;
	AtomicInteger eventNumber;
	int events_per_window;
	
	public Scheduler (Stream str, String algo, Query q,			
			ExecutorService exe, AtomicInteger dp, CountDownLatch d, AtomicLong time, AtomicInteger mem, OutputFileGenerator o,
			AtomicInteger eN, int epw) {	
		
		stream = str;				
		algorithm = algo;		
		query = q;
								
		executor = exe;
		drProgress = dp;
		transaction_number = new CountDownLatch(1);
		done = d;
		
		total_cpu = time;
		total_memory = mem;
		output = o;	
		eventNumber = eN;
		events_per_window = epw;
	}
	
	/**
	 * As long as not all events are processed, extract events from the event queue and execute them.
	 */	
	public void run() {	
		
		/*** Set local variables ***/
		int limit = 1;		
									
		/*** Get the permission to schedule current batch ***/
		try {
			while (stream.getDriverProgress(limit)) {
			
				execute(stream,limit);
			
				if (eventNumber.get() >= events_per_window) {
					break;										
				} 
				limit++;
				transaction_number.await();
				transaction_number = new CountDownLatch(1);				
			}
			
			/*** Terminate ***/
			transaction_number.await(); 
			done.countDown();	
			System.out.println("Scheduler is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }		
	}	
	
	public void execute(Stream stream, int limit) {
		
		Transaction transaction;
		if (algorithm.equals("eta")) {
			transaction = new ETA(stream,limit,query,output,transaction_number,total_cpu,total_memory);
		} else {
		if (algorithm.equals("aseq")) {
			transaction = new Aseq(stream,limit,output,transaction_number,total_cpu,total_memory);
		} else {
		if (algorithm.equals("sase")) {
			transaction = new Sase(stream,limit,query,output,transaction_number,total_cpu,total_memory);
		} else {
			transaction = new Echo(stream,limit,output,transaction_number,total_cpu,total_memory);
		}}}
		executor.execute(transaction);	
	}	
}