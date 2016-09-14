package transaction;

import iogenerator.OutputFileGenerator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import query.*;

public class CompleteETAgraph extends Transaction {
	
	Query query;
	
	public CompleteETAgraph (Window w, Query q, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		super(w,o,tn,time,mem);
		query = q;
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
		
		Graph graph = new Graph(); 
		graph = graph.getCompleteGraph(window.events, query);
		count = graph.final_count;
		total_mem.set(total_mem.get() + graph.nodeNumber + graph.edgeNumber);	
	}
}
