package transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;

public class Echo extends Transaction {
	
	HashSet<TreeSet<StockEvent>> results;
	Window window; 
	
	public Echo (ArrayList<StockEvent> b, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem, Window w) {		
		super(b,o,tn,time,mem);
		results = new HashSet<TreeSet<StockEvent>>();
		window = w;
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		total_cpu.set(total_cpu.get() + duration);
		
		// writeOutput2File();		
		transaction_number.countDown();
	}
	
	public void computeResults() {
		
		for (StockEvent event: batch)
			System.out.println(event.toString());
	}
}
