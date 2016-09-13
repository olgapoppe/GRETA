package transaction;

import iogenerator.OutputFileGenerator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.Window;

public class CompleteETAgraph extends Transaction {
	
	public CompleteETAgraph (Window w,OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
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
		
	}
}
