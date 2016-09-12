package transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import iogenerator.*;

public abstract class Transaction implements Runnable {
	
	public Window window;
	OutputFileGenerator output;
	public CountDownLatch transaction_number;	
	AtomicLong total_cpu;
	AtomicInteger total_mem;
	public int count;
	
	public Transaction (Window w, OutputFileGenerator o, CountDownLatch tn, AtomicLong time, AtomicInteger mem) {		
		window = w;		
		output = o; 
		transaction_number = tn;
		total_cpu = time;
		total_mem = mem;
		count = 0;
	}
}
