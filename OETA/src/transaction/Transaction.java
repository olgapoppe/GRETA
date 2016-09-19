package transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;

public abstract class Transaction implements Runnable {
	
	public Stream stream;
	public CountDownLatch done;	
	AtomicLong total_cpu;
	AtomicInteger total_mem;
	public int count;
	
	public Transaction (Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		stream = str;	 
		done = d;
		total_cpu = time;
		total_mem = mem;
		count = 0;
	}
}
