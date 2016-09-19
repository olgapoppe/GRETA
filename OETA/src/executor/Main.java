package executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import iogenerator.*;
import query.Query;
import event.*;
import transaction.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input file -> Driver -> Scheduler -> Executor -> Output files 
	 * @param args: 
	 */
	public static void main (String[] args) { 
		
		try {
		
		/*** Print current time ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));
	    
	    Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s);
	    
	    /*** Input and output ***/
	    // Set default values
	    String type = "stock";
	    String path = "OETA/src/iofiles/";
		String inputfile = "stream.txt";
		String outputfile = "sequences.txt";		
		
		String algorithm = "echo";
		
		String ess = "any";
		String predicate = "none";		
		int events_per_window = Integer.MAX_VALUE;
				
		// Read input parameters
	    for (int i=0; i<args.length; i++){
	    	if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-file")) 		inputfile = args[++i];
			if (args[i].equals("-algo")) 		algorithm = args[++i];
			if (args[i].equals("-ess")) 		ess = args[++i];
			if (args[i].equals("-pred")) 		predicate = args[++i];
			if (args[i].equals("-epw")) 		events_per_window = Integer.parseInt(args[++i]);
		}
	    String input = path + inputfile;
	    	   	    
	    // Print input parameters
	    System.out.println(	"Event type: " + type +
	    					"\nInput file: " + input +
	    					"\nAlgorithm: " + algorithm +
	    					"\nESS: " + ess +
	    					"\nPredicate: " + predicate +
	    					"\nEvents per window: " + events_per_window +
							"\n----------------------------------");

		/*** SHARED DATA STRUCTURES ***/		
	    CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();
		AtomicLong total_cpu = new AtomicLong(0);	
		AtomicInteger total_memory = new AtomicInteger(0);
		
		/*** STREAM PARTITIONING ***/
		StreamPartitioner sp = new StreamPartitioner(type, input, events_per_window);
		Stream stream = sp.partition();
		
		/*** EXECUTORS ***/
		Query query = new Query (ess,predicate);
		ExecutorService executor = Executors.newFixedThreadPool(3);
		Transaction transaction;
		if (algorithm.equals("eta")) {
			transaction = new ETA(stream,query,done,total_cpu,total_memory);
		} else {
		if (algorithm.equals("aseq")) {
			transaction = new Aseq(stream,done,total_cpu,total_memory);
		} else {
		if (algorithm.equals("sase")) {
			transaction = new Sase(stream,query,done,total_cpu,total_memory);
		} else {
			transaction = new Echo(stream,done,total_cpu,total_memory);
		}}}
		executor.execute(transaction);
				
		/*** Wait till all input events are processed and terminate the executor ***/
		done.await();		
		executor.shutdown();	
		
		System.out.println(
				"\nAvg CPU: " + total_cpu.get() +
				"\nAvg MEM: " + total_memory.get() + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
	}		
}