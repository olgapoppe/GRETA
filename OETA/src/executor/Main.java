package executor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import iogenerator.*;
import event.*;
import scheduler.*;
 
// -path src/iofiles/ -from 1 -to 3 -wl 3 -ws 3

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
		
		boolean realtime = true;
		int firstsec = 0;
	    int lastsec = 0;
		String algorithm = "echo";
		
		int window_length = 0;
		int window_slide = 0;
		String predicate = "none";
		String ess = "any";
				
		// Read input parameters
	    for (int i=0; i<args.length; i++){
	    	if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-file")) 		inputfile = args[++i];
			if (args[i].equals("-realtime")) 	realtime = Integer.parseInt(args[++i]) == 1;
			if (args[i].equals("-from")) 		firstsec = Integer.parseInt(args[++i]);
			if (args[i].equals("-to")) 			lastsec = Integer.parseInt(args[++i]);
			if (args[i].equals("-algo")) 		algorithm = args[++i];
			if (args[i].equals("-wl")) 			window_length = Integer.parseInt(args[++i]);
			if (args[i].equals("-ws")) 			window_slide = Integer.parseInt(args[++i]);
			if (args[i].equals("-pred")) 		predicate = args[++i];
			if (args[i].equals("-ess")) 		ess = args[++i];
			
		}
	    String input = path + inputfile;
	    OutputFileGenerator output = new OutputFileGenerator(path+outputfile); 
	   	    
	    // Print input parameters
	    System.out.println(	"Event type: " + type +
	    					"\nInput file: " + inputfile +
	    					"\nReal time: " + realtime +
	    					"\nStream from " + firstsec + " to " + lastsec +
	    					"\nAlgorithm: " + algorithm +
	    					"\nESS: " + ess +
	    					"\nPredicate: " + predicate +
	    					"\nWindow length: " + window_length + 
							"\nWindow slide: " + window_slide +							
							"\n----------------------------------");

		/*** SHARED DATA STRUCTURES ***/		
		AtomicInteger driverProgress = new AtomicInteger(-1);	
		EventQueue eventqueue = new EventQueue(driverProgress);						
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();
		AtomicInteger eventNumber = new AtomicInteger(0);
		AtomicLong total_cpu = new AtomicLong(0);	
		AtomicInteger total_memory = new AtomicInteger(0);
		
		/*** EXECUTORS ***/
		int window_number = (lastsec-firstsec)/window_slide + 1;
		ExecutorService executor = Executors.newFixedThreadPool(3);
			
		/*** Create and start the event driver and the scheduler threads.
		 *   Driver reads from the file and writes into the event queue.
		 *   Scheduler reads from the event queue and submits event batches to the executor. ***/
		EventDriver driver = new EventDriver (type, input, realtime, lastsec, eventqueue, startOfSimulation, driverProgress, eventNumber);				
				
		Scheduler scheduler = new Scheduler (eventqueue, firstsec, lastsec, algorithm,
				ess, predicate, window_length, window_slide,   
				executor, driverProgress, done, total_cpu, total_memory, output);		
		
		Thread prodThread = new Thread(driver);
		prodThread.setPriority(10);
		prodThread.start();
		
		Thread consThread = new Thread(scheduler);
		consThread.setPriority(10);
		consThread.start();		
				
		/*** Wait till all input events are processed and terminate the executor ***/
		done.await();		
		executor.shutdown();	
		output.file.close();
		
		System.out.println(//"Event number: " + eventNumber.get() +
				"\nAvg CPU: " + total_cpu.get()/window_number +
				//"\nThroughput: " + eventNumber.get()/processingTime.get() +
				"\nAvg MEM: " + total_memory.get()/window_number + "\n");
				//"\nExecutor is done." +
				//"\nMain is done.");
			
		} catch (InterruptedException e) { e.printStackTrace(); }
		  catch (IOException e1) { e1.printStackTrace(); }
	}	
}