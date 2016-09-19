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
import query.Query;
import event.*;
import scheduler.*;
 
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
	    OutputFileGenerator output = new OutputFileGenerator(path+outputfile); 
	   	    
	    // Print input parameters
	    System.out.println(	"Event type: " + type +
	    					"\nInput file: " + inputfile +
	    					"\nAlgorithm: " + algorithm +
	    					"\nESS: " + ess +
	    					"\nPredicate: " + predicate +
	    					"\nEvents per window: " + events_per_window +
							"\n----------------------------------");

		/*** SHARED DATA STRUCTURES ***/		
		AtomicInteger driverProgress = new AtomicInteger(-1);	
		Stream stream = new Stream(driverProgress);						
		CountDownLatch done = new CountDownLatch(1);
		long startOfSimulation = System.currentTimeMillis();
		AtomicInteger eventNumber = new AtomicInteger(0);
		AtomicLong total_cpu = new AtomicLong(0);	
		AtomicInteger total_memory = new AtomicInteger(0);
		
		/*** EXECUTORS ***/
		ExecutorService executor = Executors.newFixedThreadPool(3);
			
		/*** Create and start the event driver and the scheduler threads.
		 *   Driver reads from the file and writes into the event queue.
		 *   Scheduler reads from the event queue and submits event batches to the executor. ***/
		EventDriver driver = new EventDriver (type, input, stream, startOfSimulation, driverProgress, eventNumber, events_per_window);				
		
		Query query = new Query(ess, predicate); 
		Scheduler scheduler = new Scheduler (stream, algorithm, query,				   
				executor, driverProgress, done, total_cpu, total_memory, output, eventNumber, events_per_window);		
		
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
		
		System.out.println(
				"\nAvg CPU: " + total_cpu.get() +
				"\nAvg MEM: " + total_memory.get() + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
		  catch (IOException e1) { e1.printStackTrace(); }
	}	
}