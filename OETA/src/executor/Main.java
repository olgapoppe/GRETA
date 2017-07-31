package executor;

//import java.nio.file.Path;
//import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import query.Query;
import event.*;
import transaction.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input file -> Driver -> Scheduler -> Executor -> Output files 
	 * -type stock -path ../../../Dropbox/DataSets/Stock/ -file sorted.txt -pred none -epw 500 -ess any -algo sase
	 * -type activity -path ../../../Dropbox/DataSets/PhysicalActivity/ -file 114.dat -pred none -epw 100 -ess cont -algo sase
	 * -type transport -path ../../../Dropbox/DataSets/PublicTransport/ -file transport.txt -pred none -epw 100 -ess next -algo sase
	 * 
	 * -type cluster -path ../../../Dropbox/DataSets/Cluster/ -file cluster.txt -pred 50% -epw 443947 -algo greta	 
	 * -type position -path ../../../Dropbox/DataSets/LR/InAndOutput/1xway/ -file position.dat -pred 50% -epw 1000 -algo greta
	 * -type stock -path src/iofiles/ -file stream.txt -pred 100% -epw 10 -algo hcet -graphlets 2
	 */
	public static void main (String[] args) { 
		
		try {
		
		/*** Print current time ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));
	    
	    /* Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s); */
	    
	    /*** Input and output ***/
	    // Set default values
	    String type = "stock";
	    String path = "src/iofiles/";
		String inputfile = "stream.txt";
		
		String algorithm = "greta";
		String ess = "any";
		String predicate = "none";		
		int events_per_window = Integer.MAX_VALUE;
		int negated_events_per_window = 0;
		//int number_of_graphlets = 1;
				
		// Read input parameters
	    for (int i=0; i<args.length; i++){
	    	if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-file")) 		inputfile = args[++i];
			if (args[i].equals("-algo")) 		algorithm = args[++i];
			if (args[i].equals("-ess")) 		ess = args[++i];
			if (args[i].equals("-pred")) 		predicate = args[++i];
			if (args[i].equals("-epw")) 		events_per_window = Integer.parseInt(args[++i]);
			if (args[i].equals("-nepw")) 		negated_events_per_window = Integer.parseInt(args[++i]);
			//if (args[i].equals("-graphlets")) 	number_of_graphlets = Integer.parseInt(args[++i]);
		}	    
	    
	    // Make sure the input parameters are correct
	    if (algorithm.equals("aseq") && (!ess.equals("any") || !predicate.equals("none"))) {
	    	System.err.println("ASEQ works under skip-till-any-match without predicates on adjacent events");
	    	return;
	    }
	    	   	    
	    // Print input parameters
	    String input = path + inputfile;
	    System.out.println(	"Event type: " + type +
	    					//"\nInput file: " + input +
	    					"\nAlgorithm: " + algorithm +
	    					"\nESS: " + ess +
	    					"\nPredicate: " + predicate +
	    					"\nEvents per window: " + events_per_window +
	    					//"\nNegated events per window: " + negated_events_per_window +
	    					//"\nNumber of graphlets: " + number_of_graphlets +
							"\n----------------------------------");	    

		/*** SHARED DATA STRUCTURES ***/		
	    CountDownLatch done = new CountDownLatch(1);
		AtomicLong latency = new AtomicLong(0);	
		AtomicInteger memory = new AtomicInteger(0);
		
		/*** STREAM PARTITIONING ***/
		StreamPartitioner sp = new StreamPartitioner(type, input, events_per_window);
		Stream stream = sp.partition();
		
		/*** EXECUTORS ***/
		Query query = new Query (ess,predicate);
		ExecutorService executor = Executors.newFixedThreadPool(3);
		Transaction transaction;
		if (algorithm.equals("greta")) {
			transaction = new Greta(stream,query,done,latency,memory,negated_events_per_window,false);
		} else {
		if (algorithm.equals("greta-ct")) {
			transaction = new Greta(stream,query,done,latency,memory,negated_events_per_window,true);
		} else {
		/*if (algorithm.equals("hcet")) {
			transaction = new HCet(stream,query,done,latency,memory,number_of_graphlets,negated_events_per_window);
		} else {	
		if (algorithm.equals("tcet")) {
			transaction = new TCet(stream,query,done,latency,memory,negated_events_per_window);
		} else {*/
		if (algorithm.equals("sase")) {
			transaction = new Sase(stream,query,done,latency,memory,negated_events_per_window);
		} else {
			transaction = new Aseq(stream,done,latency,memory);
		}}}
		executor.execute(transaction);
				
		/*** Wait till all input events are processed and terminate the executor ***/
		done.await();		
		executor.shutdown();			
		System.out.println( "\nLatency: " + latency.get() + "\nMemory: " + memory.get() + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
	}		
}