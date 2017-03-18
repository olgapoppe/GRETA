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
	 * -type cluster -path ../../../Dropbox/DataSets/Cluster/ -file cluster.txt -pred 50% -epw 443947 -algo greta
	 * -type stock -path ../../../Dropbox/DataSets/Stock/ -file replicated.txt -pred 50% -epw 3520 -algo greta
	 * -type position -path ../../../Dropbox/DataSets/LR/InAndOutput/1xway/last_10_min/ -file 0;2.dat -pred 50% -epw 492654 -algo greta
	 */
	public static void main (String[] args) { 
		
		try {
		
		/*** Print current time ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));
	    
	    /*Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s);*/
	    
	    /*** Input and output ***/
	    // Set default values
	    String type = "stock";
	    String path = "src/iofiles/";
		String inputfile = "stream.txt";
		
		String algorithm = "greta";
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
	    					//"\nESS: " + ess +
	    					"\nPredicate: " + predicate +
	    					"\nEvents per window: " + events_per_window +
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
			transaction = new Greta(stream,query,done,latency,memory);
		} else {
		if (algorithm.equals("cet")) {
			transaction = new Cet(stream,query,done,latency,memory);
		} else {
		if (algorithm.equals("sase")) {
			transaction = new Sase(stream,query,done,latency,memory);
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