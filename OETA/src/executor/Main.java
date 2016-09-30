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
	 * -type stock -path src/iofiles/ -file stream.txt -ess any -pred none -epw 15 -algo eta
	 * -type cluster -path ../../../Dropbox/DataSets/Cluster/ -file cluster.txt -ess any -pred none -epw 443947 -algo eta
	 * -type stock -path ../../../Dropbox/DataSets/Stock/ -file replicated.txt -ess any -pred none -epw 3520 -algo eta
	 * -type position -path ../../../Dropbox/DataSets/LR/InAndOutput/1xway/last_10_min/ -file 0;2.dat -ess any -pred none -epw 492654 -algo eta
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
	    String path = "OETA/src/iofiles/";
		String inputfile = "stream.txt";
		
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
							"\n----------------------------------");	    

		/*** SHARED DATA STRUCTURES ***/		
	    CountDownLatch done = new CountDownLatch(1);
		AtomicLong total_cpu = new AtomicLong(0);	
		AtomicInteger total_memory = new AtomicInteger(0);
		
		/*** STREAM PARTITIONING ***/
		StreamPartitioner sp = new StreamPartitioner(type, input, events_per_window);
		Stream stream = sp.partition();
		
		/*** EXECUTORS ***/
		Query query = new Query (ess,predicate);
		ExecutorService executor = Executors.newFixedThreadPool(10);
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
		System.out.println( "\nCPU: " + total_cpu.get() + "\nMEM: " + total_memory.get() + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
	}		
}