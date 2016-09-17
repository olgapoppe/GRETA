package iogenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.Stack;
import event.*;

public class InputFileGenerator {
	
	public static void main (String[] args) {
		
		/*** Set default input parameters ***/
		String type = "stock";
		boolean real = false;
		String path = "src/iofiles/";
		String i_file_1 = "stream.txt";
		String i_file_2 = "stream.txt";
		String o_file = "stream.txt";
		
		int last_sec = 3;
		int total_rate = 5;
		int matched_rate = 3;
		int lambda = 100;
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-real")) 		real = Integer.parseInt(args[++i])==1;
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-i_file_1")) 	i_file_1 = args[++i];
			if (args[i].equals("-i_file_2")) 	i_file_2 = args[++i];
			if (args[i].equals("-o_file")) 		o_file = args[++i];
			if (args[i].equals("-sec")) 		last_sec = Integer.parseInt(args[++i]);
			if (args[i].equals("-trate"))   	total_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-mrate")) 		matched_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-lambda")) 		lambda = Integer.parseInt(args[++i]);
		}
				
		/*** Generate input event stream ***/
		try {		
			// Open the output file
			String output_file_name = path + o_file; 
			File output_file = new File(output_file_name);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
		
			// Generate input event stream
			if (type.equals("stock")) {		
				if (real) {
					String input_1 = path + i_file_1;
					//readInReverseOrder(input_1, output);
					String input_2 = path + i_file_2;
					twoInputsOneOutput (input_1, input_2, output);
				} else {
					int id = 1;
					for (int sec=1; sec<=last_sec; sec++) 
						for (int count=1; count<=total_rate; count++) 
							generate_stock_stream(output, id++, sec, count, matched_rate, lambda);	
				}
			} else {
				for (int sec=1; sec<=last_sec; sec++) 
					for (int count=1; count<=total_rate; count++) 			
						generate_cluster_stream(output, sec, count, matched_rate, lambda);
			}					
			// Close the file
			output.close();
			System.out.println("Done!");
			
		} catch (IOException e) { e.printStackTrace(); }	
	}
	
	public static void generate_stock_stream (BufferedWriter output, int id, int sec, int count, int matched_rate, double lambda) {
		
		Random random = new Random();
		        
        // Sector identifier determines event relevance: 1 for relevant, 0 for irrelevant
        int sector = (count <= matched_rate) ? 1 : 0;
        
        // Company identifier is a random value in a range between min and max
        int min = 1;
        int max = 3;
        int company = random.nextInt((max - min) + 1) + min;        
        
        // Poisson distribution of price
        double limit = Math.exp(-lambda);
		double prod = random.nextDouble();
        int price;
        for (price = 0; prod >= limit; price++)
            prod *= random.nextDouble();
        
        // Save this event in the file
        String event = id + "," + sec + "," + sector + "," + company + "," + price + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        System.out.println("id " + id + " sec " + sec + " sector " + sector + " company " + company + " price " + price);
	}
	
	public static void generate_cluster_stream (BufferedWriter output, int sec, int count, int matched_rate, double lambda) {
		
		Random random = new Random();
		        
        // Mapper identifier determines event relevance: 1 for relevant, 0 for irrelevant
        int mapper = (count <= matched_rate) ? 1 : 0;
        
        // Job identifier, cpu and memory measurements are random values in a range between min and max
        int min = 1;
        int max = 10;
        int job = random.nextInt((max - min) + 1) + min;       
        
        int min2 = 1;
        int max2 = 1000;
        int cpu = random.nextInt((max2 - min2) + 1) + min2;
        int memory = random.nextInt((max2 - min2) + 1) + min2;
        
        // Poisson distribution of load
        double limit = Math.exp(-lambda);
		double prod = random.nextDouble();
        int load;
        for (load = 0; prod >= limit; load++)
            prod *= random.nextDouble();
        
        // Save this event in the file
        String event = count + "," + sec + "," + mapper + "," + job + "," + cpu + "," + memory + "," + load + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        System.out.println("id " + count + "sec " + sec + " mapper " + mapper + " job " + job + " cpu " + cpu + " mem " + memory + " load " + load);
	}
	
	public static void readInReverseOrder(String input_file_name, BufferedWriter output) throws IOException {

		//String input = "../../stock_data/TimeAndSales_AMAT_210019836.txt";
	    BufferedReader br = null;

	    try {
	        br = new BufferedReader(new FileReader(input_file_name));
	        Stack<String> lines = new Stack<String>();
	        String line = br.readLine();
	        
	        while(line != null) {
	            lines.push(line);
	            line = br.readLine();
	        }

	        while(!lines.empty()) 
	        	 output.append(lines.pop() + "\n");	        

	    } finally {
	        if(br != null) {
	            try { br.close(); } catch(IOException e) { }
	        }
	    }
	}
	
	/****************************************************************************
	 * Open 2 input files and 1 output file, call the method and close all files.
	 * @param first input file
	 * @param second input file
	 * @param output file
	 */
	public static void twoInputsOneOutput (String inputfilename1, String inputfilename2, BufferedWriter output) {
		
		Scanner input1 = null;
		Scanner input2 = null;
		try {		
			/*** Input file ***/
			File input_file_1 = new File(inputfilename1);
			File input_file_2 = new File(inputfilename2);
			input1 = new Scanner(input_file_1);  			
			input2 = new Scanner(input_file_2);
					
			/*** Call the method ***/            
            merge(input1,input2,output);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Merges 2 sorted files input1 and input2 into one sorted file output. The files are sorted by time stamps. 
	 * @param input1
	 * @param input2
	 * @param output
	 */
	public static void merge (Scanner input1, Scanner input2, BufferedWriter output) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		StockEvent event1 = StockEvent.parse2(eventString1);
		StockEvent event2 = StockEvent.parse2(eventString2);
		int count = 0; 
		
		try {
			
			while (event1 != null && event2 != null) {
					
				if (event1.sec < event2.sec) {				
					
					// Write event1
					output.write(eventString1 + "\n");
						
					// Reset event1
					if (input1.hasNextLine()) {
						eventString1 = input1.nextLine();
						event1 = StockEvent.parse2(eventString1);
					} else {
						event1 = null;
					}
				} else {				
					
					// Write event2
					output.write(eventString2 + "\n");
				
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = StockEvent.parse2(eventString2);
					} else {
						event2 = null;
					}
				}
				count++;
			}
			if (event1 == null) {
				while (event2 != null) {
					// Write event2
					output.write(eventString2 + "\n");
				
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = StockEvent.parse2(eventString2);
					} else {
						event2 = null;
					}
					count++;
				}				
			}
			if (event2 == null) {
				while (event1 != null) {
					// Write event1
					output.write(eventString1 + "\n");
				
					// Reset event1
					if (input1.hasNextLine()) {
						eventString1 = input1.nextLine();
						event1 = StockEvent.parse2(eventString1);
					} else {
						event1 = null;
					}
					count++;
				}
			}				
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count);
	}
}	
	
	
	