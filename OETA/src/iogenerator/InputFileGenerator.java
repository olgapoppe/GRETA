package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class InputFileGenerator {
	
	public static void main (String[] args) {
		
		/*** Set default input parameters ***/
		String type = "stock";
		String path = "OETA/src/iofiles/";
		String file = "stream.txt";
		int last_sec = 3;
		int total_rate = 5;
		int matched_rate = 3;
		int lambda = 100;
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-type")) 	type = args[++i];
			if (args[i].equals("-path")) 	path = args[++i];
			if (args[i].equals("-file")) 	file = args[++i];
			if (args[i].equals("-sec")) 	last_sec = Integer.parseInt(args[++i]);
			if (args[i].equals("-trate"))   total_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-mrate")) 	matched_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-lambda")) 	lambda = Integer.parseInt(args[++i]);
		}
				
		/*** Generate input event stream ***/
		try {		
			// Open the output file
			String output_file_name = path + file; 
			File output_file = new File(output_file_name);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
		
			// Generate input event stream
			if (type.equals("stock")) {			
				for (int sec=1; sec<=last_sec; sec++) 
					for (int count=1; count<=total_rate; count++) 
						generate_stock_stream(output, sec, count, matched_rate, lambda);
			} else {
				for (int sec=1; sec<=last_sec; sec++) 
					for (int count=1; count<=total_rate; count++) 			
						generate_cluster_stream(output, sec, count, matched_rate, lambda);
			}					
			// Close the file
			output.close();
			
		} catch (IOException e) { e.printStackTrace(); }	
	}
	
	public static void generate_stock_stream (BufferedWriter output, int sec, int count, int matched_rate, double lambda) {
		
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
        String event = count + "," + sec + "," + sector + "," + company + "," + price + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        System.out.println("id " + count + " sec " + sec + " sector " + sector + " company " + company + " price " + price);
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
}	
	
	
	