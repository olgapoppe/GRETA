package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class InputFileGenerator {
	
	public static void main (String[] args) {
		
		/*** Set default input parameters ***/
		String file = "stream";
		int last_sec = 3;
		int total_rate = 5;
		int matched_rate = 3;
		int lambda = 100;
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++){
			if (args[i].equals("-file")) 	file = args[++i];
			if (args[i].equals("-sec")) 	last_sec = Integer.parseInt(args[++i]);
			if (args[i].equals("-trate"))   total_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-mrate")) 	matched_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-lambda")) 	lambda = Integer.parseInt(args[++i]);
		}
				
		/*** Generate input event stream ***/
		try {		
			// Open the output file
			String output_file_name = "src\\iofiles\\" + file + ".txt"; 
			File output_file = new File(output_file_name);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
		
			// Generate the input event stream
			for (int sec=1; sec<=last_sec; sec++)
				for (int count=1; count<=total_rate; count++)
					generate_event(output, sec, count, matched_rate, lambda);
					
			// Close the file
			output.close();
			
		} catch (IOException e) { e.printStackTrace(); }	
	}
	
	public static void generate_event (BufferedWriter output, int sec, int count, int matched_rate, double lambda) {
		
		Random random = new Random();
		        
        // Sector identifier determines event relevance: 1 for relevant, 0 for irrelevant
        int sector = (count<=matched_rate) ? 1 : 0;
        
        // Company identifier is a random number in a range between min and max
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
        String event = sec + "," + sector + "," + company + "," + price + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        //System.out.println("Time " + sec + " sector " + sector + " company " + company + " price " + price);
	}
}	
	
	
	