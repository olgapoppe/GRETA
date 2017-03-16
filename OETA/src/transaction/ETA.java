package transaction;

import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import query.*;

public class ETA extends Transaction {
	
	Query query;
	Long agg_time;
		
	public ETA (Stream str, Query q, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		super(str,d,time,mem);
		query = q;
		agg_time = new Long(0);
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;		
		total_cpu.set(total_cpu.get() + duration);				
		done.countDown();
	}

	public void computeResults () {
		
		Set<String> substream_ids = stream.substreams.keySet();					
		for (String substream_id : substream_ids) {					
		 
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			Graph graph = new Graph();
			if (query.compressible()) {
				graph = graph.getCompressedGraph(events, query);
			} else {
				if (query.getPercentage() < 100) {
					graph = graph.getCompleteGraphForPercentage(events, query, agg_time);
				} else {
					graph = graph.getCompleteGraph(events, query, agg_time);
				}
			} 
					
			count = count.add(new BigInteger(graph.final_count + ""));
			total_mem.set(total_mem.get() + graph.nodeNumber);
			
			//System.out.println("Sub-stream id: " + substream_id + " with count " + graph.final_count);
		}
		System.out.println("Count: " + count + "\nAgg time: " + agg_time);
	}
}
