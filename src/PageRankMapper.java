import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		
		String line = value.toString();
		
		String[] inputline = line.split(" +");
		
//		System.out.println(Arrays.toString(inputline));
		System.out.println();
		

		
		for(int i=1; i<inputline.length-1; i++){
			
			float pr_value = Float.parseFloat(inputline[inputline.length-1]) / (inputline.length-2);
			
			String prVal = Float.toString(pr_value);
			String outgoing  = inputline[0];
			
			System.out.println("key=" + inputline[i] + ", value=" + outgoing + ", " + prVal);
			
			Text pair = new Text(Arrays.toString(new String[]{outgoing, prVal}));
			
			context.write(new Text(inputline[i]), pair);
		}
		
		//generate the last line of the output
		String[] outlinks = new String[inputline.length-2];
		int j = 0;
		for(int i=1; i<inputline.length-1; i++){
			outlinks[j] = inputline[i];
			j++;
		}
		
//		System.out.println(Arrays.toString(outlinks));
		System.out.print("key=" + inputline[0] + ", value=");
		for(String link : outlinks){
			System.out.print(link + " ");
		}
		System.out.println();
		
		
		Text outlinks_pair = new Text(Arrays.toString(outlinks));
		context.write(new Text(inputline[0]), outlinks_pair);
		
		
		
	
	
	}
  
  
	
	
	
	
	
	
	

	
}