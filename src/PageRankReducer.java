import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		float pr_total = 0;
		
		for(Text value : values){

			//convert value back to array
			String value_string = value.toString();
			String array_string = value_string.substring(1, value_string.length()-1);
			String[] value_array = array_string.split(", ");
			
			System.out.print("MAPPER: ");
			System.out.println(Arrays.toString(value_array));
			
			//get the PR Score from the array (last element)
			try{
				float pr_score = Float.parseFloat(value_array[value_array.length-1]);
				pr_total += pr_score;
			}catch(Exception e){
				
			}
			
			
		}
		
		System.out.println("\n---------------");
		context.write(key, new Text(Float.toString(pr_total)));
	  
	
	}
  
  
  
  
  
}





