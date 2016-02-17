import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, ArrayWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {
		
		//get first value from values
//		String keyVal = values.iterator().next().toString();
//		System.out.print("MAPPER: ");
//		System.out.println(keyVal);
		
//		for(ArrayWritable value : values){
			
//			ArrayWritable value = value.;
			
//			System.out.print("MAPPER: ");
//			System.out.println(value.get().toString());
			
			
//		}
//		context.write(key, new ArrayWritable(new String[]{"a", "b"}) );
		context.write(key, new IntWritable(5));
	  
	
	}
  
  
  
  
  
}





