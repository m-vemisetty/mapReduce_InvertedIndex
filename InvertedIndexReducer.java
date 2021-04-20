package mr_demo.CrimeTypeIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text value : values) {
				if (first) {
					first = false; 
				} else {
					sb.append(","); 
				} 
				
				if (sb.lastIndexOf(value.toString()) < 0) {
					sb.append(value.toString());
					}
				
			}
			result.set(sb.toString());
			context.write(key, result);
}


}
