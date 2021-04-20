package mr_demo.CrimeTypeIndex;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> { 
    private Text neighbourhood = new Text(); 
    private Text crime = new Text();
    public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException { 
    	String data = value.toString();
    	String NEIGHBOURHOOD_S = data.split(",")[4];
    	String CRIME_S = data.split(",")[5];
    	
       if (NEIGHBOURHOOD_S == null || CRIME_S == null){
	 return; 
      }
       neighbourhood.set(NEIGHBOURHOOD_S);
       crime.set(CRIME_S);
       context.write(neighbourhood, crime);
          
        }
    } 

