package mr_demo.CrimeTypeIndex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NeighborhoodCrimeTypeIndexDriver {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: inverter <input path> <output path>");
      System.exit(-1);
    }

    // create a Hadoop job and set the main class
    Job job = Job.getInstance();
    job.setJarByClass(NeighborhoodCrimeTypeIndexDriver.class);
    job.setJobName("NeighborhoodCrimeTypeIndexDriver");

    // set the input and output path
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // set the Mapper and Reducer class
    job.setMapperClass(InvertedIndexMapper.class);
    job.setReducerClass(InvertedIndexReducer.class);

    // specify the type of the output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // run the job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
