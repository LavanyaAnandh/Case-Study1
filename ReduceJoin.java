
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReduceJoin {

	public static void main(String[] args) throws Exception
	{
		if(args.length!=3)
		{
			System.err.println("Reduce Join: <Input path1> <Input path2> <Output Path>");
			System.exit(-1);
		}
		
		//Configuration and Job settings
		Configuration conf = new Configuration();
		System.out.println("Config setting is done");
		Job job = new Job(conf, "Reduce-side join");
		System.out.println("job setting is done");
		job.setJarByClass(ReduceJoin.class);
				
		//Input path setting. setting 2 i/p files that are joined to get the output
		MultipleInputs.addInputPath (job, new Path(args[0]),TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		
		//setting reducer class.
		job.setReducerClass(ReduceJoinReducer.class);
		System.out.println("Reducer class setting is done");
		
		//set Output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.out.println("output key values is done");
		
		//set Output paths
		Path outputpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputpath);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

