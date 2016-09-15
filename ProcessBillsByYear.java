package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ProcessBillsByYear {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ProcessBills <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ProcessBillsByYear.class);
		job.setJobName("Process Bills");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		

		/*
		 * 3. To get the annual total for the PG & E bills 
		 * 
		 */
		job.setMapperClass(AnnualBillMapper.class);
		job.setReducerClass(AnnualBillReducer.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
