package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ProcessBillsByMonth {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ProcessBills <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ProcessBillsByMonth.class);
		job.setJobName("Process Bills");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		/*
		 * 2. To get the month wise total for the bills across all years. 
		 */
		 
		job.setMapperClass(BillMonthMapper.class);
		job.setReducerClass(AverageBillReducer.class);
		

		

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
