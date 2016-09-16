package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ProcessAllBills {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ProcessBills <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ProcessAllBills.class);
		job.setJobName("Process Bills");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * 1. Get the total PG & E bill amounts for Natural gas and electricity
		 */
		job.setMapperClass(BillMapper.class);
		job.setReducerClass(BillReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
