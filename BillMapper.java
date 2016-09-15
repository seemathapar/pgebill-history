package solution;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BillMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static List<String> months = Arrays.asList("Jan", "Feb", "Mar",
			"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

	/**
	 * Example input line: Natural gas
	 * billing,4-Sep-2014,24-Sep-2014,14,therms,$19.34
	 * 
	 * 
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Split the input line into comma-delimited fields.
		 */
		String[] fields = value.toString().split(",");

		if (fields.length >= 6) {
			// The bill amount is the 5th field in the file
			String amountStr = fields[5].trim();
			// The bill type is the first field in the file i.e Gas/ Electric
			String billType = fields[0];

			context.write(new Text(billType), new Text(amountStr));
		}

	}
}
