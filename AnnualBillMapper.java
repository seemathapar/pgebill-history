package solution;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnnualBillMapper extends Mapper<LongWritable, Text, Text, Text> {

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
			/*
			 * Save the end date field in the line to extract the billing month.
			 */
			String[] endDateFields = fields[2].split("-");
			if (endDateFields.length > 1) {
				String billYear = endDateFields[2];

				context.write(new Text(billYear), new Text(amountStr));
			}
		}
	}
}
