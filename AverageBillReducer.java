package solution;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class AverageBillReducer extends Reducer<IntWritable, Text, Text, Text> {

	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		context.write(new Text("Month"), new Text("Amount"));
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * Iterate over the values iterable and sum the bill amount for each key
		 * - month and count the values. Emit the key (month) and the average
		 * monthly bill.
		 */

		int count = 0;
		BigDecimal billAmount = new BigDecimal("0.0");
		BigDecimal totalMonthlyAmount = new BigDecimal("0.0");
		StringBuilder monthName = null;

		for (Text value : values) {

			String[] split = value.toString().split(",");
			monthName = new StringBuilder(split[0]);
			billAmount = new BigDecimal(split[1].substring(1));
			totalMonthlyAmount = totalMonthlyAmount.add(billAmount);
			count++;
		}
		if (count != 0) {
			BigDecimal avgMonthlyAmount = totalMonthlyAmount.divide(
					new BigDecimal(count), 2, BigDecimal.ROUND_HALF_UP);

			context.write(new Text(monthName.toString()), new Text(NumberFormat
					.getCurrencyInstance().format(avgMonthlyAmount)));
		}
	}
}
