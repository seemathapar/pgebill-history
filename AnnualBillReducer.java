package solution;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class AnnualBillReducer extends Reducer<Text, Text, Text, FloatWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * Iterate over the values iterable and sum the bill amount for each key - month and count the values
		 * Emit the key (month) and the average monthly bill.
		 */

		BigDecimal billAmount = new BigDecimal("0.0");
		BigDecimal totalMonthlyAmount = new BigDecimal("0.0");

		for (Text value : values) {

			if (value.getLength() > 2) {
				billAmount = new BigDecimal(value.toString().substring(1));
			} else 
				billAmount = new BigDecimal("0.0");
			totalMonthlyAmount = totalMonthlyAmount.add(billAmount);
		}
		
		
		context.write(key, new FloatWritable(totalMonthlyAmount.floatValue()));
	}
}
