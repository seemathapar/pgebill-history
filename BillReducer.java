package solution;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class BillReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		context.write(new Text("Billing Type"), new Text("       Total Amount"));
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * Iterate over the values iterable and sum the bill amount Emit the key
		 * (month) and the average monthly bill.
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

		context.write(
				key,
				new Text(NumberFormat.getCurrencyInstance().format(
						totalMonthlyAmount)));
	}
}
