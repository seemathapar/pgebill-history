package solution;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BillMonthMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	private static HashMap<String, Integer> months = new HashMap<String, Integer>();		
	
	    
	    @Override
	    /*
	     *Hashmap of month names and their numbers to aid sorting the output month wise
	     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text>.Context context)
	    		throws IOException, InterruptedException {
	    	
	    	super.setup(context);
	    	months.put("Jan", 0);
		    months.put("Feb", 1);
		    months.put("Mar", 2);
		    months.put("Apr", 3);
		    months.put("May", 4);
		    months.put("Jun", 5);
		    months.put("Jul", 6);
		    months.put("Aug", 7);
		    months.put("Sep", 8);
		    months.put("Oct", 9);
		    months.put("Nov", 10);
		    months.put("Dec", 11);
	    }

  /**
   * Example input line:
   * Natural gas billing,4-Sep-2014,24-Sep-2014,14,therms,$19.34 

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
        String billMonth = endDateFields[1];        
      
		/* if it's a valid month, write it out 
		 * The output keys will be sorted in the shuffle sort phase 
		 * as a result the monthly data will be sorted output by the reducer*/
        int monthNum = months.get(billMonth);
        if (months.get(billMonth) != null)
        	context.write(new IntWritable(monthNum), new Text(billMonth + ","+ amountStr));
      }
    }
  }
}
