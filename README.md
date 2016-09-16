# pgebill-history
This is a small demo project for Hadoop Map reduce code in Java.

Input Data : The 2 data files pge_gas_bill.csv and pge_electric_bill.csv have the historical bill data since 2014 for a household. For the sake of the demo, the size of the file is intentionally kept small.

Executable jar: I have executed the code on CDH 4.2.1. The executable jar billPartition.jar is uploaded along with the java code.
For ease in execution, I have created 3 different sets of Mapper-Reducer-Driver classes to get answers for the 3 different scenarios over the same input data set:

1. View the total amount spent in Gas and Electric bills since 2014. 

  hadoop jar billPartition.jar solution.ProcessAllBills bill_log bill_log_op1

2. View the annual expenses on PG & E bills so far.

  hadoop jar billPartition.jar solution.ProcessBillsByYear bill_log bill_log_op2

3. View the month wise expenses in Gas and electric bills to get the peak expenses

  hadoop jar billPartition.jar solution.ProcessBillsByMonth bill_log bill_log_op3
  








