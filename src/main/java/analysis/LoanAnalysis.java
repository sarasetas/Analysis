package analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class LoanAnalysis {

    public static void main(String args[]) {
        loanAnalysis();
    }

    private static void loanAnalysis() {
        SparkSession sc = SparkSession.builder().appName("BankLoan").master("local").getOrCreate();
        Dataset<Row> df = getFileAsDf(sc);

        //2. Display into the console the number of rows and the number of columns
        System.out.println(df.count());
        System.out.println(df.columns().length);

        //3. Display into the console the first rows of the bank_loan data.
        int numRows = 10;
        df.getRows(numRows, 0);

        //4. Display into the console all possible values of Loan.Status
        df.select("Loan Status").distinct().show();

        //Data Preparation
        //1. Delete from bank_loan data columns Loan.ID & Customer.ID
        String[] fieldsToDelete = {"Loan ID", "Customer ID"};
        df.drop(fieldsToDelete);

        //Filter the dataset and keep only rows that have:
        // Open.Credit<10000 AND
        //Annual.Income<7000000
        Dataset<Row> df2 = df.filter(
                df.col("Maximum Open Credit")
                        .$less(10000).and(
                        df.col("Annual Income")
                                .$less(7000000)));

        //Add a new filter and keep only data that have Current.Loan.Amount<20000000
        Dataset<Row> df3 = df2.filter(
                df2.col("Current Loan Amount").$less(20000000));

        //For each row, if at least a column has a missing value, remove the corresponding row.
        Dataset<Row> df4 = df3.na().drop();

        // NOTE: IT PASSED 1H until this point. I decided to continue anyway.

        //Display into the console the number of rows and the number of columns of the resulting dataset.
        System.out.println("Number of rows: " + df4.count());
        System.out.println("Number of columns: " + df4.columns().length);

        // Split the resulting dataset into two persistent datasets:
        String[] stringTypeFields =
                Arrays.stream(df4.dtypes()).filter(f -> f._2 != "IntegerType")
                        .map(x -> x._1).toArray(String[]::new);

        Dataset<Row> numericDf = df4.drop(stringTypeFields);
        Dataset<Row> nominalDf = df4.select(stringTypeFields[0], stringTypeFields);

        System.out.println("Show nominal Dataframe:");
        nominalDf.show();
        System.out.println("Show numeric Dataframe:");
        numericDf.show();
    }

    private static Dataset<Row> getFileAsDf(SparkSession sc) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("Loan ID", DataTypes.StringType, true),
                DataTypes.createStructField("Customer ID", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sc.read().format("csv")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("inferSchema", "true")
                //.schema(schema)
                .load("bank_loan.csv");

        System.out.println("Print schema");
        df.printSchema();
        df.show();

        return df;
    }

}

