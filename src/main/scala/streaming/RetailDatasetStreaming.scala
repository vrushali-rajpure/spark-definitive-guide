package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types._

object RetailDatasetStreaming {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate()

        spark.conf.set("spark.sql.shuffle.partitions", "5")

        var staticSchema = StructType(
            StructField(name = "InvoiceNo", dataType = IntegerType) ::
                StructField(name = "StockCode", dataType = IntegerType) ::
                StructField(name = "Description", dataType = StringType) ::
                StructField(name = "Quantity", dataType = IntegerType) ::
                StructField(name = "InvoiceDate", dataType = TimestampType) ::
                StructField(name = "UnitPrice", dataType = DoubleType) ::
                StructField(name = "CustomerID", dataType = DoubleType) ::
                StructField(name = "Country", dataType = StringType) :: Nil
        )

        val streamingDataFrame = spark
            .readStream
            .format("csv")
            .schema(staticSchema)
            .option("maxFilesPerTrigger", 1) // specifies the number of files we should read in at once.
            .load("/Users/vrushali/Tech_Learning/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

        //----------------- whether our DataFrame is streaming: ----------------------

        println(streamingDataFrame.isStreaming)

        import spark.implicits._

        //-------------------- readStream -----------------------------

        val purchaseByCustomerPerHour = streamingDataFrame
            .selectExpr(
                "CustomerID",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate"
            )
            .groupBy(
                $"CustomerID", window($"InvoiceDate", "1 hour"))
            .sum("total_cost")

        //-------------- writeStream --------------------


        purchaseByCustomerPerHour
            .writeStream
//            .format("memory") // memory = store in-memory table
            .format("console")
            .outputMode("complete") // complete = all the counts should be in the table
            .queryName("customer_purchases") // the name of the in-memory table
            .start()
                .awaitTermination(4000)

        //When we start the stream, we can run queries against it to debug what our
        // result will look like if we were to write this out to a production sink:

        spark
            .sql("SELECT * FROM customer_purchases ORDER BY `sum(total_cost)` DESC ")
            .show(5)

    }
}
