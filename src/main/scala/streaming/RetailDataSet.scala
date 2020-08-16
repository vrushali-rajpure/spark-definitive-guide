package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, column, desc, col}

object RetailDataSet {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate()

        spark.conf.set("spark.sql.shuffle.partitions", "5")

        val retailData = spark
            .read
            .format("csv")
            .option("inferSchema", value = true)
            .option("header", value = true)
            .load("/Users/vrushali/Tech_Learning/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

        println("-------------------------------------------------------")
        retailData.createOrReplaceTempView("retail_data");

        retailData.printSchema()

        //------------------- what days a customer spent the most--------------------
        val windowFunc = window(col("InvoiceDate"), "1 day")
        retailData
            .selectExpr(
                "CustomerID",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate")
            .groupBy(col("CustomerID"),windowFunc)
            .sum("total_cost")
            .show(5)

    }
}
