package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._

object Functions {
    def main(args: Array[String]): Unit = {
        {
            val spark = SparkSession
                .builder
                .appName("Spark Basics")
                .master("local[*]")
                .getOrCreate();

            val csvPath = getClass.getResource("/2015-summary.csv").getPath

            // --------------Infer Schema --------------
            val dataFrame = spark.read.
                option("header", value = true)
                .option("inferSchema", value = true)
                .csv(csvPath)

            //----------SQL----------------

            dataFrame.createOrReplaceTempView("2015_summary");

           // ------------ Functions -----------------

            val sqlWay = spark.sql("select max(count) from 2015_summary")

            val dfWay = dataFrame
                .select(max("count"))
            //                .take(1);

            sqlWay.show();

            dfWay.show();

            //---------- Top 5 destination countries --------------

            val sqlTop5 = spark
                .sql("select ORIGIN_COUNTRY_NAME,sum(count) from 2015_summary" +
                    " group by ORIGIN_COUNTRY_NAME" +
                    " order by sum(count) DESC" +
                    " limit 5")

            sqlTop5.show();

            dataFrame
                .groupBy("ORIGIN_COUNTRY_NAME")
                .sum("count")
                .withColumnRenamed("sum(count)", "destination_total")
                .sort(desc("destination_total"))
                .limit(5)
                .show()
        }

    }
}
