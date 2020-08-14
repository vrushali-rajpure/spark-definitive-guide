package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkBasics {

    def main(args: Array[String]): Unit = {
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

        // ------------ PrintSchema -----------------
        dataFrame.printSchema()

        //----------SQL----------------

        dataFrame.createOrReplaceTempView("2015_summary");
        val sqlDF = spark
            .sql("SELECT DEST_COUNTRY_NAME, count(1) FROM 2015_summary GROUP BY DEST_COUNTRY_NAME")

        sqlDF.show(5)

        val dfWay = dataFrame
            .groupBy("DEST_COUNTRY_NAME")
            .count()

        dfWay.show(6)

        //------------ Functions -----------------

    }
}
