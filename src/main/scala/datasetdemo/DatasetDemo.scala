package datasetdemo

import org.apache.spark.sql.SparkSession

object DatasetDemo {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate()

        val csvPath = getClass.getResource("/2015-summary.csv").getPath

        import spark.implicits._

        val dataFrame = spark
            .read
            .option("header", value = true)
//            .option("inferSchema", value = true)
            .csv(csvPath)
        val flights = dataFrame.as[Flight]

        flights.show(1);

        //----------- DF to DS via map


    }
}
