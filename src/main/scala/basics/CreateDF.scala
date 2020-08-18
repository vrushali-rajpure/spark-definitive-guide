package basics


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{expr, col, column}

class CreateDF {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate();

        val myManualSchema = new StructType(Array(
            StructField("some", StringType, nullable = true),
            StructField("col", StringType, nullable = true),
            StructField("names", LongType, nullable = false)))

        val myRows = Seq(Row("Hello", null, 1L))
        val myRDD = spark.sparkContext.parallelize(myRows)
        val myDf = spark.createDataFrame(myRDD, myManualSchema)
        myDf.show()

        val frame = myDf.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")

        //One common error is attempting to mix Column objects and strings.
        // For example, the following code will result in a compiler error:

        //myDf.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME") //TODO

        //you can refer to columns in a number of different ways;
        // all you need to keep in mind is that you can use them interchangeably:
        import spark.implicits._;
        myDf.select(
            myDf.col("DEST_COUNTRY_NAME").alias("tesr"),
            col("DEST_COUNTRY_NAME"),
            column("DEST_COUNTRY_NAME"),
            'DEST_COUNTRY_NAME,
            $"DEST_COUNTRY_NAME",
            expr("DEST_COUNTRY_NAME"))
            .show(2)
    }
}
