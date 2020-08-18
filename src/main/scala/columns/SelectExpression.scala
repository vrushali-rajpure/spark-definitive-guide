package columns

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SelectExpression {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate()

        val schemaType = StructType(Array(
            StructField("DEST_COUNTRY_NAME", StringType),
            StructField("ORIGIN_COUNTRY_NAME", StringType),
            StructField("count", IntegerType)
        ))
        val rows = Seq(
            Row("United States","Romania",1),
            Row("United States","Ireland",264))
        val df = spark.sparkContext.parallelize(rows)
        val dataFrame = spark.createDataFrame(df, schemaType)

        dataFrame
            .selectExpr(
                "*",
                "(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry"
            ).show();

        //selectExpr : specify aggregations over the entire DataFrame

        dataFrame
            .selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))")
            .show()
    }
}
