package columns

import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SelectExpression {

    //    Logger.getLogger("org").setLevel(Level.OFF)
    //    Logger.getLogger("akka").setLevel(Level.OFF)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate()

        //Logger off
        spark.sparkContext.setLogLevel("ERROR")

        val schemaType = StructType(Array(
            StructField("DEST_COUNTRY_NAME", StringType),
            StructField("ORIGIN_COUNTRY_NAME", StringType),
            StructField("count", IntegerType)
        ))
        val rows = Seq(
            Row("United States", "Romania", 1),
            Row("United States", "Ireland", 264))
        val df = spark.sparkContext.parallelize(rows)
        val dataFrame = spark.createDataFrame(df, schemaType)

        dataFrame
            .selectExpr(
                "*",
                "(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry"
            ).show();

        //selectExpr : specify aggregations over the entire DataFrame
        dataFrame
            .selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")
            .show()

        //Converting to Spark Types (Literals)
        val column = lit(1)
        dataFrame
            .select(expr("*"), column.as("One")).show(2)

        //Adding Columns
        dataFrame.withColumn("numberOne", lit(1)).show(2)

        // in Scala
        dataFrame.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
            .show(2)

        //Renaming Columns
        val columns = dataFrame.withColumnRenamed("ORIGIN_COUNTRY_NAME", "ORIGIN_COUNTRY").columns
        println(columns.foreach(a => println(a)))

        //Reserved Characters and Keywords
        val dfWithLongColName = dataFrame.withColumn(
            "This Long Column-Name",
            expr("ORIGIN_COUNTRY_NAME"))

        dfWithLongColName.selectExpr(
            "`This Long Column-Name`",
            "`This Long Column-Name` as `new col`")
            .show(2)

        //Changing a Column’s Type
        println("---------------- Change column type -----------------")
        val changedType = dataFrame.withColumn("count2", col("count").cast("long"))
        println(changedType.printSchema())

        //Filtering Rows
        println("---------------- Filter rows -----------------")
        dataFrame.filter(col("count")> 2).show();

        dataFrame.where("count > 2").show();

        //distinct count
        println(dataFrame.select("ORIGIN_COUNTRY_NAME").distinct().count());

        //Sample
        println("---------------- Sample -----------------")

        dataFrame
            .sample(withReplacement = false,0.5,5).count();

        // Union
        println("---------------- Union -----------------")

//        val unionRows = Seq(
//            Row("India", "Romania", 1),
//            Row("India", "Ireland", 1))
//
//        val newSchemaType = StructType(Array(
//            StructField("DEST_COUNTRY", StringType),
//            StructField("ORIGIN_COUNTRY", StringType),
//            StructField("count", IntegerType)
//        ))
//
//        val unionRdd = spark.sparkContext.parallelize(unionRows)
//        val newDF = spark.createDataFrame(unionRdd, newSchemaType);
//
//
//        dataFrame
//            .join(newDF, $"count")
//            .where("count >= 1")
//            .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
//            .show()

        // Sort
        import spark.implicits._
        println("---------------- Sort -----------------")
        dataFrame
            .sort("count").show()

        dataFrame.orderBy($"count".desc,$"ORIGIN_COUNTRY_NAME".desc).show()

        // sort within
    }
}
