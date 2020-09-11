package basics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DataTypes {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Basics")
            .master("local[*]")
            .getOrCreate();
        spark.sparkContext.setLogLevel("ERROR")

        val myManualSchema = new StructType(Array(
            StructField("UnitPrice", DoubleType, nullable = true),
            StructField("Description", StringType, nullable = true),
            StructField("StockCode", StringType, nullable = true),
            StructField("Quantity", IntegerType, nullable = true)
        ))

        val myRows = Seq(
            Row(50.0, "POSTAGE", "DOT", 1),
            Row(60.0, "POSTAGE", "DOT", 2),
            Row(70.0, "POSTAGE", "NET", 3),
            Row(55.0, "TEST", "COM", 5)
        )
        val myRDD = spark.sparkContext.parallelize(myRows)
        val df = spark.createDataFrame(myRDD, myManualSchema)
        df.show()

        println("lit function")
        df.select(lit(5), lit("five"), lit(5.0))

        df.show()

        println("boolean datatypes")
        //TODO : In Spark, if you want to filter by equality you should use === (equal) or =!= (not equal).
        df.where(col("StockCode") === "NET")
            .select("Description")
            .show();

        //TODO : with string literal
        df.where("StockCode = 'NET'")
            .show();

        val priceFilter = col("UnitPrice") > 55
        val descFilter = col("Description").contains("POSTAGE")

        df.where(col("StockCode").isin("DOT"))
            .where(priceFilter.or(descFilter))
            .show()

        //To filter a DataFrame, you can also just specify a Boolean column:

        val DOTFilter = col("StockCode") === "DOT"

        df.withColumn("isExpensive", DOTFilter.and(priceFilter.or(descFilter)))
            .where("isExpensive")
            .select("UnitPrice", "isExpensive")
            .show()

        //If there is a null in your data, you’ll need to treat things a bit differently.
        // Here’s how you can ensure that you perform a null-safe equivalence test:

        df.where(col("Description").eqNullSafe("POSTAGE"))
            .show()

        //  Working with numbers

        df.select(round(lit("2.5")), bround(lit("2.5")))
            .show(2)

        val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
        df.select(col("Description"), fabricatedQuantity as "fabricatedQuantity")
            .show()

        println("using expression")
        df.selectExpr("Description",
            "pow(Quantity * UnitPrice,2) + 5 as fabricatedQuantity")
            .show()

        //static correlation
        println("static correlation")
        df.stat.corr("Quantity", "UnitPrice")
        df.select(corr("Quantity", "UnitPrice")).show()

        //string data types

        df.select(col("Description"),
            lower(col("Description")),
            upper(lower(col("Description"))))
            .show(2)

        df.select(
            ltrim(lit("    HELLO    ")).as("ltrim"),
            rtrim(lit("    HELLO    ")).as("rtrim"),
            trim(lit("    HELLO    ")).as("trim"),
            lpad(lit("HELLO"), 3, " ").as("lp"),
            rpad(lit("HELLO"), 10, " ").as("rp"))
            .show(2)

        val simpleColors = Seq("black", "white", "red", "green", "blue")
        val regexString = simpleColors.map(_.toUpperCase).mkString("|")
        // the | signifies `OR` in regular expression syntax
        df.select(
            regexp_replace(col("Description"), regexString, "COLOR")
                .alias("color_clean"),
            col("Description")).show(2)
    }
}
