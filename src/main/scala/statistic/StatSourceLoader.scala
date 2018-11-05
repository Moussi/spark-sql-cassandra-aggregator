package statistic

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by amoussi on 05/11/18.
  */
object StatSourceLoader {
    def getLifeExpectancyDataSet(sparkSession: SparkSession): Dataset[LifeExpectancy] = {

        import sparkSession.implicits._
        //only extract the values we need
        val schema = StructType(Array(
            StructField("country", StringType),
            StructField("lifeExp", DoubleType),
            StructField("region", StringType)
        ))
        sparkSession.read.format("com.databricks.spark.csv")
            .option("delimiter", ";")
            .option("inferSchema", "false")
            .option("header", "false")
            .schema(schema)
            .load("src/main/resources/LifeExpentancy.txt")
            .as[LifeExpectancy]
    }

    case class LifeExpectancy(country: String,
                              lifeExp: Double,
                              region: String)
}
