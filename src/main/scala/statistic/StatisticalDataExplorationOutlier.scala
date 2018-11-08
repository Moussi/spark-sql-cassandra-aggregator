package statistic

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * There are many methods to identify outlier in statistics. In this Example, we are going to discuss about one of the
  * method which uses quantiles Box-and-Whisker Plot.
  *
  * Let’s say we have Q1 as first quantile(25%) and Q3 as third quantile(75%) , the inter quantile range or IQR will be given as
  *
  * IQR = Q3 - Q1
  * IQR gives the width of distribution of data between 25% and 75% of data. Using IQR we can identify the outliers.
  * This method is known as Box and Whisker method.
  *
  * In this method, any value smaller than Q1- 1.5 * IQR or any value greater than Q3+1.5 * IQR will be categorised as the outlier
  * https://www.purplemath.com/modules/boxwhisk3.htm
  */
object StatisticalDataExplorationOutlier {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().master("local[*]") getOrCreate()
        val sampleData = List(10.2, 14.1,14.4,14.4,14.4,14.5,14.5,14.6,14.7,
            14.7, 14.7,14.9,15.1, 15.9,16.4)
        val rowRDD = sparkSession.sparkContext.makeRDD(sampleData.map(value => Row(value)))
        val schema = StructType(Array(StructField("value",DoubleType)))
        val df = sparkSession.createDataFrame(rowRDD, schema)


        val medianAndQuantiles = df.stat.approxQuantile("value", Array(0.25, 0.75), 0.0)

        println("Qualtiles calculation ...")
        val Q1 = medianAndQuantiles(0)
        val Q3 = medianAndQuantiles(1)
        val IQR = Q3-Q1
        println(s" Quantile(25%) = $Q1")
        println(s" Quantile(75%) = $Q3")
        println(s" IQR = $IQR")

        val lowerRange = Q1 - 1.5*IQR
        val upperRange = Q3 + 1.5*IQR

        df.filter(s"value < $lowerRange and value > $upperRange").show
    }
}
