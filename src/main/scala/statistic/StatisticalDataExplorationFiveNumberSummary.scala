package statistic

import org.apache.spark.sql.SparkSession

/**
  * Created by amoussi on 05/11/18.
  * Five number summary is one of the basic data exploration technique where we will find how values of dataset columns are distributed. In our example, we are interested to know the summary of “LifeExp” column.
  *
  * Five Number Summary Contains following information
  *
  * Min - Minimum value of the column
  * First Quantile - The 25% th data
  * Median - Middle Value
  * Third Quartile - 75% of the value
  * Max - maximum value
  */
object StatisticalDataExplorationFiveNumberSummary {
    def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession.builder().master("local[*]") getOrCreate()
        import StatSourceLoader._
        import sparkSession.implicits._
        val lifeExpectancyDS = getLifeExpectancyDataSet(sparkSession)

        // Describe show the Summary five number of the column lifeExp
        lifeExpectancyDS.describe("lifeExp").show()

        val medianAndQuantiles = lifeExpectancyDS.stat.approxQuantile("lifeExp", Array(0.25,0.5,0.75), 0.0)

        println("median And Qualtiles calculation ...")
        println(s" Quantile(0.25) = ${medianAndQuantiles(0)}")
        println(s" Quantile(0.5) = median =  ${medianAndQuantiles(1)}")
        println(s" Quantile(0.75) = ${medianAndQuantiles(2)}")

        val (startValues, counts) = lifeExpectancyDS.select("lifeExp").map(value => {
            value.getDouble(0)
        }).rdd.histogram(5)

        println("Histogram : ")
        println("start values")
        startValues.foreach(println)
        println("counts")
        counts.foreach(println)
        /*
        The result of the above code on our data will be as below

        startValues: Array[Double] = Array(47.794, 54.914, 62.034, 69.154, 76.274, 83.394)
        counts: Array[Long] = Array(24, 18, 32, 69, 54)
        So the values signify that there are 24 countries between life expectancy from 47.794 to 54.914. Most countries are between 76-83.
        */

        // zip function to combine both arrays startValues and counts element on array of Tuple
        // This is the output of the zip function
        // (47.794,24)
        // (54.914,18)
        // (62.034,32)
        // (69.154,69)
        // (76.274,54)
        val zippedValues = startValues.zip(counts)
        case class HistRow(startPoint: Double, count: Long)
        val rowRDD = zippedValues.map(zippedValue => HistRow(zippedValue._1, zippedValue._2))
        val histDF = sparkSession.createDataFrame(rowRDD)
        histDF.createOrReplaceTempView("life_expectancy")

        // we can use zepplein de visualise histogram
    }
}
