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
        medianAndQuantiles.foreach(println)

        val (a, b) = lifeExpectancyDS.select("lifeExp").map(value => {
            value.getDouble(0)
        }).rdd.histogram(5)
        println("------------"+a)
        println("------------"+b)
    }
}
