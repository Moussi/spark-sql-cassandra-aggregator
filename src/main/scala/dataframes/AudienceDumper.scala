package dataframes

import java.time.LocalDate

import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory


object AudienceDumper{
    val logger = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {

        val ss = SparkSession.builder().config("spark.cassandra.connection.host", "localhost").master("local[*]").getOrCreate()
        import utils.StringUtils._
        import org.apache.spark.sql.functions._

        val year = args(0)
        val month = args(1)
        val day = args(2)

        val filePath = s"/mnt/hadoop/hive-warehouse/bi_poi_audience/year=$year/month=$month/day=$day/"

        val listOfFiles = filePath.getListOfFiles

        listOfFiles.foreach(file => processAndSaveToCassandra(ss, file.getAbsolutePath))

        val audienceDF = ss
            .read
            .cassandraFormat("bi_poi_audience", "mappy_bi")
            .options(ReadConf.SplitSizeInMBParam.option(32))
            .load()
        audienceDF.select(count("*")).show
    }

    def processAndSaveToCassandra(ss: SparkSession, filePath: String) = {
        /**
          * Create RDD from data.tsv file that we genrated with LogProducer class
          */

        logger.info(s"load data from $filePath")
        import utils.DateUtils._
        import AudienceParser._
        import ss.implicits._
        val sourceRDD = ss.sparkContext.textFile(s"file:///$filePath")
        val test = ss.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\\t")
            .schema(buildAudienceStructType)
            .load(s"file:///$filePath").as[AudiencePoi]

        test.show(1)
        /**
          * we let the function defines the column names of our DF
          * because of using Activity case class
          */

            println("ok")
        val audienceRDD = sourceRDD.map(line => {
            val record = line.split("\\t")

            val date = record(0).safeParse.getOrElse(LocalDate.now())
            val lastoffertypechangeDate = record(38).safeParse.getOrElse(LocalDate.now())
            parseAudiencePoi(record, date, lastoffertypechangeDate)
        })

        val audienceDF = ss.createDataFrame(audienceRDD)

        audienceDF.write
            .cassandraFormat("bi_poi_audience", "mappy_bi", CassandraSourceRelation.defaultClusterName, true)
            .mode(SaveMode.Append)
            .save()
        logger.info(s"load success from $filePath")

    }

    private def buildAudienceStructType = {
        StructType(Seq(
            StructField("pyear", StringType, true),
            StructField("pmonth", StringType, true),
            StructField("pday", StringType, true),
            StructField("pdate", TimestampType, true),
            StructField("poi_id", StringType, true),
            StructField("terminal", StringType, true),
            StructField("uad_terminal", StringType, true),
            StructField("bot", StringType, true),
            StructField("uad_bot", StringType, true),
            StructField("env", StringType, true),
            StructField("tagid", StringType, true),
            StructField("tagid_groupe", StringType, true),
            StructField("tagid_sous_groupe", StringType, true),
            StructField("tagid_intern", StringType, true),
            StructField("target_url", StringType, true),
            StructField("country", StringType, true),
            StructField("region", StringType, true),
            StructField("town", StringType, true),
            StructField("postalcode", StringType, true),
            StructField("name", StringType, true),
            StructField("provider", StringType, true),
            StructField("provider_family", StringType, true),
            StructField("rubrique", StringType, true),
            StructField("rubrique_parent", StringType, true),
            StructField("rubrique_categorie", StringType, true),
            StructField("rubrique_bu", StringType, true),
            StructField("rubrique_bu_segment", StringType, true),
            StructField("offertype", StringType, true),
            StructField("ovm", StringType, true),
            StructField("providers", StringType, true),
            StructField("allrubrics", StringType, true),
            StructField("brand", StringType, true),
            StructField("has_epjid", StringType, true),
            StructField("onumcli", StringType, true),
            StructField("visibilitylevel", StringType, true),
            StructField("localbusinesspackage", StringType, true),
            StructField("storechains", StringType, true),
            StructField("pjrating", StringType, true),
            StructField("allappids", StringType, true),
            StructField("pjcontent", StringType, true),
            StructField("indexedrubricids", StringType, true),
            StructField("lastoffertypechange", TimestampType, true),
            StructField("vdeappids", StringType, true),
            StructField("tags", StringType, true),
            StructField("nb", LongType, true),
            StructField("coefvisibility", DoubleType, true),
            StructField("cornerappids", StringType, true),
            StructField("idabtest", StringType, true),
            StructField("idvariant", StringType, true),
            StructField("indoorview", StringType, true),
            StructField("outdoorview", StringType, true),
            StructField("tabsappid", StringType, true)
            )
        )
    }
}
