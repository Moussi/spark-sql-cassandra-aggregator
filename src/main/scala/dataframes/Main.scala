package dataframes

import java.io.File

/**
  * Created by amoussi on 17/10/18.
  */
object Main extends App {

    getListOfFiles("/mnt/hadoop/hive-warehouse/bi_poi_audience/year=2018/month=10/day=3").foreach(file => println(file.getAbsoluteFile))

    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            List[File]()
        }
    }

}
