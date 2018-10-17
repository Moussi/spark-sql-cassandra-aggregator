package utils

import java.io.File
import java.sql.Timestamp

object StringUtils {

  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception.catching
    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
    def toDoubleSafe = catching(classOf[NumberFormatException]) opt s.toDouble
    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt
      Timestamp.valueOf(s)

    def getListOfFiles: List[File] = {
      val d = new File(s)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }
  }

}

