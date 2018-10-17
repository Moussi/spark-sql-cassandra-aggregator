package utils

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}

object DateUtils {

  implicit class DateImprovements(val s: String) {

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    import scala.util.control.Exception.catching
    def safeParse = catching(classOf[DateTimeParseException]) opt LocalDate.parse(s, formatter)
  }

}

