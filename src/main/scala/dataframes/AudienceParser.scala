package dataframes

import java.time.LocalDate
import java.util.UUID

/**
  * Created by amoussi on 17/10/18.
  */
object AudienceParser {

    import utils.StringUtils._
    def parseAudiencePoi(record: Array[String], date: LocalDate, lastoffertypechangeDate: LocalDate) = {
        AudiencePoi(
            pyear = date.getYear.toString,
            pmonth = date.getMonthValue.toString,
            pday = date.getDayOfMonth.toString,
            pdate = java.sql.Timestamp.valueOf(date.atStartOfDay()),
            poi_id = record(1),
            terminal = record(2),
            uad_terminal = record(3),
            bot = record(4),
            uad_bot = record(5),
            env = record(6),
            tagid = record(7),
            tagid_groupe = record(8),
            tagid_sous_groupe = record(9),
            tagid_intern = record(10),
            target_url = record(11),
            country = record(12),
            region = record(13),
            town = record(14),
            postalcode = record(15),
            name = record(16),
            provider = record(17),
            provider_family = record(18),
            rubrique = record(19),
            rubrique_parent = record(20),
            rubrique_categorie = record(21),
            rubrique_bu = record(22),
            rubrique_bu_segment = record(23),
            offertype = record(24),
            ovm = record(25),
            providers = record(26),
            allrubrics = record(27),
            brand = record(28),
            has_epjid = record(29),
            onumcli = record(30),
            visibilitylevel = record(31),
            localbusinesspackage = record(32),
            storechains = record(33),
            pjrating = record(34),
            allappids = record(35),
            pjcontent = record(36),
            indexedrubricids = record(37),
            lastoffertypechange = java.sql.Timestamp.valueOf(lastoffertypechangeDate.atStartOfDay()),
            vdeappids = record(39),
            tags = record(40),
            nb = record(41).toLong,
            coefvisibility = record(42).toDoubleSafe.getOrElse(0),
            cornerappids = record(43),
            idabtest = record(44),
            idvariant = record(45),
            indoorview = record(46),
            outdoorview = record(47),
            tabsappid = ""
        )
    }

}
