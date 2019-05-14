package models

case class CityPopulationEntry(countryOrArea: String, year: Int, area: String,
                               sex: String, city: String, cityType: String,
                               recordType: String, reliability: String,
                               sourceYear: Int, value: Double,
                               valueFootnotes: String)

object CityPopulationEntryBuilder {
  val patternCsvCommaSeparate = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  def buildEntryFromLine(lineToParse: String): CityPopulationEntry = {
    val lineSplitted: Array[String] = lineToParse.split(patternCsvCommaSeparate);

    CityPopulationEntry(
      lineSplitted(0).replace("\"", ""),
      lineSplitted(1).toInt,
      lineSplitted(2).replace("\"", ""),
      lineSplitted(3).replace("\"", ""),
      lineSplitted(4).replace("\"", ""),
      lineSplitted(5).replace("\"", ""),
      lineSplitted(6).replace("\"", ""),
      lineSplitted(7).replace("\"", ""),
      lineSplitted(8).toInt,
      lineSplitted(9).toDouble,
      if(lineSplitted.length > 10) lineSplitted(10) else ""
    )
  }
}

