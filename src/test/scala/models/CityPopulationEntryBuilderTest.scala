package models

import org.scalatest.FunSuite

class CityPopulationEntryBuilderTest extends FunSuite {
  test("Should create a valid CityPopulationEntryBuilder from the line " +
    "\"Kazakhstan\",2004,\"Total\",\"Male\",\"Temirtau\",\"Urban agglomeration\"," +
    "\"Estimate - de facto\",\"Final figure, complete\",2004,78087,"){

    val expectedEntry = CityPopulationEntry(
      "Kazakhstan", 2004, "Total", "Male",  "Temirtau", "Urban agglomeration",
      "Estimate - de facto", "Final figure, complete", 2004, 78087, ""
    )

    val lineToParse = "\"Kazakhstan\",2004,\"Total\",\"Male\",\"Temirtau\",\"Urban agglomeration\"," +
                      "\"Estimate - de facto\",\"Final figure, complete\",2004,78087,"

    val realEntry: CityPopulationEntry = CityPopulationEntryBuilder
      .buildEntryFromLine(lineToParse)

    assert(expectedEntry === realEntry)
  }
}
