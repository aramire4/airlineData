package basicscala

import scala.util._
import scala.util.Try
import swiftvis2.plotting._
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.ColorGradient
import scalafx.scene.effect.BlendMode.Red
import scalafx.scene.effect.BlendMode.Green
import scalafx.scene.effect.BlendMode.Blue
import scala.math


case class CountryGDP(countryName:String, gdp:Array[Double])

case class CountryPos(initial:String, latitude:Double, 
                        longitude: Double, country_Name:String)

case class Country(countryName:String, year:Int, age_group:String,
                    sex:String, metric:String,  
                    mean:Double, upper:Double, lower:Double)

object BasicScala {
    def pasrseLine1(line1:String):Country = {

        var p = line1.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)

        Country(p(2).toString, p(3).toInt, p(5).toString, 
            p(7).toString, p(8).toString, 
            p(10).toDouble, p(11).toDouble, p(12).toDouble)
    }

    def parseLine2(line2:String):CountryGDP = {
        val p = line2.split(",")

        CountryGDP(p(0).toString, (p.takeRight(58)).map{x => 
            if(x.size <= 2) -1.0 else (x.drop(1).dropRight(1)).toDouble
        })
    }

    def parseLine3(line3:String):CountryPos = {
        val p = line3.split("\\s+")
        if(p(0).toString == "UM"){
            CountryPos(p(0).toString, 0.0, 
                    0.0, p(2).toString)
        }
        else{
            CountryPos(p(0).toString, p(1).toDouble, 
                    p(2).toDouble, p(3).toString)
        }
    }

    def main(args:Array[String]):Unit = {
        val source1 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
        val lines1 = source1.getLines().drop(1)
        
        val source2 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
        val lines2 = source2.getLines().drop(5)
        
        val source3 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/countries.tsv")
        val lines3 = source3.getLines().drop(1)
        
        val countryData = lines1.map(pasrseLine1).toArray
        val gdpData = lines2.map(parseLine2).toArray
        val posData = lines3.map(parseLine3).toArray

        
        //How many different types of values are reported in the education file under the "metric" column? What are they?
        val metrics = countryData.map(_.metric)
        val m = metrics.distinct
        println("1. Different Types of values reported in metric " + m.length)
        m.foreach(println)
        println()

        //List the five entries with the highest value for "Education Per Capita". (Use "Education Per Capita" for all following education questions.)
        val epc = countryData.filter(_.metric == "Education Per Capita")
        val highestEPC = (epc.map(x => (x.countryName, x.upper))).sortBy(_._2)(Ordering[Double].reverse)
        println("2. Five entries with the highest value for Education Per Capita (Using upper)")
        (highestEPC.take(5)).foreach(println)
        println()

        //Which country has had the largest increase in education per capita over the time of the dataset? 
        //How big is the difference? You should potentially have a different country for each age/gender combo.
        val epcData = ((epc.filter(x => (x.year == 1970 || x.year == 2015))).map(x => (x.countryName, x.upper, x.age_group, x.sex)))
        println("3. Country with the largest increase in education per capita (using Upper)")

        val groupedEPC = epcData.groupBy(x => (x._1, x._3, x._4)) mapValues (_ map (_._2))
        val incr = groupedEPC.map( x =>
            (x._1, ((x._2).reverse).reduceLeft(_ - _))
        ).toSeq.sortBy(_._2)(Ordering[Double].reverse)

        println(incr.take(1) + "\n")

        //Which country had the largest GDP per capita in 1970? What was it? Give me the same information for the smallest value.
        val gdp1 = gdpData.map(x => (x.countryName, x.gdp(10).toDouble)).sortBy(_._2)(Ordering[Double].reverse)
        println("4. Country with the largest GDP per capita in 1970")

        println(gdp1(0))
        val minGDP1 = (gdp1.filter(_._2 > 0))
        println("Smallest GDP per capita in 1970" + minGDP1(minGDP1.length-1) + "\n")

        //Which country had the largest GDP per capita in 2015? What was it? Give me the same information for the smallest value.
        val gdp2 = gdpData.map(x => (x.countryName, x.gdp(55).toDouble)).sortBy(_._2)(Ordering[Double].reverse)
        println("5. Country with the largest GDP per capita in 2015")

        println(gdp2(0))
        val minGDP2 = (gdp2.filter(_._2 > 0))
        println("Smallest GDP per capita in 2015" + minGDP2(minGDP1.length-1) + "\n")

        //Which county had the largest increase in GDP per capita from 1970 to 2015? What were the starting and ending values? (Note that you can't assume no data means 0. It just means that it wasn't reported.)
        val filteredGDP = (gdpData.filter(x => x.gdp(10) != -1.0)).filter(x => x.gdp(55) != -1.0).map(x => (x.countryName, x.gdp(55)-x.gdp(10))).sortBy(_._2)(Ordering[Double].reverse)
        println("6. Country that had the largest increase in GDP " + filteredGDP(0))
        val countryGDP = (gdpData.filter(x => x.countryName == filteredGDP(0)._1)).map(_.gdp)
        println("Original start (1970) " + countryGDP(0)(10) + " end point (2015): " + countryGDP(0)(55) + "\n")

        //Pick three countries and make a scatter plot with year on the X-axis and educational attainment of females ages 25-34 on the Y-axis. Your three countries should have good data going back to at least 1970.
        //val cg = ColorGradient("Russia" -> RedARGB, "United States" -> BlueARGB, "Luxembourg"-> GreenARGB)
        val dat1 = (countryData.filter(_.metric == "Education Per Capita").filter(x => x.countryName == "United States" || x.countryName == "Russia" || x.countryName == "Luxembourg")).filter(x => 
                                        x.metric == "Education Per Capita" && x.sex == "Females" && x.age_group == "25 to 34")

         val eduAttPlot1 = Plot.simple(
            ScatterStyle (dat1.map(_.year), dat1.map(_.upper), 
                        symbolWidth = 3, symbolHeight = 3, colors = RedARGB),
                        "Educational Attainment for Females ages 25-34 (Using upper)", "Year", "Education Per Capita")
        SwingRenderer(eduAttPlot1, 800, 800, true)

        
        //For those same three countries you picked for #7, make a scatter plot of GDP over time.
        var nums = (1960 to 2017).toArray
        val dat2 = gdpData.filter(x => x.countryName == gdpData(249).countryName || x.countryName == gdpData(200).countryName || x.countryName == gdpData(142).countryName)

        val scatterArray = Array(
            ScatterStyle(nums, dat2(0).gdp, symbolWidth = 5, symbolHeight = 5, colors = BlueARGB),
            ScatterStyle(nums, dat2(1).gdp, symbolWidth = 5, symbolHeight = 5, colors = RedARGB),
            ScatterStyle(nums, dat2(2).gdp, symbolWidth = 5, symbolHeight = 5, colors = GreenARGB)
        )
        val gdpPlot1 = Plot.stacked(
            scatterArray, "GDP Over Time", "Time", "GDP"
        )
        
        SwingRenderer(gdpPlot1, 800, 800, true)

        //Make a scatter plot with one point per country (for all countries) with GDP on the X-axis and education level of males ages 25-34 on the Y-axis. 
        //Make a similar plot for females. Do this for both 1970 and 2015.
        val countryDataNames = countryData.map(_.countryName)
        val gdpName = gdpData.map(x => ((x.countryName).drop(1).dropRight(1).toString))
        val intersectingNames = (countryDataNames.toSet).intersect(gdpName.toSet).toArray
        val male1970Educ = (countryData.filter(x => x.metric == "Education Per Capita" 
                                            && x.sex == "Males" 
                                            && x.age_group == "25 to 34"
                                            && intersectingNames.contains(x.countryName)
                                            && x.year == 1970)).toSet.toArray.sortWith(_.countryName > _.countryName)

        val gdp1970 = gdpData.filter(x => intersectingNames.contains(x.countryName.drop(1).dropRight(1))).sortWith(_.countryName > _.countryName)
        

        val gdpEduPlot = Plot.simple(
            ScatterStyle (gdp1970.map(_.gdp(10)), male1970Educ.map(_.upper), 
                        symbolWidth = 3, symbolHeight = 3, colors = RedARGB),
                        "Educational Attainment for Males ages 25-34 and GDP in 1970(Using upper)", "GDP", "Education Per Capita")
        //throw out first South Asia on education
        SwingRenderer(gdpEduPlot, 800, 800, true)
        
        val male2015Educ = (countryData.filter(x => x.metric == "Education Per Capita" 
                                            && x.sex == "Males" 
                                            && x.age_group == "25 to 34"
                                            && intersectingNames.contains(x.countryName)
                                            && x.year == 2015)).toSet.toArray.sortWith(_.countryName > _.countryName)

        val gdp2015 = gdpData.filter(x => intersectingNames.contains(x.countryName.drop(1).dropRight(1))).sortWith(_.countryName > _.countryName)
        

        val gdpEduPlot2 = Plot.simple(
            ScatterStyle (gdp2015.map(_.gdp(55)), male2015Educ.map(_.upper), 
                        symbolWidth = 3, symbolHeight = 3, colors = RedARGB),
                        "Educational Attainment for Males ages 25-34 and GDP in 2015(Using upper)", "GDP", "Education Per Capita")
        //throw out first South Asia on education
        SwingRenderer(gdpEduPlot2, 800, 800, true)

        val female1970Educ = (countryData.filter(x => x.metric == "Education Per Capita" 
                                            && x.sex == "Females" 
                                            && x.age_group == "25 to 34"
                                            && intersectingNames.contains(x.countryName)
                                            && x.year == 1970)).toSet.toArray.sortWith(_.countryName > _.countryName)

        //val gdp1970 = gdpData.filter(x => intersectingNames.contains(x.countryName.drop(1).dropRight(1))).sortWith(_.countryName > _.countryName)
        

        val gdpEduPlot3 = Plot.simple(
            ScatterStyle (gdp1970.map(_.gdp(10)), female1970Educ.map(_.upper), 
                        symbolWidth = 3, symbolHeight = 3, colors = BlueARGB),
                        "Educational Attainment for Females ages 25-34 and GDP in 1970(Using upper)", "GDP", "Education Per Capita")
        //throw out first South Asia on education
        SwingRenderer(gdpEduPlot3, 800, 800, true)

        val female2015Educ = (countryData.filter(x => x.metric == "Education Per Capita" 
                                            && x.sex == "Females" 
                                            && x.age_group == "25 to 34"
                                            && intersectingNames.contains(x.countryName)
                                            && x.year == 2015)).toSet.toArray.sortWith(_.countryName > _.countryName)

        //val gdp1970 = gdpData.filter(x => intersectingNames.contains(x.countryName.drop(1).dropRight(1))).sortWith(_.countryName > _.countryName)
        

        val gdpEduPlot4 = Plot.simple(
            ScatterStyle (gdp2015.map(_.gdp(55)), female2015Educ.map(_.upper), 
                        symbolWidth = 3, symbolHeight = 3, colors = BlueARGB),
                        "Educational Attainment for Females ages 25-34 and GDP in 2015(Using upper)", "GDP", "Education Per Capita")
        //throw out first South Asia on education
        SwingRenderer(gdpEduPlot4, 800, 800, true)


        //Make a scatter plot with longitude and latitude on the X and Y axes. Color the points by educational attainment of females ages 25-34. 
        //Have the size of the points indicate the per capita GDP. Do this for both 1970 and 2015.
        
        //val dat1970 = (countryData.filter(x => x.metric == "Education Per Capita" && x.sex == "Females" && x.age_group == "25 to 34" && x.year == 1970))
        //val dat2015 = (countryData.filter(x => x.metric == "Education Per Capita" && x.sex == "Females" && x.age_group == "25 to 34" && x.year == 2015))

        val dat1970_1 = (countryData.filter(x => x.metric == "Education Per Capita" 
                                                    && x.sex == "Females" 
                                                    && x.age_group == "25 to 34"
                                                    && intersectingNames.contains(x.countryName)
                                                    && x.year == 1970)).toSet.toArray.sortWith(_.countryName > _.countryName)
        
        val dat2015_1 = (countryData.filter(x => x.metric == "Education Per Capita" 
                                                    && x.sex == "Females" 
                                                    && x.age_group == "25 to 34"
                                                    && intersectingNames.contains(x.countryName)
                                                    && x.year == 2015)).toSet.toArray.sortWith(_.countryName > _.countryName)

        val gdpDat = gdpData.filter(x => intersectingNames.contains(x.countryName.drop(1).dropRight(1))).sortWith(_.countryName > _.countryName)
        val longLatData = posData.filter(x => intersectingNames.contains(x.country_Name)).sortWith(_.country_Name > _.country_Name)

        val cg = ColorGradient(0.0 -> RedARGB, 12.0 -> GreenARGB, 20.0 -> BlueARGB)

        val sizes1 = gdpDat.map(x => x.gdp(10)*.001)
        val sizes2 = gdpDat.map(x => x.gdp(55)*.001)

        val loglatPlot1 = Plot.simple(
            ScatterStyle(
                longLatData.map(_.longitude), longLatData.map(_.latitude), 
                symbolWidth = sizes1, symbolHeight = sizes1, colors = cg(dat1970_1.map(_.upper))
            ), "Educational Attainment for Females 25-34 in 1970 (using Upper)", "Longitude", "Latitude"
        )
        //TODO
        SwingRenderer(loglatPlot1, 800, 800, true)

        val loglatPlot2 = Plot.simple(
            ScatterStyle(
                longLatData.map(_.longitude), longLatData.map(_.latitude), 
                symbolWidth = sizes2, symbolHeight = sizes2, colors = cg(dat2015_1.map(_.upper))
            ), "Educational Attainment for Females 25-34 in 2015 (using Upper)", "Longitude", "Latitude"
        )
        //TODO
        SwingRenderer(loglatPlot2, 800, 800, true)
    }

    //runMain basicscala.BasicScala
}