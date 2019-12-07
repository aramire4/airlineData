package sparkrdd2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer
import breeze.linalg.all
import scalafx.scene.effect.BlendMode.Red
import scalafx.scene.effect.BlendMode.Green
import scalafx.scene.effect.BlendMode.Blue
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import swiftvis2.plotting.styles.BarStyle.DataAndColor
import io.netty.handler.codec.redis.RedisArrayAggregator
import spire.std.`package`.double
import swiftvis2.plotting.styles.HistogramStyle

//import swiftvis2.spark._

case class CountryInit(countryInitial:String, countryName:String)

case class CountryData(
    countryName: String,
    cityName: String,
    latitude: Double,
    longitude: Double,
    elevation:Double,
    countryInitial:String,
    stateInit:String,
    id:String
)

case class StationData(
    stationCode:String,
    doy:String,
    observationType:String,
    observationVal:Int,
    observationTime:Int,
    qualityFlag:String,
)

object SparkRDD2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Station Data").setMaster("spark://pandora00:7077")//.setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")
    
    // Your code here.

    def parseCountryInitials(line:String):CountryInit = {
      CountryInit(line.substring(0, 2), line.substring(3, line.length()).trim())
    }

    val source1 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-countries.txt")
    val line1 = source1.getLines()
    val initials = line1.map(parseCountryInitials).toArray


    
    def parseCountryData(line:String):CountryData = {
      val init = line.substring(0,2)
      val temp = initials.filter(_.countryInitial == init)

      if(temp.length == 0){
        CountryData("", "", 0.0, 0.0, 0.0, "", "", "")
      }
      else{
        CountryData(temp(0).countryName, line.substring(41, 71).trim(), 
                    line.substring(12,20).trim().toDouble, line.substring(21,30).trim().toDouble, line.substring(31,37).trim().toDouble,
                    temp(0).countryInitial, line.substring(38,40), line.substring(0,11))
      }
    }
    
    def parseStations(line:String):StationData = {
      val p = line.split(",")
        if(p.length == 8){
          StationData(p(0), p(1), p(2), p(3).toInt, p(7).toInt, p(5))
        }
        else StationData(p(0), p(1), p(2), p(3).toInt, -1, p(5))

    }
    

    val source2 = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
    val stationData_2017 = source2.map(s => parseStations(s))

    val source3 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
    val line3 = source3.getLines()
    val countryData = (line3.map(parseCountryData)).filter(_.countryName != "").toArray

    val source4 = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1897.csv")
    val stationData_1897 = source4.map(s => parseStations(s))
    
    //1. What location has reported the largest temperature difference in one day (TMAX-TMIN) in 2017? What is the difference and when did it happen?
    val tMaxData = stationData_2017.filter(_.observationType == "TMAX").map(x => ((x.stationCode, x.doy), x.observationVal))
    val tMinData = stationData_2017.filter(_.observationType == "TMIN").map(x => ((x.stationCode, x.doy), x.observationVal))
    val joinedData = (tMaxData.join(tMinData))

    val q1 = joinedData.map({case(a, b) => (a, (b._1 - b._2))} ).reduce((x, y) => if(x._2 > y._2) x else y)
    /*
    val tempDiff = joinedData.map({case(a, b) => (a, (b._2 - b._1))} ).map(x => x._2).max//.reduce((x, y) => if(x._2 > y._2) x else y)
    val q1 = q1Data.filter(x => x._2 == tempDiff).collect()
    */
    val q1City = countryData.filter(x => x.id == (q1._1)._1)
    
    println("1. What location has reported the largest temperature difference in one day in 2017: " + q1City(0).cityName + 
            " with a difference of " + q1._2 + " on " + (q1._1)._2)
    

    //2. What location has reported the largest difference between min and max temperatures overall in 2017. What was the difference?
    val initSet = mutable.HashSet.empty[Int]
    val addTo = (s: mutable.HashSet[Int], v: Int) => s += v
    val merge = (p1: mutable.HashSet[Int], p2: mutable.HashSet[Int]) => p1 ++= p2
    
    val tMaxData2 = (stationData_2017.filter(_.observationType == "TMAX").map(x => (x.stationCode, x.observationVal)).aggregateByKey(initSet)(addTo, merge)).map(x => (x._1, x._2.max))
    val tMinData2 = (stationData_2017.filter(_.observationType == "TMIN").map(x => (x.stationCode, x.observationVal)).aggregateByKey(initSet)(addTo, merge)).map(x => (x._1, x._2.min))
    val joinedData2 = (tMaxData2.join(tMinData2))
    val q2 = joinedData2.map({case(a, b) => (a, (b._1 - b._2))} ).reduce((x, y) => if(x._2 > y._2) x else y)
    val q2City = countryData.filter(x => x.id == (q2._1))
    //TODO check
    println("2. What location has reported the largest temperature difference in between min and max in 2017: " + q2City(0).cityName + 
            " with a difference of " + q2._2)

    def standardDeviation(data:RDD[Int]):Double = {
      val m = data.sum / data.count
      (Math.sqrt(data.map(a => Math.pow((a.toDouble - m), 2)).sum / (data.count)))
    }

    //3. What is the standard deviation of the high temperatures for all US stations in 2017? What about low temperatures?
    val highTemps2017 = stationData_2017.filter(x => x.observationType == "TMAX").map(_.observationVal)
    val lowTemps2017 = stationData_2017.filter(x => x.observationType == "TMIN").map(_.observationVal)
    
    val stdDevHighs = standardDeviation(highTemps2017)
    val stdDevLows = standardDeviation(lowTemps2017)
    println("3. What is the standard deviation of high temps in  2017? " + stdDevHighs + " low temps: " + stdDevLows)
    //4. How many stations reported data in both 1897 and 2017?dr
    val stat2017 = stationData_2017.map(x => x.stationCode).distinct().collect().toSet
    val stat1897 = stationData_1897.map(x => x.stationCode).distinct().collect().toSet
    val bothStats = stat1897.filter(x => stat2017.contains(x))
    println("4. How many stations reported data in both 1897 and 2017? " + bothStats.size)

    //5. Does temperature variability change with latitude in the US in 2017? Consider three groups of latitude: lat<35, 35<lat<42, and 42<lat. Answer this question in the following three ways.
      println("5. Does temperature variabililty change with latitude in the US in 2017?")
      //a. Standard deviation of high temperatures.
      val codesOfLat35 = countryData.filter(_.latitude < 35.0).map(_.id).toSet
      val codesOfLat35to42 = countryData.filter(x => x.latitude > 35.0 && x.latitude < 42.0).map(_.id).toSet
      val codesOfLat42 = countryData.filter(_.latitude > 42.0).map(_.id).toSet

      val high35 = stationData_2017.filter(x => x.observationType == "TMAX" && codesOfLat35.contains(x.stationCode)).map(_.observationVal)
      val high35to42 = stationData_2017.filter(x => x.observationType == "TMAX" && codesOfLat35to42.contains(x.stationCode)).map(_.observationVal)
      val high42 = stationData_2017.filter(x => x.observationType == "TMAX" && codesOfLat42.contains(x.stationCode)).map(_.observationVal)
      
      println("5a. Standard deviation of high temperatures")
      println(" latitude < 35: " + standardDeviation(high35))
      println(" 35 < latitude < 42: " + standardDeviation(high35to42))
      println(" 42 < latitude: " + standardDeviation(high42))

      //b. Standard deviation of average daily temperatures (when you have both a high and a low for a given day at a given station).
      val avg35 = joinedData.filter(x => codesOfLat35.contains(x._1._1)).map(x => ((x._2._1 + x._2._2)/2))
      val avg35to42 = joinedData.filter(x => codesOfLat35to42.contains(x._1._1)).map(x => ((x._2._1 + x._2._2)/2))
      val avg42 = joinedData.filter(x => codesOfLat42.contains(x._1._1)).map(x => ((x._2._1 + x._2._2)/2))

      println("5b. Standard deviation of average daily temperatures")
      println(" latitude < 35: " + standardDeviation(avg35))
      println(" 35 < latitude < 42: " + standardDeviation(avg35to42))
      println(" 42 < latitude: " + standardDeviation(avg42))

      //c. Make histograms of the high temps for all stations in each of the regions so you can visually inspect the breadth of the distribution.
      val ar35 = high35.collect()
      val ar35to42 = high35to42.collect()
      val ar42 = high42.collect()

      //val bins = -100 to 100 //latitude
      val bins1 = -100 to 35
      val bins2 = 35 to 42
      val bins3 = 42 to 100
      /*
      Plot.stackedHistogramPlot(bins, Seq(
        DataAndColor(ar35, BlueARGB),
        DataAndColor(ar35to42, RedARGB),
        DataAndColor(ar42, GreenARGB)),
        true, "High Temperatures by Region", "latitude", "temperature"
      )
      */
      
      val ht35 = Plot.histogramPlotFromData(bins1, ar35, BlueARGB, "High Temperatures where Latitude < 35", "Latitude", "Temperature")
      val ht35to42 = Plot.histogramPlotFromData(bins2, ar35to42, RedARGB, "High Temperatures where 35 < Latitude < 42", "Latitude", "Temperature")
      val ht42 = Plot.histogramPlotFromData(bins3, ar42, GreenARGB, "High Temperatures where 42 < Latitude", "Latitude", "Temperature")
      
      SwingRenderer(ht35, 800, 800, true)
      SwingRenderer(ht35to42, 800, 800, true)
      SwingRenderer(ht42, 800, 800, true)
    //6. Plot the average high temperature for every station that has reported temperature data in 2017 with a scatter plot using longitude and latitude for x and y 
    //and the average daily temperature for color. Make 100F or higher solid red, 50F solid green, and 0F or lower solid blue. 
    //Use a SwiftVis2 ColorGradient for the scatter plot color.

    val q6 = (stationData_2017.filter(_.observationType == "TMAX").map(x => (x.stationCode, x.observationVal)).aggregateByKey(initSet)(addTo, merge)).map(x => (x._1, (x._2.sum.toDouble/x._2.size.toDouble))).collect()
    val q6IDs = q6.map(_._1)
    val latLongs = countryData.map(x => (x.id, x.longitude, x.latitude)).filter(x => q6IDs.contains(x._1)).sortBy(_._1)
    val tmp = latLongs.map(_._1)
    val qNotContained = q6.filter(x => !tmp.contains(x._1)).map(_._1)
    val newQ6 = q6.filter(x => !qNotContained.contains(x._1)).sortBy(_._1)

    
    val cg = ColorGradient(150.0 -> RedARGB, 50.0 -> GreenARGB, -150.0-> BlueARGB)

    //println(q6.count())
    
    val avgHighTempData = Plot.simple(
        ScatterStyle(latLongs.map(_._2), latLongs.map(_._3), symbolWidth = 5, symbolHeight = 5 , colors = cg(newQ6.map(_._2))),
        "Average High Temperatures", "longitude", "latitude")
    
    SwingRenderer(avgHighTempData, 800, 800, true)


    //7. How much has the average land temperature changed from 1897 to 2017? We will calculate this in a few ways.
    println("7. How much has the average land temperature changed from 1897 to 2017?")
      //a. Calculate the average of all temperature values of all stations for 1897 and compare to the same for 2017.
      val q7_2017 = (stationData_2017.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal))
      val avg2017 = q7_2017.sum() / q7_2017.count()

      val q7_1897 = (stationData_1897.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal))
      val avg1897 = q7_1897.sum() / q7_1897.count()

      println(" 7a. average of 2017 temperatures: " + avg2017 + ", average of 1897 temperatures: " + avg1897)

      //b. Calculate the average of all temperature values only for stations that reported temperature data for both 1897 and 2017.
      val q7b_2017 = (stationData_2017.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => bothStats.contains(x.stationCode)).map(_.observationVal))
      val avgFiltered2017 = q7b_2017.sum() / q7b_2017.count()

      val q7b_1897 = (stationData_1897.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => bothStats.contains(x.stationCode)).map(_.observationVal))
      val avgFiltered1897 = q7b_1897.sum() / q7b_1897.count()
      println(" 7b. average of temperature values only for stations that reported temperature data for both 1897 and 2017")
      println("   2017: " + avgFiltered2017 + ", average of 1897 temperatures: " + avgFiltered1897)

      //c. Plot data using approach (a) for all years I give you data for from 1897 to 2017. (On the Pandora machines under /data/BigData/ghcn-daily you will find a file for every 10 years from 1897 to 2017.)
      val source5 = sc.textFile("/data/BigData/ghcn-daily/1907.csv")
      val stationData_1907 = source5.map(s => parseStations(s))
      val q7_1907 = stationData_1907.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1907 = q7_1907.sum() / q7_1907.count()

      val source6 = sc.textFile("/data/BigData/ghcn-daily/1917.csv")
      val stationData_1917 = source6.map(s => parseStations(s))
      val q7_1917 = stationData_1917.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1917 = q7_1917.sum() / q7_1917.count()

      val source7 = sc.textFile("/data/BigData/ghcn-daily/1927.csv")
      val stationData_1927 = source7.map(s => parseStations(s))
      val q7_1927 = stationData_1927.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1927 = q7_1927.sum() / q7_1927.count()

      val source8 = sc.textFile("/data/BigData/ghcn-daily/1937.csv")
      val stationData_1937 = source8.map(s => parseStations(s))
      val q7_1937 = stationData_1937.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1937 = q7_1937.sum() / q7_1937.count()

      val source9 = sc.textFile("/data/BigData/ghcn-daily/1947.csv")
      val stationData_1947 = source9.map(s => parseStations(s))
      val q7_1947 = stationData_1947.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1947 = q7_1947.sum() / q7_1947.count()

      val source10 = sc.textFile("/data/BigData/ghcn-daily/1957.csv")
      val stationData_1957 = source10.map(s => parseStations(s))
      val q7_1957 = stationData_1957.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1957 = q7_1957.sum() / q7_1957.count()

      val source11 = sc.textFile("/data/BigData/ghcn-daily/1967.csv")
      val stationData_1967 = source11.map(s => parseStations(s))
      val q7_1967 = stationData_1967.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1967 = q7_1967.sum() / q7_1967.count()

      val source12 = sc.textFile("/data/BigData/ghcn-daily/1977.csv")
      val stationData_1977 = source12.map(s => parseStations(s))
      val q7_1977 = stationData_1977.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1977 = q7_1977.sum() / q7_1977.count()

      val source13 = sc.textFile("/data/BigData/ghcn-daily/1987.csv")
      val stationData_1987 = source13.map(s => parseStations(s))
      val q7_1987 = stationData_1987.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1987 = q7_1987.sum() / q7_1987.count()

      val source14 = sc.textFile("/data/BigData/ghcn-daily/1997.csv")
      val stationData_1997 = source14.map(s => parseStations(s))
      val q7_1997 = stationData_1997.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg1997 = q7_1997.sum() / q7_1997.count()

      val source15 = sc.textFile("/data/BigData/ghcn-daily/2007.csv")
      val stationData_2007 = source15.map(s => parseStations(s))
      val q7_2007 = stationData_2007.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").map(_.observationVal)
      val avg2007 = q7_2007.sum() / q7_2007.count()
      val tempData1897to2017 = Array(avg1897, avg1907, avg1917, avg1927, avg1937, avg1947, avg1957, avg1967, avg1977, avg1987, avg1997, avg2007, avg2017)
      
      val time1 = Array(1897, 1907, 1917, 1927, 1937, 1947, 1957, 1967, 1977, 1987, 1997, 2007, 2017)
      val avgHighTempData7c = Plot.simple(
        ScatterStyle(time1, tempData1897to2017, symbolWidth = 10, symbolHeight = 10 , colors = RedARGB),
        "Average of High and Low Temperatures (tenths of degrees Celcius)", "1897-2017", "Average Temperature")
    
      SwingRenderer(avgHighTempData7c, 800, 800, true)
      
      //d. Plot data using approach (b) for all years I give you data for from 1897 to 2017. (Use the files for every 10 years.)
      val stat2007 = stationData_2007.map(x => x.stationCode).distinct().collect().toSet
      val stat1997 = stationData_1997.map(x => x.stationCode).distinct().collect().toSet
      val stat1987 = stationData_1987.map(x => x.stationCode).distinct().collect().toSet
      val stat1977 = stationData_1977.map(x => x.stationCode).distinct().collect().toSet
      val stat1967 = stationData_1967.map(x => x.stationCode).distinct().collect().toSet
      val stat1957 = stationData_1957.map(x => x.stationCode).distinct().collect().toSet
      val stat1947 = stationData_1947.map(x => x.stationCode).distinct().collect().toSet
      val stat1937 = stationData_1937.map(x => x.stationCode).distinct().collect().toSet
      val stat1927 = stationData_1927.map(x => x.stationCode).distinct().collect().toSet
      val stat1917 = stationData_1917.map(x => x.stationCode).distinct().collect().toSet
      val stat1907 = stationData_1907.map(x => x.stationCode).distinct().collect().toSet
      var allStats = bothStats.filter(x => stat2007.contains(x) && stat1997.contains(x) && stat1987.contains(x) &&
                                            stat1977.contains(x) && stat1967.contains(x) && stat1957.contains(x) &&
                                            stat1947.contains(x) && stat1937.contains(x) && stat1927.contains(x) &&
                                            stat1917.contains(x) && stat1907.contains(x))

      val q7d_1907 = stationData_1907.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1907_7d = q7d_1907.sum() / q7d_1907.count()

      val q7d_1917 = stationData_1917.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1917_7d = q7d_1917.sum() / q7d_1917.count()

      val q7d_1927 = stationData_1927.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1927_7d = q7d_1927.sum() / q7d_1927.count()

      val q7d_1937 = stationData_1937.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1937_7d = q7d_1937.sum() / q7d_1937.count()

      val q7d_1947 = stationData_1947.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1947_7d = q7d_1947.sum() / q7d_1947.count()

      val q7d_1957 = stationData_1957.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1957_7d = q7d_1957.sum() / q7d_1957.count()

      val q7d_1967 = stationData_1967.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1967_7d = q7d_1967.sum() / q7d_1967.count()

      val q7d_1977 = stationData_1977.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1977_7d = q7d_1977.sum() / q7d_1977.count()

      val q7d_1987 = stationData_1987.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1987_7d = q7d_1987.sum() / q7d_1987.count()

      val q7d_1997 = stationData_1997.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg1997_7d = q7d_1997.sum() / q7d_1997.count()

      val q7d_2007 = stationData_2007.filter(x => x.observationType == "TMAX" || x.observationType == "TMIN").filter(x => allStats.contains(x.stationCode)).map(_.observationVal)
      val avg2007_7d = q7d_2007.sum() / q7d_2007.count()

      val tempData1897to2017_7d = Array(avgFiltered1897, avg1907_7d, avg1917_7d, avg1927_7d, avg1937_7d, 
          avg1947_7d, avg1957_7d, avg1967_7d, avg1977_7d, avg1987_7d, avg1997_7d, avg2007_7d, avgFiltered2017)
      
      val time2 = Array(1897, 1907, 1917, 1927, 1937, 1947, 1957, 1967, 1977, 1987, 1997, 2007, 2017)
      val avgHighTempData7d = Plot.simple(
        ScatterStyle(time2, tempData1897to2017_7d, symbolWidth = 10, symbolHeight = 10 , colors = BlueARGB),
        "Average of High and Low Temperatures from stations that reported data for all years", "1897-2017", "Average Temperature (tenths of degrees Celcius)")
    
      SwingRenderer(avgHighTempData7d, 800, 800, true)
    
    //8. Describe the relative merits and flaws with approach (a) and (b) for question 7. What would be a better approach to answering that question?
    println("8. Describe the relative merits and flaws with approach (a) and (b) [See GitHub]")

    //9. While answering any of the above questions, did you find anything that makes you believe there is a flaw in the data? If so, what was it, and how would you propose identifying and removing such flaws in serious analysis?
    println("9. While answering any of the above question, did you find anything that makes you belive there is a flaw in the data? [See GitHub]")
    
  }
}