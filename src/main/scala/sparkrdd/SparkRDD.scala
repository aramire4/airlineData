package sparkrdd

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
    observationTime:Int
)

object SparkRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Station Data").setMaster("local[*]")
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
        StationData(p(0), p(1), p(2), p(3).toInt, p(7).toInt)
      }
      else StationData(p(0), p(1), p(2), p(3).toInt, -1)
    }
    

    //val source2 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
    val source2 = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
    //val line2 = source2.collect()
    val stationData = source2.map(s => parseStations(s))//.toArray
    
    //println(stationData.count)

    val source3 = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
    val line3 = source3.getLines()
    val countryData = (line3.map(parseCountryData)).filter(_.countryName != "").toArray

   
    //How many stations are there in the state of Texas?
    val q1 = countryData.filter(_.stateInit == "TX")
    println("1. How many stations are there in the state of Texas? " + q1.length)
    
    //How many of those stations have reported some form of data in 2017?
    
    val txStationIDs = q1.map(x => x.id).toSet
    
    val q2 = stationData.filter(x => txStationIDs.contains(x.stationCode)).map(x => x.stationCode).distinct()
    println("2. How many Texas stations have reported data in 2017: " + q2.count())
    
    //What is the highest temperature reported anywhere this year? Where was it and when?
    val q3 = stationData.filter(x => x.observationType == "TMAX").reduce((x, y) => if(x.observationVal > y.observationVal) x else y)
    val q3City = countryData.filter(x => x.id == q3.stationCode)
    println("3. Highest temp reported in 2017: " + q3.observationVal + " from " + q3City(0).cityName + " in " + q3.doy)
   
    //How many stations in the stations list haven't reported any data in 2017?
    val q4 = stationData.map(x => x.stationCode).distinct()
    println("4. How many stations in the stations list have not reported data in 2017? " + (countryData.length - q4.count()))
    
    //What is the maximum rainfall for any station in Texas during 2017? What station and when?
    val q5 = stationData.filter(x => x.observationType == "PRCP" && txStationIDs.contains(x.stationCode))
                                .reduce((x, y) => if(x.observationVal > y.observationVal) x else y)
    val q5City = countryData.filter(x => x.id == q5.stationCode)
    println("5. Max rainfall reported in 2017 in Texas: " + q5.observationVal + " from " + q5City(0).cityName + " in " + q5.doy)

    //What is the maximum rainfall for any station in India during 2017? What station and when?
    val indiaStationIDs = countryData.filter(_.countryInitial == "IN").map(x =>x.id).toSet
    val q6 = stationData.filter(x => x.observationType == "PRCP" && indiaStationIDs.contains(x.stationCode))
                                .reduce((x, y) => if(x.observationVal > y.observationVal) x else y)
    val q6City = countryData.filter(x => x.id == q6.stationCode)
    println("6. Max rainfall reported in 2017 in India: " + q6.observationVal + " from " + q6City(0).cityName + " in " + q6.doy)
    
    //How many weather stations are there associated with San Antonio, TX?
    
    val q7 = countryData.filter(x => x.cityName.contains("SAN ANTONIO") && x.stateInit == "TX")
    println("7. Weather stations associated with San Antonio, TX: " + q7.length)
    
    //How many of those have reported temperature data in 2017?
    val saStationIDs = q7.map(_.id).toSet
    val saStationData = stationData.filter(x => saStationIDs.contains(x.stationCode))
                                    .filter(x => x.observationType == "TMAX" || x.observationType == "TMIN" || x.observationType == "TAVG" || x.observationType == "TOBS")
                                    .map(_.stationCode).distinct().collect()
    println("8. How many of the San Antonio stations have reported temperature data in 2017: " + saStationData.length)
    //What is the largest daily increase in high temp for San Antonio in this data file?
    val q9 = stationData.filter(x => saStationIDs.contains(x.stationCode)).filter(x => x.observationType == "TMAX").collect()
    val station1 = q9.filter(x => x.stationCode == saStationData(0))
    val station2 = q9.filter(x => x.stationCode == saStationData(1))
    val station3 = q9.filter(x => x.stationCode == saStationData(2))
    val station4 = q9.filter(x => x.stationCode == saStationData(3))


    val groupedData1 = station1.groupBy(_.doy) mapValues (_ map (_.observationVal)) //group data by doy and stationCode
    val maxEachDay1 = groupedData1.map(x => (x._1, x._2.max)).toSeq.sortBy(_._1).toArray //map to only have the biggest TMAX of each day as value and doy as key and sort based on doy

    val med1 = maxEachDay1.map(x => x._2)
    val days1 = maxEachDay1.map(x => x._1)
    val tempChange1 = (med1 zip med1.drop(1)).map({ case (a, b) => b - a })
    val max1 = (days1 zip tempChange1.take(tempChange1.length))

    val groupedData2 = station2.groupBy(_.doy) mapValues (_ map (_.observationVal))
    val maxEachDay2 = groupedData2.map(x => (x._1, x._2.max)).toSeq.sortBy(_._1).toArray 

    val med2 = maxEachDay2.map(x => x._2)
    val days2 = maxEachDay2.map(x => x._1)
    val tempChange2 = (med2 zip med2.drop(1)).map({ case (a, b) => b - a })
    val max2 = (days2 zip tempChange2.take(tempChange2.length))

    val groupedData3 = station3.groupBy(_.doy) mapValues (_ map (_.observationVal))
    val maxEachDay3 = groupedData3.map(x => (x._1, x._2.max)).toSeq.sortBy(_._1).toArray 

    val med3 = maxEachDay3.map(x => x._2)
    val days3 = maxEachDay3.map(x => x._1)
    val tempChange3 = (med3 zip med3.drop(1)).map({ case (a, b) => b - a })
    val max3 = (days3 zip tempChange3.take(tempChange3.length))

    val groupedData4 = station4.groupBy(_.doy) mapValues (_ map (_.observationVal))
    val maxEachDay4 = groupedData4.map(x => (x._1, x._2.max)).toSeq.sortBy(_._1).toArray 

    val med4 = maxEachDay4.map(x => x._2)
    val days4 = maxEachDay4.map(x => x._1)
    val tempChange4 = (med4 zip med4.drop(1)).map({ case (a, b) => b - a })
    val max4 = (days4 zip tempChange4.take(tempChange4.length))

    val allStations = max1 ++ max2 ++ max3 ++ max4
    val biggestChange = allStations.maxBy(_._2)
    println("9. Largest daily increase in high temp for San Antonio: " + biggestChange._2 + " from " + biggestChange._1 + " to " +  biggestChange._1.take(7) + (biggestChange._1.takeRight(1).toInt + 1))

    //What is the correlation coefficient between high temperatures and rainfall for San Antonio? Note that you can only use values from the same date and station for the correlation.
    val q10 = (stationData.filter(x => x.stationCode == saStationData(2)).filter(x => x.observationType == "TMAX" || x.observationType == "PRCP").collect()).groupBy(_.doy) mapValues (_ map (_.observationVal))
    val temps = q10.map(x => x._2(0)) //Map all of the temp data
    val precips = q10.map(x => x._2(1)) //Map all of the precipitation data

    val tempAvg = temps.sum / temps.size //Find the average
    val precipAvg = precips.sum / precips.size
    val tempStandDev = Math.sqrt(temps.map(x => Math.pow((x - tempAvg), 2)).reduce(_+_) / (temps.size-1)) //Get the standard deviation
    val precipStandDev = Math.sqrt(precips.map(x => Math.pow((x - precipAvg), 2)).reduce(_+_) / (precips.size-1))
    
    val zxs = temps.map(x => (x - tempAvg)/tempStandDev) //for each element, subtract the average and divide by standard deviation
    val zys = precips.map(y => (y - precipAvg)/precipStandDev)
    val addedProduct = (zxs zip zys).map({case(a,b) => a*b}).reduce(_+_) //multiply each pair of temperature and precipitation data and then add them together
    println("10. Correlation coefficient between high temperatures and rainfall for San Antonio: " + (addedProduct/(zxs.size-1))) 
    //^dividing addedProduct by the total number of points-1 yields the correlation coefficient^

    //Make a plot of temperatures over time for five different stations, each separated by at least 10 degrees in latitude. Make sure you tell me which stations you are using.
    val nums = (1 to 365).toArray
    val tempStations = stationData.filter(_.observationType == "TMAX")
    val stat1 = tempStations.filter(x => x.stationCode == saStationData(2)).map(x => (x.observationVal.toDouble/10.0)).collect() //San Antonio USW00012970 -98 lat
    val stat2 = tempStations.filter(x => x.stationCode == "ASN00085296").map(x => (x.observationVal.toDouble/10.0)).collect() //Mount Moornapa Australia -37 lat
    val stat3 = tempStations.filter(x => x.stationCode == "CA003076680").map(x => (x.observationVal.toDouble/10.0)).collect() //valleyview alberta canada 55 lat
    val stat4 = tempStations.filter(x => x.stationCode == "USS0018F01S").map(x => (x.observationVal.toDouble/10.0)).collect() //Rocksprings Oregon -118 lat
    val stat5 = tempStations.filter(x => x.stationCode == "SWE00140492").map(x => (x.observationVal.toDouble/10.0)).collect() //Pitea Sweden 65 lat
    

    val scatterArray = Array(
      ScatterStyle(nums, stat1, symbolWidth = 5, symbolHeight = 5, colors = BlueARGB),
      ScatterStyle(nums, stat2, symbolWidth = 5, symbolHeight = 5, colors = RedARGB),
      ScatterStyle(nums, stat3, symbolWidth = 5, symbolHeight = 5, colors = GreenARGB),
      ScatterStyle(nums, stat4, symbolWidth = 5, symbolHeight = 5, colors = YellowARGB),
      ScatterStyle(nums, stat5, symbolWidth = 5, symbolHeight = 5, colors = BlackARGB)
    )
    val tempPlot1 = Plot.stacked(
      scatterArray, "Temperature Over Time", "Time", "Temperature (In Celcius)"
    )  
    SwingRenderer(tempPlot1, 800, 800, true)
    
    sc.stop()
  }
}
