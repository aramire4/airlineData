package airlineData

import scala.util._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.functions.col
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.BarStyle.DataAndColor
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.Plot
import swiftvis2.DataSet
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import scala.math
import swiftvis2.plotting.styles.HistogramStyle
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler


object Airline extends App{
    val spark = SparkSession.builder().master("local[*]").appName("Flights").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")


    val flights = StructType(Array(
        StructField("originCode", StringType),
        StructField("destinationCode", StringType),
        StructField("origin_city", StringType),
        StructField("destination_city", StringType),
        StructField("passengers", IntegerType),
        StructField("seats", IntegerType),
        StructField("flights", IntegerType),
        StructField("distance", DoubleType),
        StructField("fly_date", StringType),
        StructField("origin_population", IntegerType),
        StructField("destination_population", IntegerType)
    ))

    val stringPath = "/data/BigData/students/aramire4"
    val ritaPath = "/RITA/"
    val censusBureau = "/Census_Bureau/flight_edges.tsv"
    val airports = "/airports.csv"
    
//headers:
//RITA data:
//Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,
//FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,
//Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,
//NASDelay,SecurityDelay,LateAircraftDelay

//census bureau:
//origin, destination, origin city, destination city, 
//passengers, seats, flights, distance, fly data, origin population, destination population

//airports:
//
    println("The following will take a while to read in")

    val rita1990 = readRITA("1990.csv").cache()
    val rita1991 = readRITA("1991.csv").cache()
    val rita1992 = readRITA("1992.csv").cache()
    val rita1993 = readRITA("1993.csv").cache()
    val rita1994 = readRITA("1994.csv").cache()
    val rita1995 = readRITA("1995.csv").cache()
    val rita1996 = readRITA("1996.csv").cache()
    val rita1997 = readRITA("1997.csv").cache()
    val rita1998 = readRITA("1998.csv").cache()
    val rita1999 = readRITA("1999.csv").cache()
    val rita2000 = readRITA("2000.csv").cache()
    val rita2001 = readRITA("2001.csv").cache()
    val rita2002 = readRITA("2002.csv").cache()
    val rita2003 = readRITA("2003.csv").cache()
    val rita2004 = readRITA("2004.csv").cache()
    val rita2005 = readRITA("2005.csv").cache()
    val rita2006 = readRITA("2006.csv").cache()
    val rita2007 = readRITA("2007.csv").cache()
    val rita2008 = readRITA("2008.csv").cache()
    val allRita = (rita1990.union(rita1991)).union(rita1992).union(rita1993).union(rita1994)
                            .union(rita1995).union(rita1996).union(rita1997).union(rita1998)
                            .union(rita1999).union(rita2000).union(rita2001).union(rita2002)
                            .union(rita2003).union(rita2004).union(rita2005).union(rita2006)
                            .union(rita2007).union(rita2008)


//val rita1997 = readRITA("1997.csv").cache()
//val rita2007 = readRITA("2007.csv").cache()

//"/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1997.csv"

    //stationData1997.show()

    val stationData2007 = spark.read.option("inferSchema", true).option("header", "false").csv("/data/BigData/ghcn-daily/2007.csv").cache()
                            .select('_c0.as("stationCode"), '_c1.as("doy"), '_c2.as("observationType"), '_c3.as("observationVal"), '_c7.as("observationTime"))

    val stationLocations = spark.read.option("inferSchema", true).option("header", "false").csv("/data/BigData/ghcn-daily/ghcnd-stations.txt").cache()
                            .select('_c0.substr(1, 11).as[String].as("stationCode"), '_c0.substr(13, 8).as[Double].as("lat"), '_c0.substr(23, 8).as[Double].as("long"))
    //stationLocations.show()

    val w2007 = stationData2007.join(stationLocations, "stationCode").distinct().na.drop()//.show()

    val weather2007 = w2007.filter('observationType === "TOBS").select('stationCode, 'doy, 'observationVal.as("temperature"), 'lat.as("lat1"), 'long.as("long1")).join(
        w2007.filter('observationType === "PRCP").select('stationCode, 'doy, 'observationVal.as("precip"), 'lat, 'long), Seq("stationCode", "doy")).select('stationCode, 'doy, 'precip, 'temperature, 'lat, 'long)
                                                .filter(('long < -70.0 && 'long > -130.0) && 'lat > 20.0)
    
    weather2007.show()

    def readRITA(str:String): org.apache.spark.sql.DataFrame = {
        val ret = spark.read.
            option("inferSchema", true).
            option("header", "true").
            csv(stringPath+ritaPath+str)

        ret
    }

    val censusFlights = spark.read.schema(flights).
        option("delimiter", "\t").
        option("header", "true").
        csv(stringPath+censusBureau).cache().select('originCode, 'destinationCode, 'origin_city, 'destination_city,
                                                    'passengers, 'seats, 'flights, 'distance, 'fly_date, 'fly_date.substr(1, 4).as[Int].as("fly_year"), 
                                                    'fly_date.substr(5, 2).as[Int].as("fly_month"), 'origin_population, 'destination_population)

    //censusFlights.show()

    val ports = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv(stringPath+airports).cache()

    //ports.show
    /*
    val distanceUdf = spark.udf.register("distanceUdf", ( in1 : Double, in2 : Double)  => {
        Math.abs(in1 - in2)
    })
    */
    
    println("***************************************************************")
    
    val data2007 = rita2007.join(ports, 'IATA.contains('Origin)).select('AirportName, 'City, 'Country, 'Latitude, 'Longitude, 'Year, 
                                                                    'Month, 'DayofMonth, 'DepDelay,'ArrDelay, 'Cancelled, 'CancellationCode, 'Dest)
                                                                    .filter(('Longitude < -70.0 && 'Longitude > -150.0) && 'Latitude > 20.0).toDF()
    data2007.show()
    
    //average delay times
    allRita.groupBy('Year).agg(avg(('ArrDelay)).as("Average arrival time delays")).orderBy('Year).show()
    allRita.groupBy('Year).agg(avg(('DepDelay)).as("Average departure time delays")).orderBy('Year).show()

    //most common reasons for delay



    ////////////////////////////////////////////////


    /*
    val ml2007 = data2007.join(weather2007, distanceUdf(data2007("Longitude"), weather2007("long")) < 3.0 
                                && distanceUdf(data2007("Latitude"), weather2007("lat")) < 3.0).distinct().na.drop()
    ml2007.show()
    */

    //How has the San Antonio airport changed over the years (SAT)
    println("How has the San Antonio airport changed over the years?")
    println("Number of flights each year")
    val saFlights = censusFlights.filter('originCode === "SAT" || 'destinationCode === "SAT").groupBy('fly_year)
    val numFlights = saFlights.agg(sum('flights).as("number_of_flights")).orderBy('fly_year).show()

    println("Number of flights per month ordered by number of flights")
    val saFlights2 = censusFlights.filter('originCode === "SAT" || 'destinationCode === "SAT").groupBy('fly_date)
    val numFlights2 = saFlights2.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights"))//.show()
    numFlights2.show()


    val histSA = numFlights2.orderBy('fly_date).select('number_of_flights.as[Double]).collect()

    val nums = (0 to histSA.length).toArray
    val nums2 = (0 to histSA.length by 12).toArray
    val plotSA = Plot.stacked(Seq(
        ScatterStyle(nums, histSA, 
        symbolWidth = 5, symbolHeight = 5, colors = BlueARGB),
        ScatterStyle(nums2, 6700.0,
        symbolWidth = 4, symbolHeight = 4, colors = RedARGB)),
        "San Antonio flights from 1990 to 2008", "months since 1990", "flights"
    )
    SwingRenderer(plotSA, 800, 800, true)


    /*
    val histSA = numFlights2.orderBy('fly_date).select('fly_date.as[String], 'number_of_flights.as[Double]).collect()
    val bins = histSA.map(_._1.toDouble)

    val plotSA = Plot.histogramPlotFromData(bins : PlotDoubleSeries, histSA.map(_._2) : PlotDoubleSeries, BlueARGB, "San Antonio flights from 1990 to 2008", "year and month", "flights")
    SwingRenderer(plotSA, 800, 800, true)
    */

    //which place is the source of the most travel to San Antonio
    println("What places travel to San Antonio most?")
    val saFlights3 = censusFlights.filter('destinationCode === "SAT").groupBy('origin_city, 'fly_year)
    val numFlights3 = saFlights3.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights")).show()

    val saFlights4 = censusFlights.filter('destinationCode === "SAT").groupBy('origin_city)
    val numFlights4 = saFlights4.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights")).show()

    println("Where do San Antonians travel most?")
    //where do San Antonians travel the most?
    val saFlights5 = censusFlights.filter('originCode === "SAT").groupBy('destination_city, 'fly_year)
    val numFlights5 = saFlights5.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights")).show()

    val saFlights6 = censusFlights.filter('originCode === "SAT").groupBy('destination_city)
    val numFlights6 = saFlights6.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights")).show()

    //Which airport receives the most traffic?
    println("which airport receives the most traffic")
    val trafficIn = censusFlights.groupBy('origin_city, 'fly_date.as("inDate")).agg(sum('flights).as("flightsFrom"))
    val trafficOut = censusFlights.groupBy('destination_city, 'fly_date.as("outDate")).agg(sum('flights).as("flightsTo"))
    val allTraffic = trafficIn.join(trafficOut, 'origin_city.contains('destination_city) && 'inDate.contains('outDate)).select('origin_city, 'inDate.as("fly_date"), 'flightsFrom, 'flightsTo)//.show()
    val traffic = allTraffic.select('origin_city, 'fly_date, (allTraffic("flightsFrom") + allTraffic("flightsTo")).as("flights"))
    traffic.orderBy(desc("flights")).show()

    //What are the longest and shortest flights?
    println("What are the longest and shortest flights?")
    censusFlights.orderBy(desc("distance")).select('origin_city, 'destination_city, 'distance).distinct().show()

    //visualization
    println("visualization 1")
    val trafficIn1990 = censusFlights.filter('fly_year === 1990).groupBy('originCode).agg(sum('flights).as("flightsFrom"))
    val trafficOut1990 = censusFlights.filter('fly_year === 1990).groupBy('destinationCode).agg(sum('flights).as("flightsTo"))
    val allTraffic1990 = trafficIn1990.join(trafficOut1990, 'originCode.contains('destinationCode)).select('originCode.as("airportCode"), 'flightsFrom, 'flightsTo)//.show()
    val traffic1990 = allTraffic1990.select('airportCode, (allTraffic1990("flightsFrom") + allTraffic1990("flightsTo")).as("flights"))

    val trafficIn2008 = censusFlights.filter('fly_year === 2008).groupBy('originCode).agg(sum('flights).as("flightsFrom"))
    val trafficOut2008 = censusFlights.filter('fly_year === 2008).groupBy('destinationCode).agg(sum('flights).as("flightsTo"))
    val allTraffic2008 = trafficIn2008.join(trafficOut2008, 'originCode.contains('destinationCode)).select('originCode.as("airportCode"), 'flightsFrom, 'flightsTo)//.show()
    val traffic2008 = allTraffic2008.select('airportCode, (allTraffic2008("flightsFrom") + allTraffic2008("flightsTo")).as("flights"))

    //val portLocation = ports.select('IATA, 'Longitude, 'Latitude).show()
    val ports1990 = traffic1990.join(ports, 'airportCode.contains('IATA)).filter('Longitude > -130.0).na.drop().select('flights.as[Double], 'Longitude.as[Double], 'Latitude.as[Double]).collect()
    val ports2008 = traffic2008.join(ports, 'airportCode.contains('IATA)).filter('Longitude > -130.0 && 'Latitude > 0.0).na.drop().select('flights.as[Double], 'Longitude.as[Double], 'Latitude.as[Double]).collect()

    val sz1 = ports1990.map(_._1 / 20000 + 5.0)

    val plot1 = Plot.simple(
    ScatterStyle(ports1990.map(_._2): PlotDoubleSeries, ports1990.map(_._3): PlotDoubleSeries, 
    symbolWidth = sz1, symbolHeight = sz1, colors = RedARGB),
    "Airports 1990", "Longitude", "Latitude"
    )
    SwingRenderer(plot1, 800, 800, true)
    
    val sz2 = ports2008.map(_._1 / 20000 + 5.0)

    val plot2 = Plot.simple(
    ScatterStyle(ports2008.map(_._2): PlotDoubleSeries, ports2008.map(_._3): PlotDoubleSeries, 
    symbolWidth = sz2, symbolHeight = sz2, colors = BlueARGB),
    "Airports 2008", "Longitude", "Latitude"
    )
    SwingRenderer(plot2, 800, 800, true)

    //What time of year has the most flights?
    println("what time of year has the most flights?")
    val monthlyFlights = censusFlights.groupBy('fly_month).agg(sum('flights).as("totalFlights")).orderBy('fly_month).show()

    val monthFlights1990 = censusFlights.filter('fly_year === 1990).groupBy('fly_month).agg(sum('flights).as("totalFlights_1990"))//.orderBy('fly_month).show()
    val monthFlights2008 = censusFlights.filter('fly_year === 2008).groupBy('fly_month).agg(sum('flights).as("totalFlights_2008"))//.orderBy('fly_month).show()
    monthFlights1990.orderBy('fly_month).show()
    monthFlights1990.orderBy('totalFlights_1990).show()
    monthFlights2008.orderBy('fly_month).show()
    monthFlights2008.orderBy('totalFlights_2008).show()

    //average people per flight
    println("Average people per flight")
        val passPerFlight = censusFlights.groupBy('fly_year).agg((sum('passengers)/sum('flights)).as("Average_per_flight")).orderBy('fly_year)
        passPerFlight.show()
        val seatsTaken = censusFlights.groupBy('fly_year).agg((sum('passengers)/sum('seats)).as("percent_seats_filled")).orderBy('fly_year)
        seatsTaken.show()
    
        val d1 = passPerFlight.select('Average_per_flight.as[Double]).collect()
        val d2 = seatsTaken.select(('percent_seats_filled * 100).as[Double]).collect()
        val num3 = (1990 to 2008 by 1).toArray
    
        val plot3 = Plot.stacked(Seq(
            ScatterStyle(num3, d1, 
            symbolWidth = 7, symbolHeight = 7, colors = BlueARGB),
            ScatterStyle(num3, d2, symbolWidth = 7, symbolHeight = 7, colors = RedARGB)),
            "Increase in flight business", "year", "average passengers per flight(blue) and percent of seats filled(red)"
        )
        SwingRenderer(plot3, 900, 900, true)

    //Which places receive seasonal spikes in travel?


    //Have there been breaks in the usual trends?


    //Over the years have some places gradually gained or lost travel?


    //Linear regression on census bureau data to try and predict the number of flights
    println("Linear regression")
    val namesSubset = Set("origin_population", "destination_population", "distance", "passengers", "seats").subsets(3).map(_.toArray).toArray
    var arrs = Seq[(Double, Seq[String])]() //return of the error and the strings that made it

    (0 to namesSubset.length-1).toArray.map(pos =>
        arrs :+= doThings(namesSubset(pos))
    )

    val minArr = arrs.minBy(_._1)
    minArr._2.foreach(println)
    println("error: " + minArr._1)

    def doThings(seqs : Array[String]) : (Double, Seq[String]) = {
        val s1 = seqs(0)
        val s2 = seqs(1)
        val s3 = seqs(2)

        val dataWithO2_1 = censusFlights.select(s1, s2, s3, "flights").na.drop()
        val dataVA6_1 = new VectorAssembler().setInputCols(Array(s1, s2, s3)).setOutputCol("temp")
        val dataWithTemp6_1 = dataVA6_1.transform(dataWithO2_1)

        val lr6_1 = new LinearRegression()
            .setFeaturesCol("temp")
            .setLabelCol("flights")

        val lr6_1Model = lr6_1.fit(dataWithTemp6_1)
        val fitData6_1 = lr6_1Model.transform(dataWithTemp6_1)
        val lr6Size1 = fitData6_1.count().toDouble
        val fitDouble = fitData6_1.select(('flights - 'prediction).as[Double]).collect().map(a => Math.abs(a)).sum / lr6Size1
        (fitDouble, seqs)
    }


//////////////////////Clustering//////////////////////////
val cols = Array("Cancelled", "Month", "Latitude")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("data")
val featureDF = assembler.transform(data2007)
val stdScale = new StandardScaler().setInputCol("data").setOutputCol("features").fit(featureDF).transform(featureDF)

//Trains k-means model
val kmeans2 = new KMeans()
                .setK(2)
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
val kmeans3 = new KMeans()
                .setK(3)
                .setFeaturesCol("features")
                .setPredictionCol("prediction")

val model = kmeans2.fit(stdScale)

//Make prediction
val predictions = model.transform(stdScale)
println("************** prediction *******************")
println("prediction for Cancelled, Month, and Latitude")
predictions.show()

//Evaluate clustering by computing silhouette score
val evaluator = new ClusteringEvaluator()
val silhouette = evaluator.evaluate(predictions)
println(s"*****************Silhouette with squared euclidean distance for DepDelay = $silhouette")

//results
println("Cluster Centers for Cancelled, Month, and Latitude: ")
model.clusterCenters.foreach(println)


//    val numMajorityDem = predictions.filter('prediction === 0).count().toInt
//    val numMinorityDem = predictions.filter('prediction === 1).count().toInt

    

    //Can we predict the delay time based on the temperature at the origin and destination city?

}