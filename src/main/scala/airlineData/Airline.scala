package airlineData

import scala.util._
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
import swiftvis2.DataSet
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import scala.math


object Airline extends App{
    val spark = SparkSession.builder().master("local[*]").appName("Flights").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    /*
    val rita = StructType(Array(
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("dayOfMonth", IntegerType),
        StructField("dayOfWeek", IntegerType),
        StructField("actualDepartureTime", IntegerType),
        StructField("scheduledDepartureTime", IntegerType),
        StructField("actualArrivalTime", IntegerType),
        StructField("scheduledArrivalTime", IntegerType),
        StructField("carrierCode", StringType),
        StructField("flightNumber", IntegerType),
        StructField("tailNumber", IntegerType),
        StructField("actualElapsedTime", DoubleType),
        StructField("scheduledElapsedTime", DoubleType),
        StructField("airTime", DoubleType),
        StructField("arrivalDelay", DoubleType),
        StructField("departureDelay", DoubleType),
        StructField("origin", StringType),
        StructField("destination", StringType),
        StructField("distance", DoubleType),
        StructField("taxiIn", DoubleType),
        StructField("taxiOut", DoubleType),
        StructField("cancelled", IntegerType),
        StructField("cancellationCode", StringType),
        StructField("diverted", IntegerType),
        StructField("carrierDelay", DoubleType),
        StructField("weatherDelay", DoubleType),
        StructField("nasDelay", DoubleType),
        StructField("securityDelay", DoubleType),
        StructField("lateAircraftDelay", DoubleType)
    ))
    */

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

    val stringPath = "/Users/anthonyramirez/Desktop/bigDataFinal/"
    val ritaPath = "airlineData/dataFiles/RITA/"
    val censusBureau = "airlineData/dataFiles/Census_Bureau/flight_edges.tsv"
    val airports = "airlineData/dataFiles/airports.csv"
    /*
    val rita1990 = spark.read.schema(rita).
        option("delimiter", "\t").
        option("header", "true").
        csv(stringPath+"airlineData/dataFiles/RITA/1990.csv").cache()
    */
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

    val rita1990 = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv(stringPath+ritaPath+"1990.csv").cache()
    /*
    val rita2008 = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv(stringPath+ritaPath+"2008.csv").cache()
    */
    //rita1990.show()

    val censusFlights = spark.read.schema(flights).
        option("delimiter", "\t").
        option("header", "true").
        csv(stringPath+censusBureau).cache().select('originCode, 'destinationCode, 'origin_city, 'destination_city,
                                                    'passengers, 'seats, 'flights, 'fly_date, 'fly_date.substr(1, 4).as("fly_year").as[Int], 
                                                    'fly_date.substr(5, 2).as("fly_month").as[Int], 'origin_population, 'destination_population)

    //censusFlights.show()

    val ports = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv(stringPath+airports).cache()

    //ports.show
    
    //rita1990.take(10).foreach(println)
    //println("--------------------------------------------")

    /*
    val all1990 = rita1990.join(censusFlights, 'Origin.contains('originCode) &&
                                 'Dest.contains('destinationCode) &&
                                 'fly_year.contains('Year)
                                 ).na.drop()
    */

    //all1990.show()

    //How has the San Antonio airport changed over the years (SAT)
    val saFlights = censusFlights.filter('originCode === "SAT" || 'destinationCode === "SAT").groupBy('fly_year)
    val numFlights = saFlights.agg(sum('flights).as("number_of_flights")).orderBy('fly_year).show()

    val saFlights2 = censusFlights.filter('originCode === "SAT" || 'destinationCode === "SAT").groupBy('fly_date)
    val numFlights2 = saFlights2.agg(sum('flights).as("number_of_flights")).orderBy(desc("number_of_flights")).show()

    //which place is the source of the most travel to San Antonio
    val saFlights3 = censusFlights.filter('destinationCode === "SAT").groupBy('fly_date)
    
    //Which airport receives the most/least traffic?


    //What time of year has the most flights?

    //Which places receive seasonal spikes in travel?

    //Have there been breaks in the usual trends?

    //Over the years have some places gradually gained or lost travel?

    //Does the population of an area correlate to travel to that area?

    //How many people travel yearly? Daily? Weekly? Has this changed over the years?

    //What are the longest and shortest flights?

    //Can we predict the delay time based on the temperature at the origin and destination city?

}