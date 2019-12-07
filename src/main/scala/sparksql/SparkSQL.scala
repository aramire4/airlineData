package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer


object SparkSQL extends App {
    
    val spark = SparkSession.builder().master("spark://pandora00:7077").appName("Unemployment Data").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val state = StructType(Array(
        StructField("series_id", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType), 
        StructField("footnote_codes", StringType)
    ))

    val area = StructType(Array(
        StructField("area_type", StringType),
        StructField("area_code", StringType),
        StructField("state_name", StringType)
    ))

    val geography = StructType(Array(
      StructField("zip", StringType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("city", StringType),
      StructField("state", StringType),
      StructField("county", StringType)
    ))

    //In each state's file the last 2 number of series_id are:
    //03 = Unemployment Rate
    //04 = Unemployed Population
    //05 = Employed Population
    //06 = Labor Force

    /*
    period  period_abbr period_name
    M01 JAN January
    M02 FEB February
    M03 MAR March
    M04 APR April
    M05 MAY May
    M06 JUN June
    M07 JUL July
    M08 AUG August
    M09 SEP September
    M10 OCT October
    M11 NOV November
    M12 DEC December
    M13 AN AV Annual Average
    */

    /*
    area_type Codes:
    A: 
    B: Matropolitan Statistical Areas and NECTAs
    C: Metropolitan Divisions NECTA
    D: Micropolitan Statistical Areas
    E: Combined Statistical Areas and NECTAs
    F: Countries and equivalents
    G: Cities and Towns
    H: Cities and Towns
    */

    val areaData = spark.read.schema(area).
    option("delimiter", "\t").
    option("header", "true").
    csv("/data/BigData/bls/la/la.area").cache()

    //1. How many data series does the state of New Mexico have?
    val nm = spark.read.schema(state).
    option("delimiter", "\t").
    option("header", "true").
    csv("/data/BigData/bls/la/la.data.38.NewMexico").cache()

    println("1. How many data series does the state of New Mexico have?")
    nm.agg(count('series_id)).show()
    //99504
    
    //2. What is the highest unemployment level (not rate) reported for a county or equivalent in the time series for the state of New Mexico?
    println("2. What is the highest unemployment level reported for a county  in the time series for New Mexico")
    val nmCounties = areaData.filter('area_type === "F" && 'area_code.substr(3, 2) === "35")
    nmCounties.join(nm).filter('series_id.contains('area_code)).where('series_id.substr(19, 2) === "04").select(max('value)).show()


    //3. How many cities/towns with more than 25,000 people does the BLS track in New Mexico?
    println("3. How many cities with more than 25,000 people does the BLS track in New Mexico?")
    
    val nmCities = areaData.filter('area_type === "G" && 'area_code.substr(3, 2) === "35")
    nmCities.agg(count('area_code)).show()

    //4. What was the average unemployment rate for New Mexico in 2017? Calculate this in three ways:
    val urNewMexico = nm.filter('series_id.substr(19, 2) === "03" && 'year === "2017")

    //a. Averages of the months for the BLS series for the whole state.
    println("4.a. Averages of the months for the BLS series for the whole state")
    val byMonths = urNewMexico.groupBy('period).avg("value").sort("period")
    byMonths.show()

    //b. Simple average of all unemployment rates for counties in the state.
    println("4.b. Simple average of all unemployment rates for counties in the state")
    val byCounties = urNewMexico.join(areaData).filter('series_id.contains('area_code)).where('area_type === "F")
    (byCounties.agg(sum("value")/byCounties.count())).show()

    //5. This is a continuation of the last question. Calculate the unemployment rate in a third way and discuss the differences.

    //c. Weighted average of all unemployment rates for counties in the state where the weight is the labor force in that month.
    println("5.c Weighted average of all unemployment rates for counties in the state where the weight is the labor force in that month")
    val newMexico2017 = nm.filter('year === "2017" && !('period === ("M13")))
    val laborForce = nmCounties.join(newMexico2017).filter('series_id.contains('area_code)).where('series_id.substr(19,2) === "06").select('series_id.substr(1,15).as("id"), 'period, 'value.as("force"))
    val rates = nmCounties.join(newMexico2017).filter('series_id.contains('area_code)).where('series_id.substr(19,2) === "03").select('series_id.substr(1,15).as("id"), 'period, 'value.as("rate"))
    val weighted = laborForce.join(rates, Seq("id", "period")).select(sum('rate * 'force))
    weighted.show()


    //d. How do your two averages compare to the BLS average? Which is more accurate and why?
    println("5.d. How do your two averages compare to the BLS average? Which is more accuracte and why?")
    println("")

    //6. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the state of Texas? When and where? (raise labor force limit for next year)
    println("6. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the state of Texas? When and where?")
    val tx = spark.read.schema(state).
            option("delimiter", "\t").
            option("header", "true").
            csv("/data/BigData/bls/la/la.data.51.Texas").cache()

    val txLaborForce10000 = tx.filter('series_id.substr(19,2) === "06" && ('value >= 10000) && !('period === ("M13"))).select('series_id.substr(1,15).as("ID"), 'period, 'year)
    val txRates = tx.filter('series_id.substr(19,2) === "03" && !('period === ("M13"))).select('series_id.substr(1,15).as("ID"), 'period, 'value.as("rate"), 'year)
    val highestUnemployment = txLaborForce10000.join(txRates, Seq("ID", "period", "year"))
    val highestTexasRate = highestUnemployment.select(max('rate)).first().get(0)

    println(highestTexasRate)
    highestUnemployment.select('ID, 'period, 'year, 'rate).where('rate === highestTexasRate.toString()).distinct().show() 
    
    nm.unpersist()
    tx.unpersist()


    //The following questions involve all the states.
    val allStates = spark.read.schema(state).
                    option("delimiter", "\t").
                    option("header", "true").
                    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")

    //7. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the full dataset? When and where? (raise labor force limit for next year)
    println("7. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the full dataset? When and where?")
    val laborForce10000 = allStates.filter('series_id.substr(19,2) === "06" && ('value >= 10000) && !('period === ("M13"))).select('series_id.substr(1,15).as("ID"), 'period, 'year)
    val allRates = allStates.filter('series_id.substr(19,2) === "03" && !('period === ("M13"))).select('series_id.substr(1,15).as("ID"), 'period.as("rPeriod"), 'value.as("rVal"), 'year.as("rYear"))
    val highestTotal = laborForce10000.join(allRates, Seq("ID"))
    val highestRate = highestTotal.select(max('rVal)).first().get(0)
    highestTotal.select('ID, 'rPeriod, 'rYear, 'rVal).where('rVal === highestRate.toString()).distinct().show()


    //8. Which state has most distinct data series? How many series does it have?
    println("8. Which state has most distinct data series? How many series does it have?")
    val dataSeries = allStates.groupBy('series_id.substr(6,2).as("stateID")).agg(countDistinct('series_id).as("dataSeries"))
    val mostDistinct = dataSeries.select(max('dataSeries)).first().get(0)
    dataSeries.select('stateID).where('dataSeries === mostDistinct.toString()).show()
    println("23 is Maine btw")
    println(mostDistinct + " distinct data series")

    //9. We will finish up by looking at unemployment geographically and over time. I want you to make a grid of scatter plots for the years 2000, 2005, 2010, and 2015. 
    //Each point is plotted at the X, Y for latitude and longitude and it should be colored by the unemployment rate. 
    //If you are using SwiftVis2, the Plot.scatterPlotGrid method is particularly suited for this. 
    //Only plot data from the continental US, so don't include data from Alaska, Hawaii, or Puerto Rico.

    val geoData = spark.read.schema(geography)
    .option("header", "true")
    .csv("/data/BigData/bls/zip_codes_states.csv")

    val prelimData = allStates.filter(!('series_id.substr(6, 2) === "02") && !('series_id.substr(6, 2) === "15") && !('series_id.substr(6, 2) === "72"))

    val all2000 = prelimData.filter('year === "2000")
    val all2005 = prelimData.filter('year === "2005")
    val all2010 = prelimData.filter('year === "2010")
    val all2015 = prelimData.filter('year === "2015")


    val data2000 = all2000.filter('series_id.substr(19,2) === "03").select('series_id, 'value)
    val grouped2000 = data2000.groupBy('series_id).avg("value")
    val joined2000 = grouped2000.join(areaData, 'series_id.contains('area_code))
    val local2000 = joined2000.join(geoData, 'state_name.contains('city) && 'state_name.contains('state)).select('series_id, joined2000("avg(value)").as("average"), 'latitude, 'longitude).distinct().na.drop()

    val data2005 = all2005.filter('series_id.substr(19,2) === "03").select('series_id, 'value)
    val grouped2005 = data2005.groupBy('series_id).avg("value")
    val joined2005 = grouped2005.join(areaData, 'series_id.contains('area_code))
    val local2005 = joined2005.join(geoData, 'state_name.contains('city) && 'state_name.contains('state)).select('series_id, joined2005("avg(value)").as("average"), 'latitude, 'longitude).distinct().na.drop()

    val data2010 = all2010.filter('series_id.substr(19,2) === "03").select('series_id, 'value)
    val grouped2010 = data2010.groupBy('series_id).avg("value")
    val joined2010 = grouped2010.join(areaData, 'series_id.contains('area_code))
    val local2010 = joined2010.join(geoData, 'state_name.contains('city) && 'state_name.contains('state)).select('series_id, joined2010("avg(value)").as("average"), 'latitude, 'longitude).distinct().na.drop()

    val data2015 = all2015.filter('series_id.substr(19,2) === "03").select('series_id, 'value)
    val grouped2015 = data2015.groupBy('series_id).avg("value")
    val joined2015 = grouped2015.join(areaData, 'series_id.contains('area_code))
    val local2015 = joined2015.join(geoData, 'state_name.contains('city) && 'state_name.contains('state)).select('series_id, joined2015("avg(value)").as("average"), 'latitude, 'longitude).distinct().na.drop()


    val color = ColorGradient(0.0 -> BlueARGB, 7.5 -> GreenARGB, 15.0 -> RedARGB)
    val longLat2000 = local2000.select('average.as[Double], 'longitude.as[Double], 'latitude.as[Double]).collect()
    val longLat2005 = local2005.select('average.as[Double], 'longitude.as[Double], 'latitude.as[Double]).collect()
    val longLat2010 = local2010.select('average.as[Double], 'longitude.as[Double], 'latitude.as[Double]).collect()
    val longLat2015 = local2015.select('average.as[Double], 'longitude.as[Double], 'latitude.as[Double]).collect()
    val sz = 3.0

    val grid = Plot.scatterPlotGrid(Seq(
        Seq((longLat2000.map(_._2) : PlotDoubleSeries, longLat2000.map(_._3) : PlotDoubleSeries, (longLat2000.map(t => (color(t._1))) : PlotIntSeries), sz: PlotDoubleSeries), 
        (longLat2005.map(_._2) : PlotDoubleSeries, longLat2005.map(_._3) : PlotDoubleSeries, (longLat2005.map(t => (color(t._1))) : PlotIntSeries), sz: PlotDoubleSeries)),
        Seq((longLat2010.map(_._2) : PlotDoubleSeries, longLat2010.map(_._3) : PlotDoubleSeries, (longLat2010.map(t => (color(t._1))) : PlotIntSeries), sz: PlotDoubleSeries), 
        (longLat2015.map(_._2) : PlotDoubleSeries, longLat2015.map(_._3) : PlotDoubleSeries, (longLat2015.map(t => (color(t._1))) : PlotIntSeries), sz: PlotDoubleSeries))),
        "US Unemployment Rates over Time", "Longitude", "Latitude")

    SwingRenderer(grid, 2000, 1500, true)

    geoData.unpersist()
    allStates.unpersist()
    spark.sparkContext.stop()
} 