package sparksql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.BarStyle.DataAndColor

case class PoliticalStuff(votesDEM:Double, votesGOP:Double, total_votes:Double, perDEM:Double, perGOP:Double, 
                diff:Double, per_point_diff:Double, state_initial:String, county_name:String, combined_fips:Int)


object SparkSQL2 extends App {
    val spark = SparkSession.builder().master("local[*]").appName("Unemployment Data").getOrCreate()
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

    val political = StructType(Array(
        StructField("index", IntegerType),
        StructField("votesDEM", DoubleType),
        StructField("votesGOP", DoubleType),
        StructField("total_votes", DoubleType),
        StructField("perDEM", DoubleType),
        StructField("perGOP", DoubleType),
        StructField("diff", StringType), //check
        StructField("per_point_diff", StringType), //check
        StructField("state_initial", StringType), 
        StructField("county_name", StringType),
        StructField("combined_fips", IntegerType)
    ))

    val political2 = StructType(Array(
        StructField("votesDEM", DoubleType),
        StructField("votesGOP", DoubleType),
        StructField("total_votes", DoubleType),
        StructField("perDEM", DoubleType),
        StructField("perGOP", DoubleType),
        StructField("diff", DoubleType), //check
        StructField("per_point_diff", DoubleType), //check
        StructField("state_initial", StringType), 
        StructField("county_name", StringType),
        StructField("combined_fips", IntegerType)
    ))

    def parseDF(ds: (String, String, String, String, String, String, String, String, String, String)):PoliticalStuff = {
        PoliticalStuff(ds._1.toDouble, ds._2.toDouble, ds._3.toDouble,
        ds._4.toDouble, ds._5.toDouble, ds._6.toDouble, ds._7.toDouble,
        ds._8, ds._9, ds._10.toInt)
    }

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
    B: Metropolitan Statistical Areas and NECTAs
    C: Metropolitan Divisions NECTA
    D: Micropolitan Statistical Areas
    E: Combined Statistical Areas and NECTAs
    F: Counties and equivalents
    G: Cities and Towns
    H: Cities and Towns
    */

    val areaData = spark.read.schema(area).
    option("delimiter", "\t").
    option("header", "true").
    csv("/data/BigData/bls/la/la.area").cache()


    val ds = spark.read.schema(political).
    option("header", "true").
    csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").cache()

    val allStates = spark.read.schema(state).
    option("delimiter", "\t").
    option("header", "true").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")

    val betterDS = ds.map(x => (
        x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(),
        x(6).toString.replaceAll("\\p{Sc}", "").replaceAll(",", ""),
        x(7).toString().dropRight(1),
        x(8).toString(), x(9).toString(), x(10).toString()
    ))

    val politicalData = betterDS.map(x => parseDF(x))

    //1. What fraction of counties had a Republican majority?
    val reps = politicalData.filter(row => row.votesGOP > row.votesDEM)
    println("1. What fraction of counties had a Republican majority")
    println(reps.count().toDouble / politicalData.count().toDouble)

    //2. What fraction of counties went Republican by a margin of 10% or more? What about Democratic?
    val repPer = politicalData.filter(row => (row.perGOP - row.perDEM) >= .1)
    val demPer = politicalData.filter(row => (row.perDEM - row.perGOP) >= .1)
    println("2. What fraction of counties went Republican by a margin of 10% or more? what about Democratic?")
    println("Republican: " + repPer.count().toDouble / politicalData.count().toDouble)
    println("Democratic: " + demPer.count().toDouble /politicalData.count().toDouble)
    
    //3. Plot the election results with an X-axis of the number of votes cast and a Y-axis of the percent Democratic votes minus the percent Republican votes.
    val votesCast = politicalData.map(_.total_votes).filter(_ < 1200000.0).collect()
    val perDEMREP = politicalData.map(row => (row.perDEM - row.perGOP)).collect()
    val sz = 5
    val plot3 = Plot.simple(
        ScatterStyle(votesCast, perDEMREP,
        symbolWidth = sz, symbolHeight = sz, colors = RedARGB),
        "Election Results 2016", "votes cast", "% Democratic votes - % Republican votes"
    )

    SwingRenderer(plot3, 800, 800, true)

    //4. Using both the election results and the zip code data, plot the election results by county geographically. So X is longitude, Y is latitude
    //the color is a based on percent Democratic with 40% or lower being solid red and 60% or higher being solid blue.

    val geoData = spark.read.schema(geography)
    .option("header", "true")
    .csv("/data/BigData/bls/zip_codes_states.csv")

    val perDem = politicalData.select('perDEM, 'state_initial, 'county_name)

    val refinedGeo = geoData.join(perDem, 'county_name.contains('county) && 'state_initial.contains('state)).distinct().na.drop()
                        .select('perDEM.as[Double], 'longitude.as[Double], 'latitude.as[Double]).collect()

    val cg = ColorGradient(0.4 -> RedARGB, 0.6 -> BlueARGB)

    val plot4 = Plot.simple(
        ScatterStyle(refinedGeo.map(_._2) : PlotDoubleSeries, refinedGeo.map(_._3) : PlotDoubleSeries, 
        symbolWidth = sz, symbolHeight = sz, colors = refinedGeo.map(t => cg(t._1))),
        "Election Results by county geographically", "longitude", "latitude"
    )
    SwingRenderer(plot4, 1000, 1000, true)

    //5. In this question, I want to look at the impact of some recent recessions on the unemployment distributions for the US. In particular, I want you to look at the following recessions:
        val filteredStates = allStates.filter('series_id.substr(19,2) === "03")
        //a. 7/1990 - 3/1991
        val states1990 = filteredStates.filter(('period === "M06") && ('year === "1990")).select('series_id, 'value)
        val states1991 = filteredStates.filter(('period === "M04") && ('year === "1991")).select('series_id, 'value)
        
        //b. 3/2001 - 11/2001
        val states2_2001 = filteredStates.filter(('period === "M02") && ('year === "2001")).select('series_id, 'value)
        val states12_2001 = filteredStates.filter(('period === "M12") && ('year === "2001")).select('series_id, 'value)
        //c. 12/2007 - 6/2009
        val states2007 = filteredStates.filter('period === "M11" && 'year === "2007").select('series_id, 'value)
        val states2009 = filteredStates.filter('period === "M07" && 'year === "2009").select('series_id, 'value)


    val bins = ((0.0 to 30.0) by 1.0).toArray

    //For each one, I want you to make a number of histograms for two different months. One is the month before the recession started, and the other is the last month of the recession. 
    //You will make a grid of histograms of the unemployment rates for all states combined (so you can use the big combined file I made) with bins of (0.0 to 50.0 by 1.0) for the types of series listed below for those six months. 
    //Note that the Plot.histogramGrid method can help you to make the grid. How has the distribution of unemployment rates changed over time?
    //(6x3 grid of histograms)
    //6th dimension is time
    
        //a. Metropolitan Areas
        val metropolitanArea = areaData.filter('area_type === "B").select('area_code)
        val metro1990 = metropolitanArea.join(states1990, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val metro1991 = metropolitanArea.join(states1991, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val metro2001_1 = metropolitanArea.join(states2_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val metro2001_2 = metropolitanArea.join(states12_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val metro2007 = metropolitanArea.join(states2007, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val metro2009 = metropolitanArea.join(states2009, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)


        //b. Micropolitan Areas
        val micropolitanArea = areaData.filter('area_type === "D").select('area_code)
        val micro1990 = micropolitanArea.join(states1990, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val micro1991 = micropolitanArea.join(states1991, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val micro2001_1 = micropolitanArea.join(states2_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val micro2001_2 = micropolitanArea.join(states12_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val micro2007 = micropolitanArea.join(states2007, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val micro2009 = micropolitanArea.join(states2009, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        //c. Counties and Equivalents
        val countyData = areaData.filter('area_type === "F").select('area_code)
        val county1990 = countyData.join(states1990, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val county1991 = countyData.join(states1991, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val county2001_1 = countyData.join(states2_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val county2001_2 = countyData.join(states12_2001, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        val county2007 = countyData.join(states2007, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)
        val county2009 = countyData.join(states2009, 'series_id.contains('area_code)).select('value.as[Double]).rdd.histogram(bins)

        //I suggest that you make a grid with three rows and six columns. The rows are the different types of areas while the columns are the six different months. 
        //You might want to use colors to differentiate the month before a recession from the last month of the recession. Perhaps green for the month before and red for the last month. 
        //So you would have six columns that alternate green and red histograms. What do you observe in these plots?
        
        
        import swiftvis2.plotting.styles.HistogramStyle.DataAndColor
        val histo = Plot.histogramGrid(bins, Seq(
                Seq(DataAndColor(metro1990, RedARGB), DataAndColor(metro1991, RedARGB), DataAndColor(metro2001_1, RedARGB), DataAndColor(metro2001_2, RedARGB), DataAndColor(metro2007, RedARGB), DataAndColor(metro2009, RedARGB)),
                Seq(DataAndColor(micro1990, GreenARGB), DataAndColor(micro1991, GreenARGB), DataAndColor(micro2001_1, GreenARGB), DataAndColor(micro2001_2, GreenARGB), DataAndColor(micro2007, GreenARGB), DataAndColor(micro2009, GreenARGB)),
                Seq(DataAndColor(county1990, BlueARGB), DataAndColor(county1991, BlueARGB), DataAndColor(county2001_1, BlueARGB), DataAndColor(county2001_2, BlueARGB), DataAndColor(county2007, BlueARGB), DataAndColor(county2009, BlueARGB))
            ), false, false, "Unemployment Rates before and after recessions", "Areas", "unemployment Rates"
        )
        SwingRenderer(histo, 2000, 1500, true)
        
    //6. I am interested in correlations between employment status and voting tendencies. Let's look at this in a few different ways.
    def mean(x: Seq[Double]):Double = {
        x.sum / x.length.toDouble
    }
    
    def stdev(x: Seq[Double]): Double = {
        var m = mean(x)
        (Math.sqrt(x.map(a => Math.pow((a - m), 2)).reduce(_+_) / (x.length)))
    } 
    
    def covariance(x: Seq[Double], y: Seq[Double]): Double = {
        var xAvg = mean(x)
        var yAvg = mean(y)
        var xs = x.map(a => a - xAvg)
        var ys = y.map(b => b - yAvg)
        var mult = (xs zip ys).map({case(a, b) => a*b}).reduce(_+_)
        mult/(x.length)
    }

    def correlationCoefficient(x: Seq[Double], y: Seq[Double]): Double = {
        if(x.length != y.length) -1.0
        covariance(x, y)/(stdev(x)*stdev(y))
    }
       
        //a. For all counties, calculate the correlation coefficient between the unemployment rate and percent democratic vote for November 2016. (Note that this will be a single number.) What does that number imply?
        

        val refinedStates = allStates.filter('series_id.substr(19,2) === "03" && ('period === ("M11")) && ('year === "2016")).select('series_id, 'value)
        val refinedArea = areaData.filter('area_type === "F").select('area_code, 'state_name)
        val joinedStateAreas = refinedStates.join(refinedArea, 'series_id.contains('area_code)).select('value, 'area_code, 'state_name).distinct().na.drop()

        val poliArea = politicalData.join(joinedStateAreas, 'state_name.contains('county_name) && 'state_name.contains('state_initial)).select('perDEM, 'value, 'area_code, 'total_votes).distinct().na.drop()
        val corr = correlationCoefficient(poliArea.select('perDEM.as[Double]).collect(), poliArea.select('value.as[Double]).collect())
        println("6a. For all counties, calculate correlation coefficient b/w the unemployment rate and percent democratic vote: " + corr)

        //b. Make a scatter plot that you feel effectively shows the three values of population, party vote, and the unemployment rate (again in November 2016). For the population, you can use the labor force or the number of votes cast. Your scatter plot should have one point per county. In addition to X and Y, you can use size and color. What does your plot show about these three values?
        println("6b. scatter plot of population, party vote, unemployment rate")
        val popStates = allStates.filter('series_id.substr(19,2) === "06" && ('period === ("M11")) && ('year === "2016") && ('value <= 1500000.0 )).select('series_id, 'value.as("laborForce"))
        val population = popStates.join(poliArea, 'series_id.contains('area_code)).distinct().na.drop()
        val populationPlusRates = population.select('value.as[Double], 'laborForce.as[Double], 'perDEM.as[Double]).collect()
        //populationPlusRates.show()

        val plot6b = Plot.simple(
            ScatterStyle(populationPlusRates.map(_._2) : PlotDoubleSeries, populationPlusRates.map(_._1) : PlotDoubleSeries, 
            symbolWidth = sz, symbolHeight = sz, colors = populationPlusRates.map(t => cg(t._3))),
            "labor force and unemployment rate compared to party vote", "labor force", "unemployment rate"
        )

        SwingRenderer(plot6b, 800, 800, true)

    //7. Look at the relationships between voter turnout (approximate by votes/labor force), unemployment, and political leaning. 
    //Are there any significant correlations between these values? If so, what are they? You can use plots or statistical measures to draw and illustrate your conclusions.
    //need labor force, total votes, unemployment rate, perDem
    val population7 = population.select('perDEM.as[Double], 'total_votes.divide('laborForce).as[Double], 'value.as[Double]).collect()
    val plot7 = Plot.simple(
        ScatterStyle(population7.map(_._2) : PlotDoubleSeries, population7.map(_._3) : PlotDoubleSeries,
        symbolWidth = sz, symbolHeight = sz, colors = population7.map(t => cg(t._1))),
        "Relationship b/w voter turnout, unemployment, and political leaning", "Voter Turnout", "Unemployment Rate"
    )
    SwingRenderer(plot7, 800, 800, true)

    geoData.unpersist()
    areaData.unpersist()
    ds.unpersist()
    allStates.unpersist()
}