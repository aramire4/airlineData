package sparkml

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

case class PoliticalStuff(votesDEM:Double, votesGOP:Double, total_votes:Double, perDEM:Double, perGOP:Double, 
                diff:Double, per_point_diff:Double, state_initial:String, county_name:String, combined_fips:Int)

object SparkCluster extends App {
    val spark = SparkSession.builder().master("local[*]").appName("Bottles").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val area = StructType(Array(
        StructField("area_type", StringType),
        StructField("area_code", StringType),
        StructField("state_name", StringType)
    ))

    val state = StructType(Array(
        StructField("series_id", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType), 
        StructField("footnote_codes", StringType)
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

    val allStates = spark.read.schema(state).
    option("delimiter", "\t").
    option("header", "true").
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")

    val geoData = spark.read.schema(geography)
    .option("header", "true")
    .csv("/data/BigData/bls/zip_codes_states.csv")

    val ds = spark.read.schema(political).
    option("header", "true").
    csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").cache()
    
    val betterDS = ds.map(x => (
        x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(),
        x(6).toString.replaceAll("\\p{Sc}", "").replaceAll(",", ""),
        x(7).toString().dropRight(1),
        x(8).toString(), x(9).toString(), x(10).toString()
    ))

    val politicalData = betterDS.map(x => parseDF(x))

    val wages = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv").cache()

    val stateMap = Map("Alabama" -> "AL", "Alaska" -> "AK", "Arizona" -> "AZ", "Arkansas" -> "AR", 
                        "California" -> "CA", "Colorado" -> "CO", "Connecticut" -> "CT", "Delaware" -> "DE",
                        "Florida" -> "FL", "Georgia" -> "GA", "Hawaii" -> "HI", "Idaho" -> "ID", "Illinois" -> "IL", 
                        "Indiana" -> "IN", "Iowa" -> "IA", "Kansas" -> "KS", "Kentucky" -> "KY", "Louisiana" -> "LA",
                        "Maine" -> "ME", "Maryland" -> "MD", "Massachusetts" -> "MA", "Michigan" -> "MI", "Minnesota" -> "MN", 
                        "Mississippi" -> "MS", "Missouri" -> "MO", "Montana" -> "MT", "Nebraska" -> "NE", "Nevada" -> "NV", "New Hampshire" -> "NH",
                        "New Jersey" -> "NJ", "New Mexico" -> "NM", "New York" -> "NY", "North Carolina" -> "NC", "North Dakota" -> "ND", "Ohio" -> "OH",
                        "Oklahoma" -> "OK", "Oregon" -> "OR", "Pennsylvania" -> "PA", "Rhode Island" -> "RI", "South Carolina" -> "SC", "South Dakota" -> "SD",
                        "Tennessee" -> "TN", "Texas" -> "TX", "Utah" -> "UT", "Vermont" -> "VT", "Virginia" -> "VA", "Washington" -> "WA", "West Virginia" -> "WV",
                        "Wisconsin" -> "WI", "Wyoming" -> "WY")
    val stateNames = stateMap.keySet
    //1. Which aggregation level codes are for county-level data? How many entries are in the main data file for each of the county level codes?
    
    
    
    println("1. Which aggregation level codes are for county-level data? How many entries are in the main data file for each of the county level codes?")
    println("70 - 78")
    val wages1 = wages.select('agglvl_code)
    
    (70 to 78).toArray.map(i =>
        println(i + ": " + wages1.filter(a => a.getInt(0) == i).count() + " entries")
    )
    /*
    70: 13084 entries
    71: 51964 entries
    72: 76460 entries
    73: 277932 entries
    74: 406868 entries
    75: 1100372 entries
    76: 2162440 entries
    77: 3396052 entries
    78: 3984876 entries
    */

    //2. How many entries does the main file have for Bexar County?
    
    println("2. How many entries does the main file have for Bexar County?")
    val bexar = wages.select('area_fips).filter(a => a.get(0) == "48029")
    println(bexar.count() + " entries for Bexar County")
    //9244 entries for Bexar County

    //3. What are the three most common industry codes by the number of records? How many records for each?
    
    println("3. What are the three most common industry codes by the number of records? How many records for each?")
    val industries = wages.groupBy("industry_code").agg(count("industry_code").as("counts"))
    industries.select('industry_code, 'counts).orderBy(desc("counts")).limit(3).show()
/*
        +-------------+------+
        |industry_code|counts|
        +-------------+------+
        |           10| 76952|
        |          102| 54360|
        |         1025| 40588|
        +-------------+------+
*/
    //4. What three industries have the largest total wages for 2016? What are those total wages? (Consider only NAICS 6-digit County values.)
    
    wages.select("agglvl_code", "industry_code", "total_qtrly_wages").filter('agglvl_code === 78).groupBy("industry_code").agg(sum("total_qtrly_wages").as("wages")).orderBy(desc("wages")).limit(3).show()
    println("industry_code 611110: Elementary and secondary schools")
    println("industry_code 551114: Managing Offices")
    println("industry_code 622110: General medical and surgical hospitals")
    
    /*
[info] +-------------+------------+
[info] |industry_code|       wages|
[info] +-------------+------------+
[info] |       611110|244321853090|
[info] |       551114|219387605630|
[info] |       622110|205806016743|
[info] +-------------+------------+
[info] industry_code 611110: Elementary and secondary schools
[info] industry_code 551114: Managing Offices
[info] industry_code 622110: General medical and surgical hospitals
    */
    

    //5. Explore clustering options on the BLS data to try to find clusters that align with voting tendencies. You will do this for two clusters and for more clusters. 
    //(I won't tell you exactly how many, but something like 3, where you have clusters for strongly Democractic, strongly Republican, and neutral would be a option.) 
    //You need to tell me what values you used for dimensions, and how good a job the clustering does of reproducing voting tendencies. 
    //You can do that by giving the fraction of counties that were in the appropriate cluster.
    val refinedArea = areaData.filter('area_type === "F").select('area_code, 'state_name)

    val states = allStates.filter('series_id.substr(19,2) === "03" && ('period === ("M11")) && ('year === "2016")).join(refinedArea, 'series_id.contains('area_code))
                    .select('series_id.substr(1,15).as("id"), 'value.as("unemployment_rate"), 'state_name)

    val refinedStates = allStates.filter('series_id.substr(19,2) === "06" && ('period === ("M11")) && ('year === "2016")).select('series_id.substr(1,15).as("id"), 'value.as("labor_force"))
                                            .join(states, "id").select('id, 'labor_force, 'unemployment_rate, 'state_name)

    val poliArea = politicalData.join(refinedStates, 'state_name.contains('county_name) && 'state_name.contains('state_initial))
                                    //.select('perDEM, 'unemployment_rate, 'labor_force, 'area_code, 'total_votes).distinct().na.drop()
                       
    val data1 = geoData.join(poliArea, 'county_name.contains('county) && 'state_initial.contains('state)).distinct().na.drop()
                                    .select('id, 'perDEM, 'unemployment_rate, 'labor_force, 'total_votes, 'longitude, 'latitude, 'county_name, 'state_initial).distinct().na.drop()

    //joining wage data
    val areaFips = spark.read.option("inferSchema", true).
                            option("header", "true").
                            csv("/data/BigData/bls/qcew/area_titles.csv").cache()

    val w = wages.groupBy('area_fips.as("fips")).agg(sum("total_qtrly_wages").as("wages"))
                                .join(areaFips, ('fips.contains('area_fips) 
                                                    && 'area_fips < 57000 
                                                    && !('area_title.contains("District of Columbia"))
                                                    && !('area_title.contains("Statewide")))).na.drop()
                                .select('wages.as[Long], 'area_title.as[String]).collect()

    val wageMap = w.map(x => (x._1, x._2, x._2.replaceAll(".*,", "").trim()))
                            .map(y => (y._1, y._2, stateMap(y._3)))

    val inputCols = Array("wages", "countyName", "state")

    val df = spark.createDataFrame(wageMap)
    val dfColumns = df.columns
    val query=inputCols.zipWithIndex.map(index=>dfColumns(index._2)+" as "+inputCols(index._2))
    val newDf = df.selectExpr(query:_*)
    //newDf.show()

    val data = data1.join(newDf, 'countyName.contains('county_name) && 'state_initial.contains('state)).select('id, 'perDEM, 'wages, 'unemployment_rate, 'labor_force, 'total_votes, 'longitude, 'latitude, 'county_name, 'state_initial).distinct().na.drop()
    //data.show()

    //actual number of votes based on percent democratic vote
    val dataDem = data.filter('perDEM >= .6).count().toDouble
    val dataRep = data.filter('perDEM <= .4).count().toDouble
    val dataNeutral = data.filter(('perDEM > .4) && ('perDEM < .6)).count().toDouble

    //////////////////////Clustering//////////////////////////
    val cols = Array("perDEM", "total_votes")
    val cols2 = Array("perDEM", "wages")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("data")
    val assembler2 = new VectorAssembler().setInputCols(cols2).setOutputCol("data")
    val featureDF = assembler.transform(data)
    val featureDF2 = assembler2.transform(data)
    val stdScale = new StandardScaler().setInputCol("data").setOutputCol("features").fit(featureDF).transform(featureDF)
    val stdScale2 = new StandardScaler().setInputCol("data").setOutputCol("features").fit(featureDF2).transform(featureDF2)

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
    val model2 = kmeans3.fit(stdScale2)

    //Make prediction
    val predictions = model.transform(stdScale)
    val predictions2 = model2.transform(stdScale2)
    println("************** prediction *******************")
    println("prediction for perDEM and total_votes")
    predictions.show()
    println("prediction for perDEM and wages")
    predictions2.show()

    //Evaluate clustering by computing silhouette score
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    val silhouette2 = evaluator.evaluate(predictions2)
    println(s"*****************Silhouette with squared euclidean distance for total_votes = $silhouette")
    println(s"*****************Silhouette with squared euclidean distance for wages = $silhouette2")

    //results
    println("Cluster Centers for total_votes and perDEM: ")
    model.clusterCenters.foreach(println)
    println("Cluster Centers for wages and perDEM: ")
    model2.clusterCenters.foreach(println)


    val numMajorityDem = predictions.filter('prediction === 0).count().toInt
    val numMinorityDem = predictions.filter('prediction === 1).count().toInt

    val numNeutral2 = predictions2.filter('prediction === 2 && ('perDEM > .4 || 'perDEM < .6)).count().toInt
    val numMinorityDem2 = predictions2.filter('prediction === 0 && 'perDEM < .5).count().toInt
    val numMajorityDem2 = predictions2.filter('prediction === 1 && 'perDEM > .5).count().toInt

    //predictions.filter('prediction === 2).show()

    println("Clustering accuracy with total_votes: ")
    println("Rep Majority: " + numMinorityDem/dataRep + " aka " + numMinorityDem + "/" + dataRep)
    println("Dem Majority: " + numMajorityDem/dataDem + " aka " + numMajorityDem + "/" + dataDem)

    println("Clustering accuracy with wages: ")
    println("Neutral: " + numNeutral2/dataNeutral + " aka " + numNeutral2 + "/" + dataNeutral)
    println("Rep Majority: " + numMinorityDem2/dataRep + " aka " + numMinorityDem2 + "/" + dataRep)
    println("Dem Majority: " + numMajorityDem2/dataDem + " aka " + numMajorityDem2 + "/" + dataDem)

    
    //6. Make scatter plots of the voting results of the 2016 election (I suggest ratio of votes for each party) and each of your groupings. 
    //Use color schemes that make it clear how good a job the clustering did.
    //total_votes
    val pdata = predictions.select('perDEM.as[Double], 'labor_force.as[Double], 'prediction.as[Double]).collect()
    val cg = ColorGradient(0.0 -> BlueARGB, 1.0 -> RedARGB)
    val plot = Plot.simple(ScatterStyle(pdata.map(_._1), pdata.map(_._2), symbolHeight = 6, symbolWidth = 6, colors = cg(pdata.map(_._3))), "Voting Results of the 2016 election (in counties) using perDEM and total_votes to cluster", "Percent democrat vote", "Labor force")
    SwingRenderer(plot, 800, 800, true)

    //wages
    val pdata2 = predictions2.select('perDEM.as[Double], 'labor_force.as[Double], 'prediction.as[Double]).collect()
    val cg2 = ColorGradient(0.0 -> RedARGB, 1.0 -> BlueARGB, 2.0 -> GreenARGB)
    val plot2 = Plot.simple(ScatterStyle(pdata2.map(_._1), pdata2.map(_._2), symbolHeight = 6, symbolWidth = 6, colors = cg2(pdata2.map(_._3))), "Voting Results of the 2016 election (in counties) using perDEM and wages to cluster", "Percent democrat vote", "Labor force")
    SwingRenderer(plot2, 800, 800, true)
    
    spark.sparkContext.stop()

    geoData.unpersist()
    //areaData.unpersist()
    ds.unpersist()
    allStates.unpersist()
    wages.unpersist()
    politicalData.unpersist()
}