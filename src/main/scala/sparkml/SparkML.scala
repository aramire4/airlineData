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
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler


object SparkML extends App {
    val spark = SparkSession.builder().master("local[*]").appName("Bottles").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val bottles = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv("/data/BigData/Oceans/bottle.csv").cache()

    val bottles2 = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv("/data/BigData/Oceans/bottle.csv").cache().withColumnRenamed("Sta_ID", "StaID")

    val cast = spark.read.
        option("inferSchema", true).
        option("header", "true").
        csv("/data/BigData/Oceans/cast.csv").cache()

    //1. How many columns in bottle.csv have data for at least half the rows?
    var totalColumns = 0
    val colNames = bottles.schema.fieldNames.filter(a => (a != "Oxy_µmol/Kg") && (a != "DIC Quality Comment"))
    val rows = bottles.select(colNames(0)).count().toDouble //total number of rows
    
    colNames.map(name => 
        if(bottles.filter(name + " is null").count.toDouble < rows/2) totalColumns += 1
        //println(bottles.filter(name + " is null").count)
    )
    
    println("1. How many columns in bottle.csv have data for at least half the rows? \n\t" + totalColumns + " columns")

    //2. How many bottles were in each cast on average? (Don't make this harder than it needs to be.)
    println("2. Bottles in each cast on average: ")
    val q2 = bottles.groupBy("Cst_Cnt").agg(count("Btl_Cnt").as("Counts"))
    q2.select(avg("Counts")).show()
    //println(q2.count())

    //3. Plot the locations of the casts using Lat_Dec and Lon_Dec. Make the point size indicate depth and the point color indicate temperature.

    val cs = cast.join(bottles2, "Cst_Cnt").select('Lon_Dec, 'Lat_Dec, 'Depthm, 'T_degC).na.drop()
                                                .select('Lon_Dec.as[Double],'Lat_Dec.as[Double], 'Depthm.as[Double], 'T_degC.as[Double]).collect()//.collect()
    val cg = ColorGradient(10.0 -> BlueARGB, 26.0 -> RedARGB)
    val sz = cs.map(_._3 / 500 + 2.0)
    
    val plot1 = Plot.simple(
        ScatterStyle(cs.map(_._1): PlotDoubleSeries, cs.map(_._2): PlotDoubleSeries, 
        symbolWidth = sz, symbolHeight = sz, colors = cs.map(t => cg(t._4))),
        "Bottles found", "Longitude", "Latitude"
    )
    SwingRenderer(plot1, 800, 800, true)
    

    //4. Using linear regression, make a prediction of salinity based only on temperature for the bottles data. What is the average error in your predictions?
    val dataWithTemp = bottles.select('T_degC, 'Salnty).na.drop()
    val dataVA = new VectorAssembler().setInputCols(Array("T_degC")).setOutputCol("temp")
    val dataWithTempFeature = dataVA.transform(dataWithTemp)
  
    val lr4 = new LinearRegression()
      .setFeaturesCol("temp")
      .setLabelCol("Salnty")
  
    val lrModel = lr4.fit(dataWithTempFeature)
    val fitData = lrModel.transform(dataWithTempFeature)
    val lr4Size = fitData.count().toDouble
    val fitArr = fitData.select(('Salnty - 'prediction).as[Double]).collect().map(a => Math.abs(a)).sum / lr4Size
    println("4. Using linear regression, make a prediction of salinity based only on temperature for the bottles data. What is the average error in your predictions?")
    println(fitArr + " average error")

    //5. Now do a linear regression that also includes Depth and O2ml_L. What is the average error for this?
    val dataWithTempDepth = bottles.select('T_degC, 'Salnty, 'Depthm, 'O2ml_L).na.drop()
    val dataVA5 = new VectorAssembler().setInputCols(Array("T_degC", "Depthm", "O2ml_L")).setOutputCol("tempFeatures")
    val dataWithTempFeature2 = dataVA5.transform(dataWithTempDepth)
    val lr5 = new LinearRegression()
            .setFeaturesCol("tempFeatures")
            .setLabelCol("Salnty")

    val lrModel2 = lr5.fit(dataWithTempFeature2)
    val fitData2 = lrModel2.transform(dataWithTempFeature2)
    val lr5Size = fitData2.count().toDouble
    val fitArr2 = fitData2.select(('Salnty - 'prediction).as[Double]).collect().map(a => Math.abs(a)).sum / lr5Size
    println("5. Using linear regression that includes Depth and O2ml_L")
    println(fitArr2 + " average error")
    
    //6. Using any regression algorithm you want in SparkML, make a prediction of O2ml_L from other columns other than O2Sat and O2Satq. 
    //If you restrict yourself to a 3-D input, what is the best prediction you can make? What method and set of input columns produces it?
    
    val namesSubset = Set("Depthm", "T_degC", "ChlorA", "LightP", "STheta", "ChlorA", "DarkAs", "DarkAp", "DarkAq").subsets(3).map(_.toArray).toArray
    var arrs = Seq[(Double, Seq[String])]() //return of the error and the strings that made it

    (0 to namesSubset.length-1).toArray.map(pos =>
        arrs :+= doThings(namesSubset(pos))
    )

    def doThings(seqs : Array[String]) : (Double, Seq[String]) = {
        val s1 = seqs(0)
        val s2 = seqs(1)
        val s3 = seqs(2)

        val dataWithO2_1 = bottles.select(s1, s2, s3, "O2ml_L").na.drop()
        val dataVA6_1 = new VectorAssembler().setInputCols(Array(s1, s2, s3)).setOutputCol("temp")
        val dataWithTemp6_1 = dataVA6_1.transform(dataWithO2_1)
    
        val lr6_1 = new LinearRegression()
            .setFeaturesCol("temp")
            .setLabelCol("O2ml_L")
    
        val lr6_1Model = lr6_1.fit(dataWithTemp6_1)
        val fitData6_1 = lr6_1Model.transform(dataWithTemp6_1)
        val lr6Size1 = fitData6_1.count().toDouble
        val fitDouble = fitData6_1.select(('O2ml_L - 'prediction).as[Double]).collect().map(a => Math.abs(a)).sum / lr6Size1
        (fitDouble, seqs)
    }

    val minArr = arrs.minBy(_._1)
    minArr._2.foreach(println)
    println("error: " + minArr._1)
    

    //7. Make a plot that demonstrates what you found in #5.                                        
    val predictedSal = fitData2.select('prediction.as[Double]).collect()
    val actualSal = bottles.select('Salnty).na.drop().select('Salnty.as[Double]).collect()

    val predicSz = (0 to predictedSal.length).toArray
    val actualSz = (0 to actualSal.length).toArray
    
    /*
    println(predictedSal.length)
    println(actualSal.length)

    predictedSal.take(5).foreach(println)
    actualSal.take(5).foreach(println)
    */
    
    val plot2 = Plot.simple(
        ScatterStyle(predicSz, predictedSal, symbolWidth = 5, symbolHeight = 5, colors = BlueARGB),
        "Predicted Salinity", "Data point", "Salinity"
    )

    val plot3 = Plot.simple(
        ScatterStyle(actualSz, actualSal, symbolWidth = 5, symbolHeight = 5, colors = RedARGB),
        "Actual Salinity", "Data point", "Salinity"
    )

    /*

    
    val scatterArray = Array(
        ScatterStyle(predicSz, predictedSal, symbolWidth = 5, symbolHeight = 5, colors = BlueARGB),
        ScatterStyle(actualSz, actualSal, symbolWidth = 5, symbolHeight = 5, colors = RedARGB)
    )

    val plot2 = Plot.stacked(
        scatterArray, "Salinity Prediction vs Actual Salinity", "Data Point", "Salinity"
    )
    */
    SwingRenderer(plot2, 800, 800, true)
    SwingRenderer(plot3, 800, 800, true)
    
    
    spark.sparkContext.stop()
}
