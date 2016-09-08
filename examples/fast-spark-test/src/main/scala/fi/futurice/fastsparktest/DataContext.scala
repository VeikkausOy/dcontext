package fi.futurice.fastsparktest

import java.io.File

import org.apache.spark.sql._
import fi.veikkaus.dcontext.Contextual
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class DataRow(symbol:String, date:String, time:String, open:Double,
                   high:Double, low:Double, close:Double, volume:Double)

object DataContext extends Contextual("fastsparktest") {

  def dataDir  = new File("io/in")
  def spyFile  = new File(dataDir, "SPY.csv")
  def djiaFile = new File(dataDir, "DJIA.csv")
  def spxFile  = new File(dataDir, "SPX.csv")

  val sparkContext = cvalc("sparkContext") { case c =>
    org.apache.log4j.PropertyConfigurator.configure("io/in/config.properties")

    val config =
      new SparkConf()
        .set("spark.driver.memory", "4g")
        .set("spark.executor.memory", "4g")
        .setMaster("local[4]")
        .setAppName("fast-spark-test")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN" )

    sc.addJar("target/scala-2.10/fast-spark-test_2.10-0.0.1-tests.jar")
    sc.addJar("target/scala-2.10/fast-spark-test_2.10-0.0.1.jar")

    sc
  }{ case sc => sc.stop }


  val sqlContext = cval("sqlContext") { case c =>
    new SQLContext(sparkContext(c))
  }
  val djia = cval("djia") { case c =>
    sparkContext(c).textFile(djiaFile.getPath)
  }
  val spx = cval("spx") { case c =>
    sparkContext(c).textFile(spxFile.getPath)
  }

  val spy = cval("spy") { case c =>
    sparkContext(c).textFile(spyFile.getPath)
  }
  val spyDf = cval("spyDf") { case c =>
    val sqlc = sqlContext(c)
    import sqlc.implicits._
    val first = spy(c).take(0)
    val rv =
      spy(c)
        .filter(!_.contains("Open"))
        .map { s =>
          val split = s.split(",")
          DataRow(split(0), split(1), split(2), split(3).toDouble,
            split(4).toDouble,split(5).toDouble, split(6).toDouble, split(7).toDouble)
        }.toDF()
    rv.cache
    rv
  }

}
