package fi.futurice.fastsparktest

import java.io.{Closeable, File}
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql._
import fi.veikkaus.dcontext.Contextual
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import fi.futurice.fastsparktest.DataContext._

class ExampleTest extends TestSuite("example") {

  test("config")((c, t) => {
    t.tln("the files are:")
    t.tln("  " + spyFile +  ", exists:" + spyFile.exists())
    t.tln("  " + djiaFile +  ", exists:" + spyFile.exists())
    t.tln("  " + spxFile +  ", exists:" + spyFile.exists())
  })

  test("setup")((c, t) => {
    t.t("getting the data...")
    val (y, d, x) = iMsLn(t, (spy(c), djia(c), spx(c)))
    t.tln()
    t.tln("spy  first entry " + y.take(2)(1))
    t.tln("djia first entry " + d.take(2)(1))
    t.tln("spx  first entry " + x.take(2)(1))
  })

  test("spy")((c, t) => {
    t.tln("labels:")
    t.tln("  " + spy(c).take(1)(0))
    t.tln
    t.tln("first 8 entries:")
    spy(c).take(8).tail.foreach { s =>
      t.tln("  " + s)
    }
  })

  test("djia")((c, t) => {
    t.tln("labels:")
    t.tln("  " + djia(c).take(1)(0))
    t.tln

    t.tln("first 8 entries:")
    djia(c).take(8).tail.foreach { s =>
      t.tln("  " + s)
    }
  })


  test("spyDf")((c, t) => {
    val df : DataFrame = spyDf(c)
    val count = df.count
    t.tln("columns:")
    df.columns.foreach { c => t.tln("  " + c) }
    t.tln
    t.tln("rows: " + count)
    t.tln // new
    t.tln("the data frame:") // new
    tDf(t, df) // prints the data frame
  })


}