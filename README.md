
# Veikkaus dcontext

dcontext is a tool that can offer rapid iteration cycle with huge amounts of data and/or infrastructure. For example: dcontext can be used together with spark in order to keep the Spark infrastructure and data in the JVM's memory and the cluster's memory, while the code is modified and tested in rapid iteration cycle.

# Example use:

## Preparations


The 'examples' folder contains a project named fast-spark-test, which uses dcontext and Spark for analyzing moderately sized stock market data sets.

You can run this project by publishing testtoys from GitHub locally. Then running 'sbt publishLocal' local in both dcontext and ports/dtesttoys projects. Then you need to run ./get_data.sh and 'sbt testsh' in the fast-spark-test project folder.

## Running dcontext console

'sbt testsh' launches the dcontext console with the example test suite and an example test case called spyDf. Executing the spyDf produces following results

```
$ test spyDf

  ...lots of Spark log..

  columns:
    symbol
    date
    time
    open
    high
    low
    close
    volume
  
  rows: 1128951

5029 ms. 
```

Running the test case takes several seconds, because the task also sets up the SparkContext and loads and parses about 50MB of data.

## Modifying dynamically reloaded code

Now, let's launch a new terminal, and run 'sbt ~;package;test:package' on it, in order to compile fast-spark-test sources on the background.

Now, let's examine fast-spark-test sources codes. The 'spyDf' test is located in the ExampleTest.scala file and it looks like this:

```
test("spyDf")((c, t) => {
 val df : DataFrame = spyDf(c)
 val count = df.count
 t.tln("columns:")
 df.columns.foreach { c => t.tln("  " + c) }
 t.tln
 t.tln("rows: " + count)
})
```

Let's test how dynamic class loading works, by adding three new lines for printing SPY dataframe contents:

```
test("spyDf")((c, t) => {
 val df : DataFrame = spyDf(c)
 val count = df.count
 t.tln("columns:")
 df.columns.foreach { c => t.tln("  " + c) }
 t.tln
 t.tln("rows: " + count)
 t.tln // new
 t.tln("the data frame:") // new
 tDf(t, df) // prints first lines of the data frame
})
```

Then let's wait for a second for the sbt compilation process to finish on the background. After compilation is finished, we can relaunch the 'spyDf' test:

```
$ test spyDf
  ...lots of Spark log..
  columns:
    symbol
    date
    time
    open
    high
    low
    close
    volume
  
  rows: 1128951

! the data frame:
! 1128951 entries
! 8 columns

! symbol        |date          |time          |open          |high          |low           |close         |volume        
…lots of Spark log..
! SPY           |20040701      |0931          |114.25        |114.32        |114.24        |114.27        |216400.0      
! SPY           |20040701      |0932          |114.26        |114.33        |114.24        |114.31        |207200.0      
! SPY           |20040701      |0933          |114.3         |114.34        |114.28        |114.3         |83900.0       
! SPY           |20040701      |0934          |114.3         |114.32        |114.29        |114.32        |245500.0      
! SPY           |20040701      |0935          |114.29        |114.31        |114.29        |114.3         |69400.0       
! SPY           |20040701      |0936          |114.31        |114.34        |114.31        |114.32        |218200.0      
! SPY           |20040701      |0937          |114.33        |114.36        |114.32        |114.34        |59600.0       
! SPY           |20040701      |0938          |114.34        |114.34        |114.26        |114.28        |143300.0      
! ...

187 ms. 15 errors! [d]iff, [c]ontinue or [f]reeze?
```

The testtoys behavioral test suite notices, that the test case results have changed, according to our modications. Even more, the test was faster to run, because the SparkContext and the SPY dataframe remained loaded in the JVM's heap.

The Spark infrastructure and the various data frames can actually be seen by running '-l' in the dcontext console:

```
$ -l
fastsparktest.sparkContext     org.apache.spark.SparkContext
fastsparktest.spy              org.apache.spark.rdd.MapPartitionsRDD
fastsparktest.spyDf            org.apache.spark.sql.DataFrame
fastsparktest.sqlContext       org.apache.spark.sql.SQLContext
test                           fi.futurice.fastsparktest.ExampleTest
```
