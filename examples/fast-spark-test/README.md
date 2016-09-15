
# fast-spark-test

Fast-spark-test is an example application, that uses dcontext and testtoys
for examining stock index data in a fast iteration cycle. With dcontext,
the sources can be modified, while the SparkContext and the data is kept in
JVM heap.

# Dependencies

fast-spark-test depends on following libraries:

1. dcontext
2. dtesttoys
3. testtoys

In order to run dcontext, you need run 'sbt publish-local' on all these projects.
dcontext and dtesttoys are available in this repository. testtoys is available
in https://github.com/futurice/testtoys.

# Downloading the data

The test data is somewhat large (~ 150 MB), so it is not stored in the
dcontext git.

You can alternatively:

1. Run ./get_datas.h

2. Or download the test data here snd store the csv files in the folder ./io/in/

  https://googledrive.com/host/0B2YVEgbOqh5_dUQzbDg1NUxSbUU/fast-spark-test-data.zip

# Running test console

Start the DContext console with:

```
sbt testsh
```

To see available test cases, print

```
-l
```

This should create following output

```
$ test -l
config
setup
spy
djia
spyDf
```

# Modifying the code, while test console is running and data is kept in memory 

You can next run e.g. spyDf by typing

```
$ test spyDf
```

Now, open a separate console on the background and run

```
sbt ~test:package
```

If you modify the code, and wait for the project to compile, following test runs
will ran with you new modifications fast. DContext keeps SparkContext, SQLContext
and various dataframes in JVM heap, which makes development cycle faster.

You can see the loaded data and infrastructure by typing -l. This should produce
following output

$ -l
fastsparktest.sparkContext     org.apache.spark.SparkContext
fastsparktest.spy              org.apache.spark.rdd.MapPartitionsRDD
fastsparktest.spyDf            org.apache.spark.sql.DataFrame
fastsparktest.sqlContext       org.apache.spark.sql.SQLContext
test                           fi.futurice.fastsparktest.ExampleTest

