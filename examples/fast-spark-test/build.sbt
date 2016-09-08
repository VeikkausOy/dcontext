
import AssemblyKeys._
import TestWithSparkKeys._

val scalaMajorVersion = "2.10"
val scalaMinorVersion = "6"
val sparkVersion = "1.6.0"

scalaVersion := f"${scalaMajorVersion}.${scalaMinorVersion}"
scalacOptions += "-deprecation"
scalacOptions += "-feature"

net.virtualvoid.sbt.graph.Plugin.graphSettings

// Merge strategy shared between app & test

val sharedMergeStrategy: (String => MergeStrategy) => String => MergeStrategy =
  old => {
    case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    case x if x.endsWith("plugin.properties") => MergeStrategy.last
    case x => old(x)
  }

// Load Assembly Settings

assemblySettings

// Assembly App

mainClass in assembly := Some("com.github.ezhulenev.spark.RunSparkApp")

jarName in assembly := "spark-testing-example-app.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly)(sharedMergeStrategy)

// Assembly Tests

Project.inConfig(Test)(assemblySettings)

jarName in (Test, assembly) := "spark-testing-example-tests.jar"

mergeStrategy in (Test, assembly) <<= (mergeStrategy in assembly)(sharedMergeStrategy)

test in (Test, assembly) := {} // disable tests in assembly

// Run tests in Standalone Spark

Project.inConfig(Test)(testWithSparkSettings)

// Resolvers

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "Scalafi Bintray Repo" at "http://dl.bintray.com/ezhulenev/releases"

// Library Dependencies

libraryDependencies ++= Seq(
  "org.slf4j"          % "slf4j-api"       % "1.7.7",
  "ch.qos.logback"     % "logback-classic" % "1.1.2",
  //"com.scalafi"       %% "scala-openbook"  % "0.0.4",
  "org.apache.spark" % f"spark-mllib_${scalaMajorVersion}" % f"${sparkVersion}",
  "org.apache.spark" % f"spark-streaming_${scalaMajorVersion}" % f"${sparkVersion}" % "provided",
  "com.bionicspirit" % f"shade_${scalaMajorVersion}" % "1.7.2",
  "com.datastax.spark" % f"spark-cassandra-connector_${scalaMajorVersion}" % "1.5.0-RC1",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.2",
  "org.apache.spark" % f"spark-streaming-kafka_${scalaMajorVersion}" % f"${sparkVersion}",
  "org.apache.spark" % f"spark-sql_${scalaMajorVersion}" % f"${sparkVersion}",
  "org.apache.spark"  %% "spark-core"      % f"${sparkVersion}" /* excludeAll(
    ExclusionRule("commons-beanutils", "commons-beanutils-core"),
    ExclusionRule("commons-collections", "commons-collections"),
    ExclusionRule("commons-logging", "commons-logging"),
    ExclusionRule("org.slf4j", "slf4j-log4j12"),
    ExclusionRule("org.hamcrest", "hamcrest-core"),
    ExclusionRule("junit", "junit"),
    ExclusionRule("org.jboss.netty", "netty"),
    ExclusionRule("com.esotericsoftware.minlog", "minlog")
    )*/
)

// Test Dependencies

libraryDependencies ++= Seq(
  "com.futurice" %% "testtoys" % "0.1-SNAPSHOT" % "test",
  "fi.veikkaus" %% "dcontext" % "0.2-SNAPSHOT"
)

lazy val testsh = taskKey[Unit]("interactive shell for running tasks in a JVM instance, while code may be modified")

lazy val root =
  (project in file(".")).
  settings(
   name := "fast-spark-test",
   organization := "fi.futurice",
   version := "0.0.1",
   testsh := {
     // Get the SBT test configuration class path
     val classpath =
       (fullClasspath in Test)
         .value
         .map(i=>i.data)

     // Separate SBT class path into A) static and B) dynamic parts
     //  - Dynamic paths are paths, that contain following string
     val dynamicPathFilter = (path:File) => {
       val p = path.getPath
       p.contains(f"target/scala-${scalaMajorVersion}")
     }
     val staticCP = classpath.filter(!dynamicPathFilter(_))
     val dynamicCP = classpath.filter(dynamicPathFilter)

     // The console arguments contains
     //   1) dynamic class path
     //   2) mounted run context (containing tasks or unit tests)
     //   3) command for starting the console in interactive mode
     val args = Seq(
       f"-p ${dynamicCP.mkString(":")}",
       "-m fi.futurice.fastsparktest.TestContext",
       "-i")

     val mainClass = "fi.veikkaus.dcontext.Console"
     (runner in run).value.run(mainClass, staticCP, args, streams.value.log)
   }
 )

