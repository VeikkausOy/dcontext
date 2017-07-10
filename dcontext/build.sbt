//
// dcontext
//
// provides a data context and dynamic class loader for code
//

scalaVersion := "2.12.2"

crossScalaVersions := Seq("2.10.6", "2.11.7")

scalaVersion := "2.12.2"
val scalaMajorVersion = "2.12"

version := "0.2-SNAPSHOT"

libraryDependencies ++=
  Seq("jline" % "jline" % "2.12.1",
      "org.scala-lang" % "scala-reflect" % "2.12.2",
      "org.slf4j" % "slf4j-api" % "1.7.21",
      "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
      "com.futurice" %% "testtoys" % "0.2" % "test")

lazy val testsh =
  taskKey[Unit]("interactive shell for running tasks in a JVM instance, while code may be modified")

lazy val dcontext = (project in file(".")).
  settings(
    name := "dcontext",
    organization := "fi.veikkaus",
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
        p.contains(f"target/scala-${scalaMajorVersion}/test-classes")
      }
      val staticCP = classpath.filter(!dynamicPathFilter(_))
      val dynamicCP = classpath.filter(dynamicPathFilter)

      // The console arguments contains
      //   1) dynamic class path
      //   2) mounted run context (containing tasks or unit tests)
      //   3) command for starting the console in interactive mode
      val args = Seq(
        f"-p ${dynamicCP.mkString(":")}",
        "-m fi.veikkaus.dcontext.TestContext",
        "-i")

      // Start the DContext console. Give static class paths as arguments for JVM
      val mainClass = "fi.veikkaus.dcontext.Console"
      (runner in run).value.run(mainClass, staticCP, args, streams.value.log)
    }
  )
