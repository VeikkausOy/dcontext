scalaVersion := "2.12.2"

crossScalaVersions := Seq("2.10.6", "2.11.7")

connectInput      in Test := true
parallelExecution in Test := false

lazy val dtesttoys = (project in file(".")).
  settings(
    name := "dtesttoys",
    organization := "com.futurice",
    version := "0.1",
    libraryDependencies ++= Seq(
      "fi.veikkaus" %% "dcontext" % "0.2-SNAPSHOT",
      "com.futurice" %% "testtoys" % "0.2"
    )
)

