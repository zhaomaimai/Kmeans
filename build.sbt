ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.5"

lazy val root = (project in file("."))
  .settings(
    name := "Kmeans"

)
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.4"