ThisBuild / organization := "com.cdp"
ThisBuild / scalaVersion := "2.12.19"
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "cdp-lakehouse-platform",
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark"   %% "spark-core"                  % "3.5.3",
      "org.apache.spark"   %% "spark-sql"                   % "3.5.3",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.5"   % "1.6.1",
      "org.scalatest"      %% "scalatest"                   % "3.2.19" % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2048M",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )

