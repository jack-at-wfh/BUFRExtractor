// In your build.sbt
val Scala3 = "3.7.1" // UPDATED SCALA VERSION
val ZIOVersion = "2.1.20" // Make sure this still matches your ZIO version

lazy val myZioApp = project
  .in(file("."))
  .settings(
    name := "my-zio-app",
    scalaVersion := Scala3,
    coverageExcludedPackages := ".*\\$anon\\$.*",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % ZIOVersion,
      "dev.zio" %% "zio-streams" % ZIOVersion,
      "dev.zio" %% "zio-test"     % ZIOVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZIOVersion % Test,
      "dev.zio" %% "zio-logging" % "2.2.3" % Test,
      "dev.zio" %% "zio-logging-slf4j" % "2.2.3" % Test,
      "dev.zio" %% "zio-json" % "0.7.44",
      "dev.zio" %% "zio-nio" % "2.0.2",
      "ch.qos.logback" % "logback-classic" % "1.4.14" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZioTestFramework"),
  )
  