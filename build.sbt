val zioVersion             = "2.1.25"
val zioPreludeVersion      = "1.0.0-RC47"
val zioBlocksSchemaVersion = "0.0.33"
val scodecCoreVersion      = "2.3.3"
val scodecBitsVersion      = "1.2.4"

ThisBuild / organization      := "com.springernature"
ThisBuild / scalaVersion      := "3.8.3"
ThisBuild / version           := "0.2.0-SNAPSHOT"
ThisBuild / fork              := true
ThisBuild / licenses          := List(
  "Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage          := Some(url("https://github.com/springernature/fs2-pdf"))
ThisBuild / autoAPIMappings   := true

ThisBuild / scalacOptions ++= List(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-language:higherKinds",
  "-Wunused:imports",
  "-Wunused:locals",
  "-Wunused:privates",
  "-Wunused:explicits",
  "-Wvalue-discard"
)

lazy val root = (project in file("."))
  .settings(
    name := "zio-pdf",
    libraryDependencies ++= List(
      "dev.zio"   %% "zio"               % zioVersion,
      "dev.zio"   %% "zio-streams"       % zioVersion,
      "dev.zio"   %% "zio-prelude"       % zioPreludeVersion,
      "dev.zio"   %% "zio-blocks-schema" % zioBlocksSchemaVersion,
      "org.scodec" %% "scodec-core"      % scodecCoreVersion,
      "org.scodec" %% "scodec-bits"      % scodecBitsVersion,
      "dev.zio"   %% "zio-test"          % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt"      % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
