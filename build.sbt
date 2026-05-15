val zioVersion                 = "2.1.25"
val zioPreludeVersion          = "1.0.0-RC47"
val zioBlocksVersion           = "0.0.38"
val scodecCoreVersion          = "2.3.3"
val scodecBitsVersion          = "1.2.4"
val kyoVersion                 = "1.0-RC1"
val pdfBoxVersion              = "3.0.7"

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
      "dev.zio"   %% "zio-blocks-schema"     % zioBlocksVersion,
      "dev.zio"   %% "zio-blocks-mediatype"  % zioBlocksVersion,
      "dev.zio"   %% "zio-blocks-ringbuffer" % zioBlocksVersion,
      "dev.zio"   %% "zio-blocks-streams"    % zioBlocksVersion,
      "dev.zio"   %% "zio-blocks-scope"      % zioBlocksVersion,
      "org.scodec" %% "scodec-core"          % scodecCoreVersion,
      "org.scodec" %% "scodec-bits"          % scodecBitsVersion,
      "io.getkyo" %% "kyo-data"          % kyoVersion,
      "io.getkyo" %% "kyo-kernel"        % kyoVersion,
      "io.getkyo" %% "kyo-prelude"       % kyoVersion,
      "io.getkyo" %% "kyo-core"          % kyoVersion,
      "io.getkyo" %% "kyo-zio"           % kyoVersion,
      "org.apache.pdfbox" % "pdfbox"     % pdfBoxVersion % Test,
      "dev.zio"   %% "zio-test"          % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt"      % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/**
 * JMH benchmark subproject for the ZIO-based codec stack. Run with:
 *
 *   sbt 'bench/Jmh/run -i 5 -wi 3 -f 1 -t 1'
 *
 * (-i = measurement iterations, -wi = warmup iterations,
 *  -f = forks, -t = threads).
 */
lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root)
  .settings(
    name              := "zio-pdf-bench",
    publish / skip    := true,
    Jmh / version     := "1.37",
    scalacOptions := (root / scalacOptions).value.filterNot(_.startsWith("-Wunused"))
  )

/**
 * Head-to-head benches against fs2 + the (folded-into-fs2)
 * scodec-stream interop. Lives in its own subproject so fs2 +
 * cats-effect and their transitive cloud of types never touch the
 * main project. This is the apples-to-apples comparison: same
 * scodec.Decoder fed to both libraries, decoding the same in-memory
 * byte stream, throughput in MB/s.
 *
 * Run with:
 *
 *   sbt 'benchFs2/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu ms'
 */
lazy val benchFs2 = (project in file("bench-fs2"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root)
  .settings(
    name              := "zio-pdf-bench-fs2",
    publish / skip    := true,
    Jmh / version     := "1.37",
    scalacOptions := (root / scalacOptions).value.filterNot(_.startsWith("-Wunused")),
    libraryDependencies ++= List(
      "co.fs2"         %% "fs2-core"    % "3.13.0",
      "co.fs2"         %% "fs2-io"      % "3.13.0",
      "co.fs2"         %% "fs2-scodec"  % "3.13.0",
      "org.typelevel"  %% "cats-effect" % "3.7.0"
    )
  )
