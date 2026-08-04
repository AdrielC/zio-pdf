val zioVersion                 = "2.1.25"
val zioPreludeVersion          = "1.0.0-RC47"
val zioBlocksSchemaVersion     = "0.0.33"
val zioBlocksMediaTypeVersion  = "0.0.33"
val zioBlocksRingbufferVersion = "0.0.32"
val zioBlocksStreamsVersion    = "0.0.20"
val zioBlocksChunkVersion      = "0.0.33"
val scodecCoreVersion          = "2.3.3"
val scodecBitsVersion          = "1.2.4"
val kyoVersion                 = "1.0-RC1"

ThisBuild / organization      := "com.springernature"
ThisBuild / scalaVersion      := "3.8.3"
ThisBuild / version           := "0.2.0-RC1"
ThisBuild / fork              := true
ThisBuild / licenses          := List(
  "Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage          := Some(url("https://git.tybera.net/Tybera/zio-pdf"))
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
      "dev.zio"   %% "zio-blocks-schema"     % zioBlocksSchemaVersion,
      "dev.zio"   %% "zio-blocks-mediatype"  % zioBlocksMediaTypeVersion,
      "dev.zio"   %% "zio-blocks-ringbuffer" % zioBlocksRingbufferVersion,
      "dev.zio"   %% "zio-blocks-streams"    % zioBlocksStreamsVersion,
      "dev.zio"   %% "zio-blocks-chunk"      % zioBlocksChunkVersion,
      "org.scodec" %% "scodec-core"          % scodecCoreVersion,
      "org.scodec" %% "scodec-bits"          % scodecBitsVersion,
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
/**
 * Kyo-based scan algebra (experimental). Not part of the ZIO-only core;
 * kept for benchmarks and migration reference. Run tests with `scanKyo/test`.
 */
lazy val scanKyo = (project in file("scan-kyo"))
  .dependsOn(root)
  .settings(
    name           := "zio-pdf-scan-kyo",
    publish / skip := true,
    libraryDependencies ++= List(
      "io.getkyo" %% "kyo-data"    % kyoVersion,
      "io.getkyo" %% "kyo-kernel"  % kyoVersion,
      "io.getkyo" %% "kyo-prelude" % kyoVersion,
      "io.getkyo" %% "kyo-core"    % kyoVersion,
      "io.getkyo" %% "kyo-zio"     % kyoVersion,
      "dev.zio"   %% "zio-test"    % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root, scanKyo)
  .settings(
    name              := "zio-pdf-bench",
    publish / skip    := true,
    Jmh / version     := "1.37",
    // Inline expansion of `zio.pdf.pipe` in forked JMH runs can fail class loading; keep fuse in PdfHyperdrive.
    Jmh / fork        := false,
    // Scala 3.8.3 JVM optimizer — scoped to sources + our packages (not JDK/deps).
    scalacOptions := (root / scalacOptions).value.filterNot(_.startsWith("-Wunused")) ++ List(
      "-opt",
      "-opt-inline:<sources>,zio.pdf.**,zio.scan.**,zio.scodec.**,zio.blocks.**",
      "-Wopt:at-inline-failed-summary"
    ),
    Compile / unmanagedResourceDirectories +=
      (LocalRootProject / Test / resourceDirectory).value
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
/**
 * Runnable examples. Start here if the repo feels scattered:
 *
 *   sbt examples/run
 */
lazy val examples = (project in file("examples"))
  .dependsOn(root)
  .settings(
    name           := "zio-pdf-examples",
    publish / skip := true,
    Compile / mainClass := Some("zio.pdf.examples.ReadAndDecode"),
    Compile / unmanagedResourceDirectories +=
      (LocalRootProject / Test / resourceDirectory).value
  )

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
