import org.scalajs.linker.interface.ModuleKind
import org.scalajs.sbtplugin.ScalaJSPlugin

val zioVersion                 = "2.1.26"
val zioPreludeVersion          = "1.0.0-RC47"
val zioBlocksSchemaVersion     = "0.0.51"
val zioBlocksMediaTypeVersion  = "0.0.51"
val zioBlocksRingbufferVersion = "0.0.51"
val zioBlocksChunkVersion      = "0.0.51"
val zioBlocksStreamsVersion    = "0.0.51"
val scodecCoreVersion          = "2.3.3"
val scodecBitsVersion          = "1.2.5"
val kyoVersion                 = "1.0.0-RC4"
val scalaJsDomVersion          = "2.8.1"
val scalaJavaTimeVersion       = "2.7.0"

ThisBuild / organization      := "io.github.adrielc"
ThisBuild / scalaVersion      := "3.8.4"
ThisBuild / description       := "Streaming PDF parsing and structural boundary scanning for ZIO and Scala 3"
ThisBuild / versionScheme     := Some("early-semver")
ThisBuild / fork              := true
ThisBuild / Test / fork               := false
ThisBuild / Test / parallelExecution  := false
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / Test / publishArtifact := false
ThisBuild / licenses          := List(
  "Apache-2.0" -> uri("https://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage          := Some(uri("https://github.com/AdrielC/zio-pdf"))
ThisBuild / scmInfo           := Some(
  ScmInfo(
    uri("https://github.com/AdrielC/zio-pdf"),
    "scm:git:https://github.com/AdrielC/zio-pdf.git",
    Some("scm:git:git@github.com:AdrielC/zio-pdf.git")
  )
)
ThisBuild / autoAPIMappings   := true
ThisBuild / developers        := List(
  Developer(
    "AdrielC",
    "Adriel Casellas",
    "adrielcasellas@gmail.com",
    uri("https://github.com/AdrielC")
  )
)

ThisBuild / scalacOptions ++= List(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-language:higherKinds",
  "-Wunused:imports",
  "-Wunused:locals",
  "-Wunused:privates",
  "-Wunused:explicits",
  "-Wvalue-discard",
  "-Werror"
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
      "dev.zio"   %% "zio-blocks-chunk"      % zioBlocksChunkVersion,
      "dev.zio"   %% "zio-blocks-streams"    % zioBlocksStreamsVersion,
      "dev.zio"   %% "zio-blocks-ringbuffer" % zioBlocksRingbufferVersion,
      "dev.zio"   %% "zio-blocks-scope"      % zioBlocksSchemaVersion,
      "dev.zio"   %% "zio-blocks-context"    % zioBlocksSchemaVersion,
      "dev.zio"   %% "zio-blocks-config"     % zioBlocksSchemaVersion,
      "org.scodec" %% "scodec-core"          % scodecCoreVersion,
      "org.scodec" %% "scodec-bits"          % scodecBitsVersion,
      "org.apache.pdfbox" % "pdfbox" % "3.0.4" % Test,
      "dev.zio"   %% "zio-test"          % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt"      % zioVersion % Test
    ),
    // ZIOSpecDefault creates a generated main for every spec. Choosing a
    // representative entry point keeps sbt 2 from warning while `test` still
    // delegates discovery to ZIO Test.
    Test / mainClass := Some("zio.pdf.PdfEngineSpec"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

private val jsExcludedSourcePaths = Set(
  "zio/blocks/pure/Env.scala",
  "zio/blocks/pure/Pure.scala",
  "zio/blocks/pure/Stack.scala",
  "zio/blocks/pure/Support.scala",
  "zio/pdf/EvidenceDigestPlatform.scala",
  "zio/pdf/FilterEncode.scala",
  "zio/pdf/FlateDecode.scala",
  "zio/pdf/HyperdriveStream.scala",
  "zio/pdf/PdfEngine.scala",
  "zio/pdf/PdfHyperdrive.scala",
  "zio/pdf/Tiff.scala",
  "zio/pdf/io/PdfIO.scala",
  "zio/pdf/pipe/ByteDigest.scala",
  "zio/pdf/pipe/ByteFeed.scala",
  "zio/pdf/pipe/DecodePipeline.scala",
  "zio/pdf/pipe/FusedDecode.scala",
  "zio/pdf/pipe/FusedElements.scala",
  "zio/pdf/pipe/HyperFuse.scala",
  "zio/pdf/pipe/IngestPipeline.scala",
  "zio/scan/BlocksPureByteScan.scala",
  "zio/scan/BytePipeline.scala",
  "zio/scan/InlineByteScan.scala",
  "zio/scodec/schema/ScodecDeriver.scala"
)

private def jsSharedSources(base: File): Seq[File] = {
  val sourceRoot = base / "src" / "main" / "scala"
  (sourceRoot ** "*.scala").get().filter { source =>
    IO.relativize(sourceRoot, source).forall(path => !jsExcludedSourcePaths.contains(path))
  }
}

/**
 * Browser / Node.js artifact. Shared parser code continues to live in the
 * primary source tree; JVM-only I/O, mmap, and crypto implementations are
 * replaced by JS-specific sources under `js/src`.
 */
lazy val scalaJs = (project in file("js"))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    name := "zio-pdf",
    libraryDependencies ++= List(
      "dev.zio"      % "zio_sjs1_3"                   % zioVersion,
      "dev.zio"      % "zio-streams_sjs1_3"           % zioVersion,
      "dev.zio"      % "zio-prelude_sjs1_3"           % zioPreludeVersion,
      "dev.zio"      % "zio-blocks-schema_sjs1_3"     % zioBlocksSchemaVersion,
      "dev.zio"      % "zio-blocks-mediatype_sjs1_3"  % zioBlocksMediaTypeVersion,
      "dev.zio"      % "zio-blocks-chunk_sjs1_3"      % zioBlocksChunkVersion,
      "dev.zio"      % "zio-blocks-streams_sjs1_3"    % zioBlocksStreamsVersion,
      "dev.zio"      % "zio-blocks-ringbuffer_sjs1_3" % zioBlocksRingbufferVersion,
      "dev.zio"      % "zio-blocks-scope_sjs1_3"      % zioBlocksSchemaVersion,
      "dev.zio"      % "zio-blocks-context_sjs1_3"    % zioBlocksSchemaVersion,
      "dev.zio"      % "zio-blocks-config_sjs1_3"     % zioBlocksSchemaVersion,
      "org.scodec"   % "scodec-core_sjs1_3"           % scodecCoreVersion,
      "org.scodec"   % "scodec-bits_sjs1_3"           % scodecBitsVersion,
      "org.scala-js" % "scalajs-dom_sjs1_3"           % scalaJsDomVersion,
      // Required by ZIO's browser runtime once the Scala.js export is linked
      // into an application rather than a test-only bundle.
      "io.github.cquiroz" % "scala-java-time_sjs1_3" % scalaJavaTimeVersion,
      "dev.zio"      % "zio-test_sjs1_3"              % zioVersion % Test,
      "dev.zio"      % "zio-test-sbt_sjs1_3"          % zioVersion % Test
    ),
    Compile / unmanagedSources := {
      val repo = (LocalRootProject / baseDirectory).value
      jsSharedSources(repo) ++ ((baseDirectory.value / "src" / "main" / "scala") ** "*.scala").get()
    },
    Test / unmanagedSources := ((baseDirectory.value / "src" / "test" / "scala") ** "*.scala").get(),
    Test / unmanagedResourceDirectories +=
      (LocalRootProject / Test / resourceDirectory).value,
    Test / fork := false,
    scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.ESModule)),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Scala.js export bridge consumed by the Vite browser workspace. */
lazy val scalaJsFrontend = (project in file("examples-js"))
  .enablePlugins(ScalaJSPlugin)
  .dependsOn(scalaJs)
  .settings(
    name := "zio-pdf-scalajs-frontend",
    publish / skip := true,
    scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.ESModule)),
    Compile / fastLinkJS / scalaJSLinkerOutputDirectory :=
      baseDirectory.value / "frontend" / "src" / "generated",
    Compile / fullLinkJS / scalaJSLinkerOutputDirectory :=
      baseDirectory.value / "frontend" / "src" / "generated"
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
      "io.getkyo" %% "kyo-parse"   % kyoVersion % Test,
      "dev.zio"   %% "zio-test"    % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt" % zioVersion % Test
    ),
    Test / mainClass := Some("zio.pdf.scan.ScanSpec"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root, scanKyo)
  .settings(
    name              := "zio-pdf-bench",
    publish / skip    := true,
    Jmh / version     := "1.37",
    libraryDependencies ++= List(
      "dev.zio" %% "zio-blocks-ringbuffer" % zioBlocksRingbufferVersion,
      "dev.zio" %% "zio-test"              % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt"          % zioVersion % Test
    ),
    Test / fork := false,
    Test / mainClass := Some("zio.pdf.bench.scan.BytePipelineSpec"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Inline expansion of `zio.pdf.pipe` in forked JMH runs can fail class loading; keep fuse in PdfHyperdrive.
    Jmh / fork        := false,
    // Scala 3.8.4 JVM optimizer, scoped to sources and project packages.
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
    libraryDependencies += "org.apache.pdfbox" % "pdfbox" % "3.0.4",
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
