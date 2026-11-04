import org.scalajs.linker.interface.ModuleKind
import org.scalajs.sbtplugin.ScalaJSPlugin
import scala.scalanative.sbtplugin.ScalaNativePlugin

val zioVersion                 = "2.1.26"
val zioPreludeVersion          = "1.0.0-RC47"
val pipelineSchemaVersion      = "0.0.56"
val zioBlocksSchemaVersion     = "0.0.51"
val zioBlocksMediaTypeVersion  = "0.0.51"
val zioBlocksRingbufferVersion = "0.0.51"
val zioBlocksChunkVersion      = "0.0.51"
val zioBlocksStreamsVersion    = "0.0.51"
val scodecCoreVersion          = "2.3.3"
val scodecBitsVersion          = "1.2.5"
val scalaJsDomVersion          = "2.8.1"
val scalaJavaTimeVersion       = "2.7.0"
val kyoVersion                 = "1.0.0-RC4"

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

lazy val purgeStaleJmhGenerated = taskKey[Unit]("Remove cached JMH classes from deleted benchmarks")

/**
 * Vendored volga core (`modules/volga-core`) — FreeProp wiring diagrams, SMC
 * pipeline syntax macros, and symmetric-monoidal category kit for `zio.pdf.arrow`.
 * Forked from tofu-tf/volga with Scala 3.8 SMC parsing fixes; see module README.
 */
lazy val volgaCore = (project in file("modules/volga-core"))
  .settings(
    name           := "volga-core",
    publish / skip := true,
    Compile / scalaSource := baseDirectory.value / "src" / "main" / "scala-3",
    Test / scalaSource    := baseDirectory.value / "src" / "test" / "scala",
    // Upstream volga diag tests need PlantUML/GUI; zio-pdf covers SMC via ArrowSyntaxSpec.
    Test / sources := Seq.empty,
    scalacOptions := (ThisBuild / scalacOptions).value.filterNot(o => o == "-Werror" || o.startsWith("-Wunused")) ++ List(
      "-Xkind-projector:underscores",
      "-Yshow-suppressed-errors"
    ),
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0-M7" % Test
  )

private def kyoPdfArtifactSettings = Seq(
  Compile / packageDoc / publishArtifact := true,
  Compile / packageSrc / publishArtifact := true,
  Test / packageBin / publishArtifact := false,
  Test / packageSrc / publishArtifact := false,
  Test / packageDoc / publishArtifact := false
)

/** Kyo-native PDF core. Main sources intentionally have no ZIO dependency. */
lazy val kyoPdf = (project in file("kyo-pdf-core"))
  .settings(kyoPdfArtifactSettings)
  .settings(
    name := "kyo-pdf",
    libraryDependencies ++= List(
      "io.getkyo" %% "kyo-core"    % kyoVersion,
      "io.getkyo" %% "kyo-parse"   % kyoVersion,
      "io.getkyo" %% "kyo-schema"  % kyoVersion,
      "dev.zio"   %% "zio-test"     % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt" % zioVersion % Test
    ),
    Test / unmanagedResourceDirectories += (LocalRootProject / Test / resourceDirectory).value,
    Test / mainClass := Some("io.github.adrielc.kyopdf.PdfParserSpec"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Scala Native artifact: same core with an Expat-backed XMP reader. */
lazy val kyoPdfNative = (project in file("kyo-pdf-native"))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    name := "kyo-pdf",
    Compile / packageDoc / publishArtifact := true,
    Test / packageBin / publishArtifact := false,
    Test / packageSrc / publishArtifact := false,
    Test / packageDoc / publishArtifact := false,
    Compile / unmanagedSources ++= {
      val shared = (LocalRootProject / baseDirectory).value / "kyo-pdf-core" / "src" / "main" / "scala"
      (shared ** "*.scala").get().filterNot(_.getName == "XmpReader.scala")
    },
    Compile / packageSrc / mappings := {
      val repo = (LocalRootProject / baseDirectory).value
      val converter = fileConverter.value
      val roots = Seq(repo / "kyo-pdf-core" / "src" / "main" / "scala", baseDirectory.value / "src" / "main" / "scala")
      (Compile / unmanagedSources).value.map { source =>
        converter.toVirtualFile(source.toPath) -> roots.iterator.flatMap(root => IO.relativize(root, source)).next()
      }
    },
    Test / unmanagedSources +=
      (LocalRootProject / baseDirectory).value / "kyo-pdf-core" / "src" / "test" / "scala" /
        "io" / "github" / "adrielc" / "kyopdf" / "PdfAIdentificationSpec.scala",
    libraryDependencies ++= List(
      "io.getkyo" % "kyo-core_native0.5_3"    % kyoVersion,
      "io.getkyo" % "kyo-parse_native0.5_3"   % kyoVersion,
      "io.getkyo" % "kyo-schema_native0.5_3"  % kyoVersion,
      "dev.zio"   % "zio-test_native0.5_3"     % zioVersion % Test,
      "dev.zio"   % "zio-test-sbt_native0.5_3" % zioVersion % Test
    ),
    libraryDependencySchemes += "org.scala-native" % "test-interface_native0.5_3" % VersionScheme.EarlySemVer,
    Test / fork := false,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Optional compatibility edge for ZIO applications consuming the Kyo core. */
lazy val kyoPdfZio = (project in file("kyo-pdf-zio"))
  .dependsOn(kyoPdf)
  .settings(kyoPdfArtifactSettings)
  .settings(
    name := "kyo-pdf-zio",
    libraryDependencies ++= List(
      "io.getkyo" %% "kyo-zio"      % kyoVersion,
      "dev.zio"   %% "zio"          % zioVersion,
      "dev.zio"   %% "zio-streams"  % zioVersion,
      "dev.zio"   %% "zio-test"     % zioVersion % Test,
      "dev.zio"   %% "zio-test-sbt" % zioVersion % Test
    ),
    Test / mainClass := Some("io.github.adrielc.kyopdf.zio.PdfZIOSpec"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Data-defined PDF graphs; interpreters supply artwork and validation services. */
lazy val kyoPdfPipeline = (project in file("kyo-pdf-pipeline"))
  .dependsOn(kyoPdf)
  .settings(kyoPdfArtifactSettings)
  .settings(
    name := "kyo-pdf-pipeline",
    libraryDependencies ++= List(
      "dev.zio" %% "zio-blocks-schema" % pipelineSchemaVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Public-source Native backport of the ZIO Blocks runtime used by pipelines. */
private def blocksNativeLegalMappings = Def.task {
  val root = (LocalRootProject / baseDirectory).value / "modules" / "zio-blocks-native"
  val converter = fileConverter.value
  Seq("LICENSE", "NOTICE", "UPSTREAM.json").map(name =>
    converter.toVirtualFile((root / name).toPath) -> s"META-INF/zio-blocks-native/$name")
}

private def blocksNativeSettings(module: String) = Seq(
  name := s"kyo-pdf-blocks-$module",
  description := s"Public-source ZIO Blocks 0.0.56 $module runtime for Kyo PDF on Scala Native",
  Compile / unmanagedSourceDirectories := Seq("shared", "native").flatMap(platform =>
    Seq("scala", "scala-3").map(kind => baseDirectory.value / platform / "src" / "main" / kind)),
  Test / unmanagedSourceDirectories := Seq(baseDirectory.value / "shared" / "src" / "test" / "scala"),
  Compile / packageSrc / mappings := {
    val roots = (Compile / unmanagedSourceDirectories).value
    val converter = fileConverter.value
    (Compile / unmanagedSources).value.map(source =>
      converter.toVirtualFile(source.toPath) -> roots.iterator.flatMap(root => IO.relativize(root, source)).next())
  },
  Compile / packageBin / mappings ++= blocksNativeLegalMappings.value,
  Compile / packageSrc / mappings ++= blocksNativeLegalMappings.value,
  Compile / packageDoc / mappings ++= blocksNativeLegalMappings.value,
  scalacOptions := Seq("-deprecation", "-feature", "-unchecked"),
  Test / fork := false,
  libraryDependencies ++= List(
    "dev.zio" % "zio-test_native0.5_3" % zioVersion % Test,
    "dev.zio" % "zio-test-sbt_native0.5_3" % zioVersion % Test
  ),
  libraryDependencySchemes += "org.scala-native" % "test-interface_native0.5_3" % VersionScheme.EarlySemVer,
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val blocksChunkNative = (project in file("modules/zio-blocks-native/chunk"))
  .enablePlugins(ScalaNativePlugin).settings(kyoPdfArtifactSettings).settings(blocksNativeSettings("chunk"))
lazy val blocksTypeIdNative = (project in file("modules/zio-blocks-native/typeid"))
  .enablePlugins(ScalaNativePlugin).dependsOn(blocksChunkNative)
  .settings(kyoPdfArtifactSettings).settings(blocksNativeSettings("typeid"))
lazy val blocksMaybeNative = (project in file("modules/zio-blocks-native/maybe"))
  .enablePlugins(ScalaNativePlugin).dependsOn(blocksTypeIdNative)
  .settings(kyoPdfArtifactSettings).settings(blocksNativeSettings("maybe"))
lazy val blocksMarkdownNative = (project in file("modules/zio-blocks-native/markdown"))
  .enablePlugins(ScalaNativePlugin).dependsOn(blocksChunkNative)
  .settings(kyoPdfArtifactSettings).settings(blocksNativeSettings("markdown"))
lazy val blocksSchemaNative = (project in file("modules/zio-blocks-native/schema"))
  .enablePlugins(ScalaNativePlugin).dependsOn(blocksTypeIdNative, blocksChunkNative, blocksMarkdownNative, blocksMaybeNative)
  .settings(kyoPdfArtifactSettings).settings(blocksNativeSettings("schema"))
  .settings(libraryDependencies ++= List(
    "io.github.cquiroz" % "scala-java-time_native0.5_3" % scalaJavaTimeVersion,
    "io.github.cquiroz" % "scala-java-time-tzdb_native0.5_3" % scalaJavaTimeVersion
  ))

/** Native pipelines use only public-source, published dependencies. */
lazy val kyoPdfPipelineNative = (project in file("kyo-pdf-pipeline-native"))
  .enablePlugins(ScalaNativePlugin)
  .dependsOn(kyoPdfNative, blocksSchemaNative)
  .settings(kyoPdfArtifactSettings)
  .settings(
    name := "kyo-pdf-pipeline",
    Compile / scalaSource := (kyoPdfPipeline / Compile / scalaSource).value,
    Test / scalaSource := (kyoPdfPipeline / Test / scalaSource).value,
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "scala",
    libraryDependencies ++= List(
      "io.github.cquiroz" % "scala-java-time_native0.5_3" % scalaJavaTimeVersion,
      "dev.zio" % "zio-test_native0.5_3" % zioVersion % Test,
      "dev.zio" % "zio-test-sbt_native0.5_3" % zioVersion % Test
    ),
    libraryDependencySchemes += "org.scala-native" % "test-interface_native0.5_3" % VersionScheme.EarlySemVer,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

/** Loopback-only visual explorer; rendering stays outside the Kyo core. */
lazy val paperMachine = (project in file("examples/paper-machine"))
  .dependsOn(kyoPdfPipeline, volgaCore)
  .settings(
    name := "paper-machine",
    publish / skip := true,
    libraryDependencies ++= List(
      "org.apache.pdfbox" % "pdfbox" % "3.0.4",
      "org.verapdf" % "validation-model" % "1.30.3",
      "com.google.code.gson" % "gson" % "2.13.1",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Compile / mainClass := Some("io.github.adrielc.kyopdf.demo.PaperMachine")
  )

lazy val root = (project in file("."))
  .dependsOn(volgaCore % "compile-internal,runtime-internal,test-internal")
  .settings(
    name := "zio-pdf",
    // Keep macros in a separate compilation unit, then ship their existing
    // classes and sources with the JVM API instead of an unpublished dependency.
    Compile / packageBin / mappings ++= (volgaCore / Compile / packageBin / mappings).value,
    Compile / packageSrc / mappings ++= (volgaCore / Compile / packageSrc / mappings).value,
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
  // JVM Volga wiring and ingest adapters depend on compiler macros and the
  // JVM-only decode/digest pipelines excluded below.
  "zio/pdf/arrow/ArrowObjects.scala",
  "zio/pdf/arrow/BiConst.scala",
  "zio/pdf/arrow/BiFunctionK.scala",
  "zio/pdf/arrow/Category.scala",
  "zio/pdf/arrow/FnArrow.scala",
  "zio/pdf/arrow/FnArrowCat.scala",
  "zio/pdf/arrow/FreeArrow.scala",
  "zio/pdf/arrow/GraphRender.scala",
  "zio/pdf/arrow/GraphSummary.scala",
  "zio/pdf/arrow/IngestGraph.scala",
  "zio/pdf/arrow/LabeledFnArrowCat.scala",
  "zio/pdf/arrow/PipelineFlow.scala",
  "zio/pdf/arrow/PipelineGraph.scala",
  "zio/pdf/arrow/ScanGraph.scala",
  "zio/pdf/io/PdfIO.scala",
  "zio/pdf/pipe/ByteDigest.scala",
  "zio/pdf/pipe/ByteFeed.scala",
  "zio/pdf/pipe/DecodePipeline.scala",
  "zio/pdf/pipe/FusedDecode.scala",
  "zio/pdf/pipe/FusedElements.scala",
  "zio/pdf/pipe/HyperFuse.scala",
  "zio/pdf/pipe/IngestPipeline.scala",
  "zio/pdf/tacit/PdfFlow.scala",
  "zio/pdf/tacit/PdfPipeline.scala",
  "zio/pdf/tacit/PdfPipes.scala",
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

private def publicScalaJsSourceSettings = Seq(
  scalacOptions += s"-scalajs-mapSourceURI:${(LocalRootProject / baseDirectory).value.toURI}->https://raw.githubusercontent.com/AdrielC/zio-pdf/main/"
)

/**
 * Browser / Node.js artifact. Shared parser code continues to live in the
 * primary source tree; JVM-only I/O, mmap, and crypto implementations are
 * replaced by JS-specific sources under `js/src`.
 */
lazy val scalaJs = (project in file("js"))
  .enablePlugins(ScalaJSPlugin)
  .settings(publicScalaJsSourceSettings)
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
    Compile / packageSrc / mappings := {
      val repo = (LocalRootProject / baseDirectory).value
      val converter = fileConverter.value
      val roots = Seq(repo / "src" / "main" / "scala", baseDirectory.value / "src" / "main" / "scala")
      (Compile / unmanagedSources).value.map { source =>
        converter.toVirtualFile(source.toPath) -> roots.iterator.flatMap(root => IO.relativize(root, source)).next()
      }
    },
    Test / unmanagedSources := {
      val shared = (LocalRootProject / baseDirectory).value / "src" / "test" / "scala" / "zio" / "pdf"
      ((baseDirectory.value / "src" / "test" / "scala") ** "*.scala").get() ++
        Seq(shared / "XrefStreamSpec.scala", shared / "AsciiFilterBoundsSpec.scala")
    },
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
  .settings(publicScalaJsSourceSettings)
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
lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .dependsOn(root, kyoPdf)
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
    Jmh / fork := false,
    // Action cache can restore jmh_generated classes from deleted ScanBench sources.
    purgeStaleJmhGenerated := {
      val stale = (Compile / classDirectory).value / "zio" / "pdf" / "scan" / "bench" / "jmh_generated"
      if stale.exists then IO.delete(stale)
    },
    Jmh / run := (Jmh / run).dependsOn(purgeStaleJmhGenerated).evaluated,
    cleanFiles ++= {
      val stale = (Compile / classDirectory).value / "zio" / "pdf" / "scan" / "bench" / "jmh_generated"
      if stale.exists then Seq(stale) else Nil
    },
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
