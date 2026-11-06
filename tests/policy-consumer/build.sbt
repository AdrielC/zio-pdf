ThisBuild / scalaVersion := "3.8.4"
Compile / unmanagedSources += baseDirectory.value.getParentFile / "policy-shared" / "PolicyConsumerChecks.scala"
Compile / unmanagedSources += baseDirectory.value.getParentFile / "policy-shared" / "ScanConsumerChecks.scala"

resolvers ++= sys.props.get("zio.pdf.local.repo")
  .map(path => "policy-proof" at file(path).toURI.toString).toList

val pdfVersion = sys.props.getOrElse("zio.pdf.version", sys.error("zio.pdf.version is required"))
libraryDependencies ++= List(
  "io.github.adrielc" %% "kyo-pdf-pipeline" % pdfVersion,
  "io.github.adrielc" %% "kyo-pdf-zio" % pdfVersion,
  "io.github.adrielc" %% "kyo-pdf-fs2" % pdfVersion
)
