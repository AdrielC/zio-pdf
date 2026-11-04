ThisBuild / scalaVersion := "3.8.4"

resolvers ++= sys.props.get("zio.pdf.local.repo")
  .map(path => "policy-proof" at file(path).toURI.toString).toList

val pdfVersion = sys.props.getOrElse("zio.pdf.version", sys.error("zio.pdf.version is required"))
libraryDependencies ++= List(
  "io.github.adrielc" %% "kyo-pdf-pipeline" % pdfVersion,
  "io.github.adrielc" %% "kyo-pdf-zio" % pdfVersion
)
