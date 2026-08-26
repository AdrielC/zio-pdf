ThisBuild / scalaVersion := "3.8.4"

resolvers ++= sys.props
  .get("zio.pdf.local.repo")
  .map(path => "zio-pdf-proof" at file(path).toURI.toString)
  .toList

libraryDependencies +=
  "io.github.adrielc" %% "zio-pdf" % sys.props.getOrElse("zio.pdf.version", sys.error("zio.pdf.version is required"))
