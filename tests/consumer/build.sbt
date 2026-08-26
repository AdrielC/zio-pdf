ThisBuild / scalaVersion := "3.8.4"

libraryDependencies +=
  "io.github.adrielc" %% "zio-pdf" % sys.props.getOrElse("zio.pdf.version", sys.error("zio.pdf.version is required"))
