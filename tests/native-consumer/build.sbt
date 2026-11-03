import scala.scalanative.sbtplugin.ScalaNativePlugin

lazy val root = (project in file("."))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    scalaVersion := "3.8.4",
    resolvers += "native-proof" at file(sys.props("zio.pdf.local.repo")).toURI.toString,
    libraryDependencies += "io.github.adrielc" % "kyo-pdf_native0.5_3" % sys.props("zio.pdf.version")
  )
