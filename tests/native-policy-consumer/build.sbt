import scala.scalanative.sbtplugin.ScalaNativePlugin

lazy val root = (project in file("."))
  .enablePlugins(ScalaNativePlugin)
  .settings(
    scalaVersion := "3.8.4",
    Compile / unmanagedSources += baseDirectory.value.getParentFile / "policy-shared" / "PolicyConsumerChecks.scala",
    resolvers ++= sys.props.get("zio.pdf.local.repo").map(path => "native-policy-proof" at file(path).toURI.toString).toList,
    libraryDependencies += "io.github.adrielc" % "kyo-pdf-pipeline_native0.5_3" % sys.props("zio.pdf.version")
  )
