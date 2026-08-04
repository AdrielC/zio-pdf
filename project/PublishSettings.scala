import sbt.*
import sbt.Keys.*

/** Tybera Gitea Package Registry — internal Maven only (no Sonatype / public mirror). */
object PublishSettings {

  private def gitServer   = sys.env.getOrElse("GIT_SERVER", "git.tybera.net")
  private def gitProtocol = sys.env.getOrElse("GIT_PROTOCOL", "https")
  private def gitHost     = gitServer.split(":").head

  private def credUser =
    sys.env.getOrElse("MAVEN_USER", sys.env.getOrElse("CI_USER", sys.env.getOrElse("GIT_USER", "")))

  private def credToken =
    sys.env.getOrElse("MAVEN_TOKEN", sys.env.getOrElse("CI_TOKEN", sys.env.getOrElse("GIT_TOKEN", "")))

  val settings: Seq[Def.Setting[?]] = Seq(
    publishMavenStyle    := true,
    publishArtifact      := true,
    pomIncludeRepository := { _ => false },
    Test / publishArtifact := false,

    publishTo := {
      val mavenPath = "/api/packages/Tybera/maven"
      val isStable  = !version.value.contains("SNAPSHOT")
      val repoName  = if (isStable) "gitea-releases" else "gitea-snapshots"
      val url       = s"$gitProtocol://$gitServer$mavenPath"
      Some(repoName at url withAllowInsecureProtocol (gitProtocol == "http"))
    },

    credentials ++= {
      if (credUser.isEmpty) Nil
      else
        Credentials(
          realm = "Gitea Package API",
          host = gitHost,
          userName = credUser,
          passwd = credToken
        ) :: Nil
    },

    publish := {
      if (credUser.isEmpty)
        throw new MessageOnlyException(
          "Publishing requires GIT_USER/GIT_TOKEN (or MAVEN_USER/MAVEN_TOKEN) in the environment."
        )
      publish.value
    }
  )

  /** Resolver for other Tybera projects consuming this artifact. */
  val tyberaResolver: Resolver =
    "Tybera Gitea Maven" at s"$gitProtocol://$gitServer/api/packages/Tybera/maven"
}
