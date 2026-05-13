/*
 * ZIO port of the legacy fs2.pdf test helper: load classpath PDF fixtures.
 */

package zio.pdf.testkit

import java.nio.file.{Files, Path, Paths}

import zio.*
import zio.stream.*

final case class JarError(message: String)

object Jar {

  def noResource(name: String): JarError =
    JarError(s"no jar resource found for `$name`")

  def resourcePath(name: String): ZIO[Any, JarError, Path] =
    ZIO
      .fromOption(Option(getClass.getClassLoader.getResource(name)))
      .orElseFail(noResource(name))
      .flatMap(url => ZIO.attempt(Paths.get(url.toURI)).mapError(e => JarError(e.getMessage)))

  def resourceStream(name: String): ZIO[Any, JarError, (ZStream[Any, Throwable, Byte], Long)] =
    resourcePath(name).map { p =>
      (ZStream.fromPath(p), Files.size(p))
    }
}
