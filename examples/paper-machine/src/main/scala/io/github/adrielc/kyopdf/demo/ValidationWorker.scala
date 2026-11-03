package io.github.adrielc.kyopdf.demo

import java.nio.file.Path
import kyo.*
import org.verapdf.gf.foundry.VeraGreenfieldFoundryProvider
import org.verapdf.pdfa.Foundries
import org.verapdf.pdfa.flavours.PDFAFlavour
import org.verapdf.pdfa.results.TestAssertion
import org.verapdf.processor.reports.enums.JobEndStatus
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** A disposable, heap-limited JVM validates every requested profile against one parsed PDF. */
object ValidationWorker:
  val Version = "1.30.3"
  private def quote(value: String): String = PaperMachine.quote(value)

  def main(args: Array[String]): Unit =
    import kyo.AllowUnsafe.embrace.danger
    val work: String < (Abort[Exception] & Sync) = Abort.catching[Exception]((error: Exception) => error) {
      Sync.defer {
        require(args.length == 2, "Expected a PDF path and explicit profiles")
        val profiles = args(1).split(',').toVector
        require(profiles.nonEmpty && profiles.forall(PdfValidation.Profiles.contains), "Unsupported validation profile")
        val flavours = profiles.map(PDFAFlavour.byFlavourId).asJava
        VeraGreenfieldFoundryProvider.initialise()
        Using.resource(Foundries.defaultInstance().createParser(Path.of(args(0)).toFile, flavours.get(0))) { parser =>
          parser.setFlavours(flavours)
          Using.resource(Foundries.defaultInstance().createValidator(flavours, 3, false, true, false)) { validator =>
            val results = validator.validateAll(parser).asScala.toVector
            require(results.length == profiles.length, "Validator returned incomplete results")
            val json = results.map { result =>
              val complete = result.getJobEndStatus == JobEndStatus.NORMAL
              val status = if !complete then "unable" else if result.isCompliant then "passed" else "failed"
              val failures = result.getTestAssertions.asScala.iterator.filter(_.getStatus == TestAssertion.Status.FAILED).take(24).map { failure =>
                val rule = failure.getRuleId
                val message = Option(failure.getErrorMessage).filter(_.nonEmpty).getOrElse(failure.getMessage)
                s"{\"clause\":${quote(rule.getClause)},\"test\":${rule.getTestNumber},\"message\":${quote(Option(message).getOrElse("Rule failed").take(480))},\"context\":${quote(Option(failure.getLocationContext).getOrElse("").take(640))}}"
              }.mkString("[", ",", "]")
              val failedChecks = result.getFailedChecks.values().asScala.map(_.longValue()).sum
              s"{\"profile\":${quote(result.getPDFAFlavour.getId)},\"status\":${quote(status)},\"checks\":${result.getTotalAssertions},\"failedRules\":${result.getFailedChecks.size},\"failedChecks\":$failedChecks,\"failures\":$failures,\"detailsLimited\":true}"
            }.mkString("[", ",", "]")
            s"{\"results\":$json}"
          }
        }
      }
    }
    val output = Abort.run[Exception](Sync.Unsafe.run(work)).eval match
      case Result.Success(value) => value
      case _ => "{\"error\":\"The validator could not complete. The PDF may be encrypted, malformed, unsupported, or exceed resource limits.\"}"
    println(output)
