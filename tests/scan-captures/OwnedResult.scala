import scala.language.experimental.captureChecking
import io.github.adrielc.bytescan.SharedScan.View
import io.github.adrielc.bytescan.capture.ScopedView

object OwnedResult:
  def read(view: View) = ScopedView.use(view)(borrow => borrow.read(borrow.from))
  def detach(view: View) = ScopedView.use(view)(borrow => borrow.detach(borrow.from, borrow.until))
