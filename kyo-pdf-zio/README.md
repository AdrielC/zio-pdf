# kyo-pdf-zio

Compatibility adapters for using the Kyo-native `kyo-pdf` core from ZIO
applications. `PdfZIO` uses Kyo's official `ZIOs.run` interpreter so typed
`Abort` failures remain typed ZIO failures and cancellation reaches the Kyo
fiber.

This module is deliberately one-way: the PDF implementation lives in
`kyo-pdf`; ZIO is an application-edge adapter.
