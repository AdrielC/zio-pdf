// Optional native reader check for the synthetic regression, run on macOS.
import Foundation
import PDFKit
import Darwin

guard CommandLine.arguments.count == 3,
      let expected = Int(CommandLine.arguments[2]), expected > 0 else {
    print("Usage: swift check_pdfkit.swift file.pdf expected-pages")
    exit(2)
}
guard let document = PDFDocument(url: URL(fileURLWithPath: CommandLine.arguments[1])),
      document.pageCount == expected,
      (0..<expected).allSatisfy({ document.page(at: $0) != nil }) else {
    print("PDFKit could not open the expected pages")
    exit(1)
}
print("PDFKit opened all \(expected) pages")
