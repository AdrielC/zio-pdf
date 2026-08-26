import { ZioPdfDemo } from "zio-pdf-demo";
import type { ScanWorkerMessage, ScanWorkerRequest } from "./scan-protocol";

const workerScope = self as unknown as {
  addEventListener(type: "message", listener: (event: MessageEvent<ScanWorkerRequest>) => void): void;
  postMessage(message: ScanWorkerMessage, options?: StructuredSerializeOptions): void;
};

workerScope.addEventListener("message", (event) => {
  const request = event.data;
  if (request.kind === "transform") {
    void ZioPdfDemo.executeTransformBlob(
      request.file,
      request.remapExistingFonts,
      request.sourceFont,
      request.targetFont,
      request.tokenize,
      request.tokenizer
    ).then(
      (execution) => workerScope.postMessage(
        { kind: "transform-complete", id: request.id, execution },
        { transfer: execution.chunks.map((chunk) => chunk.buffer) }
      ),
      (error: unknown) => workerScope.postMessage({
        kind: "error",
        id: request.id,
        message: error instanceof Error ? error.message : "The PDF transform worker stopped unexpectedly."
      })
    );
    return;
  }

  void ZioPdfDemo.analyzeBlobWithProgress(request.file, (phase, loadedBytes, totalBytes) => {
    workerScope.postMessage({
      kind: "progress",
      id: request.id,
      phase,
      loadedBytes,
      totalBytes
    });
  }).then(
    (analysis) => workerScope.postMessage({ kind: "complete", id: request.id, analysis }),
    (error: unknown) => workerScope.postMessage({
      kind: "error",
      id: request.id,
      message: error instanceof Error ? error.message : "The PDF evidence worker stopped unexpectedly."
    })
  );
});
