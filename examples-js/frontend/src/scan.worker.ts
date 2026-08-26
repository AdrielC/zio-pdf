import { ZioPdfDemo } from "zio-pdf-demo";
import type { ScanWorkerMessage, ScanWorkerRequest } from "./scan-protocol";

const workerScope = self as unknown as {
  addEventListener(type: "message", listener: (event: MessageEvent<ScanWorkerRequest>) => void): void;
  postMessage(message: ScanWorkerMessage): void;
};

workerScope.addEventListener("message", (event) => {
  const request = event.data;
  if (request.kind !== "analyze") return;

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
