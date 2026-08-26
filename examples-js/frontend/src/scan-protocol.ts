import type { Analysis } from "zio-pdf-demo";

export interface ScanWorkerRequest {
  readonly kind: "analyze";
  readonly id: number;
  readonly file: File;
}

export type ScanWorkerMessage =
  | {
      readonly kind: "progress";
      readonly id: number;
      readonly phase: string;
      readonly loadedBytes: number;
      readonly totalBytes: number;
    }
  | {
      readonly kind: "complete";
      readonly id: number;
      readonly analysis: Analysis;
    }
  | {
      readonly kind: "error";
      readonly id: number;
      readonly message: string;
    };
