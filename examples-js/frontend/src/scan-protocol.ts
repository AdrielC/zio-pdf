import type { Analysis, TransformExecution } from "zio-pdf-demo";

export type ScanWorkerRequest =
  | {
      readonly kind: "analyze";
      readonly id: number;
      readonly file: File;
    }
  | {
      readonly kind: "transform";
      readonly id: number;
      readonly file: File;
      readonly remapExistingFonts: boolean;
      readonly sourceFont: string;
      readonly targetFont: string;
      readonly tokenize: boolean;
      readonly tokenizer: "characters" | "words";
    };

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
      readonly kind: "transform-complete";
      readonly id: number;
      readonly execution: TransformExecution;
    }
  | {
      readonly kind: "error";
      readonly id: number;
      readonly message: string;
    };
