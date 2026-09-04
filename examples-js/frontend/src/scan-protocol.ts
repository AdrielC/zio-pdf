import type { Analysis, TransformExecution, WorkflowExecution } from "zio-pdf-demo";

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
    }
  | {
      readonly kind: "linearize";
      readonly id: number;
      readonly file: File;
    }
  | {
      readonly kind: "merge";
      readonly id: number;
      readonly file: File;
      readonly secondary: File;
    }
  | {
      readonly kind: "append";
      readonly id: number;
      readonly file: File;
    }
  | {
      readonly kind: "flatten";
      readonly id: number;
      readonly file: File;
    }
  | {
      readonly kind: "extract";
      readonly id: number;
      readonly file: File;
      readonly fromPage: number;
      readonly toPage: number;
    }
  | {
      readonly kind: "rotate";
      readonly id: number;
      readonly file: File;
      readonly degrees: number;
      readonly fromPage: number;
      readonly toPage: number;
    }
  | {
      readonly kind: "split";
      readonly id: number;
      readonly file: File;
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
      readonly kind: "workflow-complete";
      readonly id: number;
      readonly execution: WorkflowExecution;
    }
  | {
      readonly kind: "error";
      readonly id: number;
      readonly message: string;
    };
