declare module "zio-pdf-demo" {
  export interface Inspection {
    status: "accepted" | "rejected";
    violation?: string;
    completion: string;
    elementsRead: number;
    linearizedObject?: number;
    pdfAObject?: number;
    pdfAPart?: string;
    pdfAConformance?: string;
    pdfA3bDeclared: boolean;
    thumbnailPageObject?: number;
    thumbnailImageObject?: number;
    encrypted: boolean;
    encryptionObject?: number;
    javaScriptObject?: number;
    acroFormObject?: number;
    acroFormFields: number;
    acroFormNeedAppearances: boolean;
    fonts: FontResource[];
  }

  export interface FontResource {
    objectNumber: number;
    baseFont: string;
    subtype?: string;
    existingResourceRemapCandidate: boolean;
  }

  export interface ContentFacts {
    images: number;
    attachments: number;
    tableCandidates: number;
    imageEvidence: string;
    attachmentEvidence: string;
    tableCandidateEvidence: string;
    pages: number;
    textPages: number;
    textCharacters: number;
    textPreview: string;
    citations: Citation[];
    textRecoveryRequests: TextRecoveryRequest[];
  }

  export interface Citation {
    id: string;
    page: number;
    pageObjectNumber: number;
    contentObjectNumbers: number[];
    excerpt: string;
    truncated: boolean;
  }

  export interface TextRecoveryRequest {
    page: number;
    pageObjectNumber: number;
    contentObjectNumbers: number[];
    reason: "NoUsableNativeText";
  }

  export interface Analysis {
    inspection: Inspection;
    content: ContentFacts;
    valid: boolean;
    strictPolicyPassed: boolean;
    sha256: string;
    decodedEvents: number;
    elapsedMs: number;
  }

  export interface TransformPlan {
    operations: string[];
    requiresMaterializedDocument: boolean;
    readsContentStreams: boolean;
    code: string;
  }

  export interface TransformExecution {
    sourceFont?: string;
    targetFont?: string;
    sourceObjectNumbers: number[];
    targetObjectNumber?: number;
    resourceBindingsRewritten: number;
    tokenPages: number;
    tokenCount: number;
    chunks: Uint8Array[];
    outputBytes: number;
    maxMaterializedBytes: number;
  }

  export interface WorkflowExecution {
    kind: "linearize" | "merge" | "append" | "flatten";
    chunks: Uint8Array[];
    outputBytes: number;
    maxMaterializedBytes: number;
    firstPagePrefixBytes?: number;
  }

  export const ZioPdfDemo: {
  analyze(input: Uint8Array): Promise<Analysis>;
  analyzeBlob(input: Blob): Promise<Analysis>;
  analyzeBlobWithProgress(
    input: Blob,
    progress: (phase: string, loadedBytes: number, totalBytes: number) => void
  ): Promise<Analysis>;
  inspectTransformPlan(
    remapExistingFonts: boolean,
    sourceFont: string,
    targetFont: string,
    tokenize: boolean,
    tokenizer: "characters" | "words"
  ): TransformPlan;
  executeTransformBlob(
    input: Blob,
    remapExistingFonts: boolean,
    sourceFont: string,
    targetFont: string,
    tokenize: boolean,
    tokenizer: "characters" | "words"
  ): Promise<TransformExecution>;
  linearizeBlob(input: Blob): Promise<WorkflowExecution>;
  mergeBlobs(primary: Blob, secondary: Blob): Promise<WorkflowExecution>;
  appendRevisionBlob(input: Blob): Promise<WorkflowExecution>;
  flattenFormsBlob(input: Blob): Promise<WorkflowExecution>;
  attachFirstPageThumbnail(
    input: Uint8Array,
    grayPixels: Uint8Array | undefined,
    width: number,
    height: number
  ): Promise<Uint8Array>;
};
}
