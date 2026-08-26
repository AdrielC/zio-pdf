import { readdir, readFile } from "node:fs/promises";

const source = await readFile(new URL("../src/main.ts", import.meta.url), "utf8");
const requiredImports = [
  'pdfjs-dist/legacy/build/pdf.mjs',
  'pdfjs-dist/legacy/build/pdf.worker.min.mjs?worker'
];

for (const requiredImport of requiredImports) {
  if (!source.includes(requiredImport)) {
    throw new Error(`PDF preview compatibility import is missing: ${requiredImport}`);
  }
}

if (source.includes('pdfjs-dist/build/pdf.worker.min.mjs')) {
  throw new Error("The raw modern PDF.js worker must not be shipped to browsers.");
}

const assets = await readdir(new URL("../dist/assets/", import.meta.url));
if (!assets.some((name) => /^pdf\.worker\.min-[\w-]+\.js$/.test(name))) {
  throw new Error("The PDF.js worker was not compiled into a browser-compatible JavaScript asset.");
}
if (assets.some((name) => /^pdf\.worker\.min-[\w-]+\.mjs$/.test(name))) {
  throw new Error("The build copied an uncompiled PDF.js module worker.");
}

Reflect.deleteProperty(Promise, "withResolvers");
Reflect.deleteProperty(Uint8Array, "fromBase64");
const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
if (typeof pdfjs.getDocument !== "function") {
  throw new Error("The PDF.js legacy display layer did not load.");
}
if (typeof Promise.withResolvers !== "function" || typeof Uint8Array.fromBase64 !== "function") {
  throw new Error("The PDF.js legacy display layer did not install its required runtime polyfills.");
}

console.log("PDF preview compatibility gate passed.");
