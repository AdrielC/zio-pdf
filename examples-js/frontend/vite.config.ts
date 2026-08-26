import { defineConfig } from "vite";

const normalizeBase = (value: string | undefined): string => {
  const trimmed = value?.trim();
  if (!trimmed) return "/";
  const withLeadingSlash = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeadingSlash.endsWith("/") ? withLeadingSlash : `${withLeadingSlash}/`;
};

export default defineConfig({
  // Gitea Pages serves the published workspace under /docs/zio-pdf/ while local
  // development and consumer embeds remain rooted at /.
  base: normalizeBase(process.env.DOCS_BASE),
  build: {
    // The lazy Scala.js runtime is intentionally its own 1.5 MB optimized chunk.
    chunkSizeWarningLimit: 1600,
  },
  resolve: {
    alias: {
      "zio-pdf-demo": new URL("./src/generated/main.js", import.meta.url).pathname,
    },
  },
  server: {
    host: "127.0.0.1",
    port: 5176,
    strictPort: true
  }
});
