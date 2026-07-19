import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The Lakeleto UI is embedded in the binary (rust-embed over `frontend/dist/`, --features
// serve) and served at `/` with SPA fallback, so assets use RELATIVE paths (`base: "./"`).
// Build output is committed so `cargo build` / `cargo install` need no Node toolchain.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "assets/[name].js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/[name][extname]",
      },
    },
  },
  server: {
    // `npm run dev` proxies the API to a locally-running `lakeleto serve`.
    proxy: { "/v1": "http://127.0.0.1:8080" },
  },
});
