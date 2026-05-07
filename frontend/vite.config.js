import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiLocal = "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "localhost",
    port: 5173,
    strictPort: true,
    proxy: {
      "/fetch-leads": { target: apiLocal, changeOrigin: true },
      "/leads": { target: apiLocal, changeOrigin: true },
      "/jobs": { target: apiLocal, changeOrigin: true },
      "/settings": { target: apiLocal, changeOrigin: true },
      "/health": { target: apiLocal, changeOrigin: true },
      "/docs": { target: apiLocal, changeOrigin: true },
      "/openapi.json": { target: apiLocal, changeOrigin: true },
    },
  },
});
