import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Local `npm run dev` proxies /api to the backend; in Docker, nginx does the same.
const apiTarget = process.env.API_PROXY_TARGET || 'http://localhost:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    host: true,
    proxy: {
      '/api': {
        target: apiTarget,
        changeOrigin: true
      }
    }
  }
});
