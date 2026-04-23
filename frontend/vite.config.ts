import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  server: {
    proxy: {
      '/api': {
        // For this repo's docker-compose, FastAPI is exposed on host port 8300.
        // If you run FastAPI directly, switch this to http://localhost:8000.
        target: 'http://localhost:8300',
        changeOrigin: true,
      },
    },
  },
})
