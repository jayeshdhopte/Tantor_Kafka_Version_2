import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const envDir = '..'
  const env = loadEnv(mode, envDir, '')
  const apiTarget = env.VITE_API_PROXY_TARGET || 'http://localhost:8443'
  const artifactTarget = env.VITE_ARTIFACT_PROXY_TARGET || 'http://localhost:8081'

  return {
    envDir,
    plugins: [react()],
    build: {
      // Vite 7 uses Rollup rather than Vite 8's Rolldown bundler. Keep normal
      // production minification enabled; Recharts and its CommonJS helpers
      // remain in one valid dependency graph.
      minify: 'esbuild',
      chunkSizeWarningLimit: 800,
      // Let Rollup derive chunks from the full dependency graph.
    },
    server: {
      proxy: {
        '/api/v1/artifacts': {
          target: artifactTarget,
          secure: false,
        },
        '/api': {
          target: apiTarget,
          secure: false,
        },
      },
    },
    preview: {
      proxy: {
        '/api/v1/artifacts': {
          target: artifactTarget,
          secure: false,
        },
        '/api': {
          target: apiTarget,
          secure: false,
        },
      },
    },
  }
})
