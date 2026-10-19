import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import './styles/TableTheme.css'
import App from './App.tsx'
import { AuthProvider } from './contexts/AuthContext.tsx'

async function start() {
  let SessionProvider = AuthProvider;
  // Temporary local UI fixtures. Remove this hook and src/dev before pushing.
  if (import.meta.env.DEV && ['localhost', '127.0.0.1', '[::1]'].includes(window.location.hostname)) {
    const { demoEnabled, installDemoAgent } = await import('./dev/demoAgent');
    if (demoEnabled()) {
      installDemoAgent();
      SessionProvider = (await import('./dev/DemoAuthProvider')).DemoAuthProvider;
    }
  }
  createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <SessionProvider>
      <App />
    </SessionProvider>
  </StrictMode>,
)
}

void start();
