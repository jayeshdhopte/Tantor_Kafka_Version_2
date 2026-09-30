// Deployment replaces this file at runtime. The empty object is safe for the
// Vite development server; production validation rejects missing values.
window.__TANTOR_CONFIG__ = window.__TANTOR_CONFIG__ || {
  environment: "development",
  publicOrigin: window.location.origin,
  authEnabled: false,
  apiBasePath: "/api",
  artifactApiBasePath: "/api/v1/artifacts"
};
