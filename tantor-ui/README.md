# React + TypeScript + Vite

## Keycloak SSO

The UI uses Keycloak Authorization Code Flow with PKCE S256 through `keycloak-js`.
Do not configure a client secret in the React application.

Deployment-supplied client settings:

```text
Keycloak URL: <identity-provider-origin>
Realm: <realm>
Client ID: <public-client-id>
Client type: Public
```

Required Keycloak client configuration:

```text
Client authentication: OFF
Standard flow: ON
Direct access grants: OFF
Implicit flow: OFF

Valid redirect URIs:
<frontend-origin>/*

Valid post logout redirect URIs:
<frontend-origin>/*

Web origins:
<frontend-origin>
```

Optional local-development Vite overrides:

```text
VITE_KEYCLOAK_URL=https://identity.development.internal
VITE_KEYCLOAK_REALM=development
VITE_KEYCLOAK_CLIENT_ID=tantor-ui-development
```

Production values are not compiled into the bundle. `package-deployment.ps1`
generates `runtime-config.js`; `runtimeConfig.ts` validates it and routes all
same-origin `/api` and `/api/v1/artifacts` calls through its configured paths.

Every protected backend request must include `Authorization: Bearer <access-token>`.
The backend must validate token signature, issuer, expiry, and required roles or authorities.

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Oxc](https://oxc.rs)
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the ESLint configuration

If you are developing a production application, we recommend updating the configuration to enable type-aware lint rules:

```js
export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...

      // Remove tseslint.configs.recommended and replace with this
      tseslint.configs.recommendedTypeChecked,
      // Alternatively, use this for stricter rules
      tseslint.configs.strictTypeChecked,
      // Optionally, add this for stylistic rules
      tseslint.configs.stylisticTypeChecked,

      // Other configs...
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])
```

You can also install [eslint-plugin-react-x](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-x) and [eslint-plugin-react-dom](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-dom) for React-specific lint rules:

```js
// eslint.config.js
import reactX from 'eslint-plugin-react-x'
import reactDom from 'eslint-plugin-react-dom'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...
      // Enable lint rules for React
      reactX.configs['recommended-typescript'],
      // Enable lint rules for React DOM
      reactDom.configs.recommended,
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])
```
