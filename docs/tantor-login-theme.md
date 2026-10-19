# Tantor login theme

Issue #34 is implemented as the `tantor` Keycloak login theme in
`themes/tantor/login`. It extends the existing `keycloak.v2` theme and retains
its forms, scripts, field names, error messages, and authentication behavior.
Only CSS, English branding messages, and the existing Tantor logo are added.
No external fonts, scripts, or images are requested by the custom theme.

## Install and select

Validated against the local Keycloak 26.7.4 theme structure. Confirm compatibility
with the target Keycloak version before production deployment.

1. Copy the complete `themes/tantor` directory into the Keycloak server's
   `themes` directory, or mount it read-only at `/opt/keycloak/themes/tantor`.
2. In the Keycloak admin console, select the realm used by Tantor (`Gatekeeper`
   locally), then open **Realm settings > Themes**.
3. Set **Login theme** to `tantor` and save. Leave all other realm settings and
   authentication flows unchanged.
4. Open Tantor in a new Chrome Incognito window to inspect the login page
   without an existing sign-in session.

The local Compose file includes the theme mount for subsequent container
creation. Copying the directory into an already-running local container allows
previewing it without recreating that container. Selection on an existing realm
must be saved explicitly; realm import does not overwrite existing realms.

This theme is not included in Vite's frontend bundle. Deploy it to the identity
provider separately. Do not disable production theme caching for this change.
If cached assets remain after deployment, use the environment's normal Keycloak
theme-cache refresh procedure.

## Checks

- Logo, heading, fields, show-password control, and sign-in button are readable
  on desktop and mobile-width windows.
- Tab navigation, visible focus, and password visibility use inherited controls.
- Empty or rejected credentials still show Keycloak's validation messages.
- Successful sign-in returns to the existing application redirect URL.
- Password reset, OTP, and other enabled login flows retain inherited templates;
  exercise those flows in the target environment before sign-off.

To revert only the appearance, restore the previous **Login theme** selection.
Do not reset the realm or change clients, users, secrets, roles, or redirects.
