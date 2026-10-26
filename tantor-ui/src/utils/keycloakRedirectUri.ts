const callbackParameters = ['code', 'state', 'session_state', 'iss', 'error', 'error_description', 'error_uri'];

const removeCallback = (parameters: URLSearchParams) => {
  if (!parameters.has('state') || (!parameters.has('code') && !parameters.has('error'))) return false;
  for (const parameter of callbackParameters) parameters.delete(parameter);
  return true;
};

export function keycloakRedirectUri(href: string): string {
  const url = new URL(href);
  const fragment = new URLSearchParams(url.hash.slice(1));
  // Clean the next login destination, not the callback that the adapter must process.
  if (removeCallback(fragment)) url.hash = fragment.toString();
  removeCallback(url.searchParams);
  return url.href;
}
