/**
 * Google sign-in (Google Identity Services token model) for "Deploy now".
 * The access token is kept in memory only and sent only to *.googleapis.com.
 *
 * Needs an OAuth 2.0 Client ID (type "Web application") whose Authorized
 * JavaScript origins include this site's origin. Provided at build time via
 * VITE_GOOGLE_CLIENT_ID, or entered by the user in the deploy dialog.
 */

const GIS_SRC = "https://accounts.google.com/gsi/client";
export const DEPLOY_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

interface TokenResponse {
  access_token?: string;
  expires_in?: number;
  error?: string;
  error_description?: string;
}

interface TokenClient {
  requestAccessToken: (overrides?: { prompt?: string }) => void;
}

interface GoogleOAuth2 {
  initTokenClient: (config: {
    client_id: string;
    scope: string;
    callback: (r: TokenResponse) => void;
    error_callback?: (e: { type?: string; message?: string }) => void;
  }) => TokenClient;
  revoke: (token: string, done?: () => void) => void;
}

declare global {
  interface Window {
    google?: { accounts?: { oauth2?: GoogleOAuth2 } };
  }
}

export const BUILT_IN_CLIENT_ID: string = (import.meta.env.VITE_GOOGLE_CLIENT_ID as string | undefined)?.trim() ?? "";

let loader: Promise<GoogleOAuth2> | null = null;

function loadGis(): Promise<GoogleOAuth2> {
  if (window.google?.accounts?.oauth2) return Promise.resolve(window.google.accounts.oauth2);
  loader ??= new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = GIS_SRC;
    s.async = true;
    s.onload = () => (window.google?.accounts?.oauth2 ? resolve(window.google.accounts.oauth2) : reject(new Error("Google sign-in failed to load.")));
    s.onerror = () => {
      loader = null;
      reject(new Error("Could not load Google sign-in (blocked by an extension or offline?)."));
    };
    document.head.appendChild(s);
  });
  return loader;
}

export interface AccessToken {
  token: string;
  expiresAt: number;
}

export async function signIn(clientId: string): Promise<AccessToken> {
  if (!/^[0-9]+-[a-z0-9]+\.apps\.googleusercontent\.com$/.test(clientId)) {
    throw new Error("That doesn't look like an OAuth Client ID (…apps.googleusercontent.com).");
  }
  const oauth2 = await loadGis();
  return new Promise((resolve, reject) => {
    const client = oauth2.initTokenClient({
      client_id: clientId,
      scope: DEPLOY_SCOPE,
      callback: (r) => {
        if (r.error || !r.access_token) reject(new Error(r.error_description || r.error || "Sign-in was cancelled."));
        else resolve({ token: r.access_token, expiresAt: Date.now() + (r.expires_in ?? 3600) * 1000 });
      },
      error_callback: (e) => reject(new Error(e.message || (e.type === "popup_closed" ? "Sign-in window was closed." : "Sign-in failed."))),
    });
    client.requestAccessToken({ prompt: "" });
  });
}

export function signOut(token: string): void {
  window.google?.accounts?.oauth2?.revoke(token);
}
