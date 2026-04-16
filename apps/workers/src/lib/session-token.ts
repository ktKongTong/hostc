import { createSignedToken, verifySignedToken } from "./signed-token";

const SESSION_TOKEN_TTL_MS = 10 * 60_000;
const SESSION_TOKEN_VERSION = 1;
const SESSION_TOKEN_OPTIONS = {
	ttlMs: SESSION_TOKEN_TTL_MS,
	version: SESSION_TOKEN_VERSION,
};

export async function createSessionToken(
	secret: string,
	subdomain: string,
): Promise<string> {
	return createSignedToken(secret, subdomain, SESSION_TOKEN_OPTIONS);
}

export async function verifySessionToken(
	secret: string,
	subdomain: string,
	token: string,
): Promise<boolean> {
	return verifySignedToken(secret, subdomain, token, SESSION_TOKEN_OPTIONS);
}
