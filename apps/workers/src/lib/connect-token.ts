import { createSignedToken, verifySignedToken } from "./signed-token";

const CONNECT_TOKEN_TTL_MS = 60_000;
const CONNECT_TOKEN_VERSION = 1;
const CONNECT_TOKEN_OPTIONS = {
	ttlMs: CONNECT_TOKEN_TTL_MS,
	version: CONNECT_TOKEN_VERSION,
};

export async function createConnectToken(
	secret: string,
	subdomain: string,
): Promise<string> {
	return createSignedToken(secret, subdomain, CONNECT_TOKEN_OPTIONS);
}

export async function verifyConnectToken(
	secret: string,
	subdomain: string,
	token: string,
): Promise<boolean> {
	return verifySignedToken(secret, subdomain, token, CONNECT_TOKEN_OPTIONS);
}
