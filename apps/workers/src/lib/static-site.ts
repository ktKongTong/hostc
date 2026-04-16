const HTML_REQUEST_METHODS = new Set(["GET", "HEAD"]);
const SERVICE_UNAVAILABLE_PAGE_PATH = "/errors/service-unavailable/";

export function canServeStaticAsset(request: Request): boolean {
	return HTML_REQUEST_METHODS.has(request.method);
}

export function wantsHtmlResponse(request: Request): boolean {
	if (!canServeStaticAsset(request)) {
		return false;
	}

	const destination = request.headers.get("sec-fetch-dest");

	if (destination === "document") {
		return true;
	}

	const accept = request.headers.get("accept") ?? "";
	return accept.includes("text/html");
}

export function serveStaticAsset(
	request: Request,
	env: Env,
): Promise<Response> {
	return env.ASSETS.fetch(request);
}

export async function serveServiceUnavailablePage(
	request: Request,
	env: Env,
	status = 503,
): Promise<Response> {
	const assetUrl = new URL(request.url);
	assetUrl.pathname = SERVICE_UNAVAILABLE_PAGE_PATH;
	assetUrl.search = "";
	assetUrl.hash = "";

	const assetResponse = await env.ASSETS.fetch(
		new Request(assetUrl.toString(), {
			method: request.method,
			headers: request.headers,
		}),
	);

	if (!assetResponse.ok) {
		return new Response("Service unavailable", {
			status,
			headers: {
				"cache-control": "no-store",
				"content-type": "text/plain; charset=UTF-8",
			},
		});
	}

	const headers = new Headers(assetResponse.headers);
	headers.set("cache-control", "no-store");

	return new Response(request.method === "HEAD" ? null : assetResponse.body, {
		status,
		headers,
	});
}
