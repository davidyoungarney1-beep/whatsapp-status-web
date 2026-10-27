export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Serve the website HTML and other static files from Cloudflare.
    // The Node.js/WhatsApp backend remains on Render and is used as a fallback
    // for API and other non-static requests.
    if (request.method === "GET" || request.method === "HEAD") {
      try {
        const assetResponse = await env.ASSETS.fetch(request);
        if (assetResponse.status !== 404) {
          const headers = new Headers(assetResponse.headers);
          headers.set("Cache-Control", url.pathname.endsWith(".html")
            ? "public, max-age=60, must-revalidate"
            : "public, max-age=86400");
          return new Response(assetResponse.body, {
            status: assetResponse.status,
            statusText: assetResponse.statusText,
            headers
          });
        }
      } catch (error) {
        // Continue to the backend if the asset binding is unavailable.
      }
    }

    // Keep the existing Render backend as the source of truth for APIs,
    // WhatsApp sessions, saved groups, forwarding, and account data.
    const upstream = new URL(request.url);
    upstream.protocol = "https:";
    upstream.hostname = "whatsapp-status-web.onrender.com";
    upstream.port = "";

    const headers = new Headers(request.headers);
    headers.set("Host", "whatsapp-status-web.onrender.com");

    return fetch(new Request(upstream.toString(), {
      method: request.method,
      headers,
      body: ["GET", "HEAD"].includes(request.method) ? undefined : request.body,
      redirect: "follow"
    }));
  }
};
