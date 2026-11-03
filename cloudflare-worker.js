export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Serve the website HTML and other static files from Cloudflare.
    // The Node.js/WhatsApp backend remains on Render and handles APIs.
    if (request.method === "GET" || request.method === "HEAD") {
      try {
        const assetResponse = await env.ASSETS.fetch(request);
        if (assetResponse.status !== 404) {
          const headers = new Headers(assetResponse.headers);

          // Cloudflare serves frontend/index.html directly, bypassing the
          // Render "/" route that normally injects the shared join panel.
          // Add that panel here so Cloudflare and Render show the same UI.
          if (
            request.method === "GET" &&
            url.pathname === "/" &&
            assetResponse.ok &&
            (headers.get("Content-Type") || "").includes("text/html")
          ) {
            const html = await assetResponse.text();
            if (!html.includes('id="shared-join-groups"')) {
              const panelUrl = new URL("/join-groups-panel.html", url);
              const panelResponse = await env.ASSETS.fetch(
                new Request(panelUrl.toString(), { method: "GET" })
              );

              if (panelResponse.ok) {
                const panel = await panelResponse.text();
                const updatedHtml = html.replace(
                  /<\/body\s*>/i,
                  `${panel}\n</body>`
                );
                headers.set("Cache-Control", "public, max-age=60, must-revalidate");
                headers.delete("Content-Length");
                return new Response(updatedHtml, {
                  status: assetResponse.status,
                  statusText: assetResponse.statusText,
                  headers
                });
              }
            }
          }

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
