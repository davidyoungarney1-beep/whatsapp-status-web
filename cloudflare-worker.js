export default {
  async fetch(request) {
    const upstream = new URL(request.url);
    upstream.protocol = "https:";
    upstream.hostname = "whatsapp-status-web.onrender.com";
    upstream.port = "";

    // Keep the original path, query, method, headers, and request body.
    // Render remains the single source of truth for the Node.js/WhatsApp backend.
    return fetch(new Request(upstream.toString(), request));
  }
};
