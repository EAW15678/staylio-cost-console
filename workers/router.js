const PAGES_ROUTES = {
  dashboard:           'https://staylio-dashboard.erick-542.workers.dev',
  console:             'https://staylio-dashboard.erick-542.workers.dev',
  'dashboard-proxy':   'https://staylio-dashboard-proxy.erick-542.workers.dev',
  intake:              'https://staylio-intake.pages.dev',
};

const BLOCKED = new Set(["www", "app", "api", "admin", "mail", "smtp", "portal"]);

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const hostParts = url.hostname.split(".");
    if (hostParts.length < 3) {
      return new Response("Not found", { status: 404 });
    }
    const slug = hostParts[0];

    if (PAGES_ROUTES[slug]) {
      const target = new URL(request.url);
      target.hostname = new URL(PAGES_ROUTES[slug]).hostname;
      return fetch(new Request(target.toString(), request));
    }

    if (BLOCKED.has(slug)) {
      return new Response("Not found", { status: 404 });
    }

    const r2Key = `${slug}/index.html`;
    let object;
    try {
      object = await env.STAYLIO_PAGES.get(r2Key);
    } catch (err) {
      console.error(`R2 fetch error for key ${r2Key}:`, err);
      return new Response("Internal server error", { status: 500 });
    }

    if (!object) {
      return new Response(
        `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Coming Soon — Staylio</title></head><body><h1>This property page is coming soon.</h1></body></html>`,
        { status: 404, headers: { "Content-Type": "text/html; charset=utf-8" } }
      );
    }

    return new Response(object.body, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "public, max-age=300, s-maxage=3600",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "SAMEORIGIN",
        "Referrer-Policy": "strict-origin-when-cross-origin",
      },
    });
  },
};
