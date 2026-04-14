const ALLOWED_ORIGINS = [
  'https://dashboard.staylio.ai',
  'https://console.staylio.ai',
  'http://localhost:8080',
  'http://localhost:5173',
];

function isAllowedOrigin(origin) {
  return origin && ALLOWED_ORIGINS.includes(origin);
}

function corsHeaders(origin) {
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';

    if (request.method === 'OPTIONS') {
      if (!isAllowedOrigin(origin)) {
        return new Response(JSON.stringify({ detail: 'Origin not allowed.' }), {
          status: 403,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (origin && !isAllowedOrigin(origin)) {
      return new Response(JSON.stringify({ detail: 'Origin not allowed.' }), {
        status: 403,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (request.method !== 'GET') {
      return new Response(JSON.stringify({ detail: 'Method not allowed.' }), {
        status: 405,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (!url.pathname.startsWith('/metrics/')) {
      return new Response(JSON.stringify({ detail: 'Not found.' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const upstreamUrl = env.COST_CONSOLE_URL + url.pathname + url.search;

    let upstreamResponse;
    try {
      upstreamResponse = await fetch(upstreamUrl, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${env.COST_CONSOLE_TOKEN}`,
        },
      });
    } catch (err) {
      return new Response(JSON.stringify({ detail: 'Upstream request failed.' }), {
        status: 502,
        headers: {
          'Content-Type': 'application/json',
          ...(isAllowedOrigin(origin) ? corsHeaders(origin) : {}),
        },
      });
    }

    const body = await upstreamResponse.text();
    return new Response(body, {
      status: upstreamResponse.status,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
        ...(isAllowedOrigin(origin) ? corsHeaders(origin) : {}),
      },
    });
  },
};
