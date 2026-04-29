export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/auth") {
      const shop = url.searchParams.get("shop");

      if (!shop) {
        return new Response("Missing shop", { status: 400 });
      }

      // 1. state genereren
      const state = crypto.randomUUID();

      const now = new Date();
      const expires = new Date(now.getTime() + 10 * 60 * 1000); // 10 min

      // 2. opslaan in D1
      await env.DB.prepare(`
        INSERT INTO oauth_states (state, shop_domain, created_at, expires_at)
        VALUES (?1, ?2, ?3, ?4)
      `)
        .bind(
          state,
          shop,
          now.toISOString(),
          expires.toISOString()
        )
        .run();

      // 3. OAuth URL bouwen
      const params = new URLSearchParams({
        client_id: env.SHOPIFY_API_KEY,
        scope: "read_products,write_products", // later uitbreiden
        redirect_uri: `${env.APP_URL}/auth/callback`,
        state,
      });

      const redirectUrl = `https://${shop}/admin/oauth/authorize?${params.toString()}`;

      // 4. redirect
      return Response.redirect(redirectUrl, 302);
    }

// endpoint auth/callback
    if (url.pathname === "/auth/callback") {
      const params = Object.fromEntries(url.searchParams.entries());
      const { hmac, code, shop, state } = params;

  if (!hmac || !code || !shop || !state) {
    return new Response("Missing params", { status: 400 });
  }

  // =========================
  // 1. HMAC VALIDATIE
  // =========================
  const message = Object.keys(params)
    .filter((key) => key !== "hmac")
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");

  const encoder = new TextEncoder();

  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(env.SHOPIFY_API_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(message)
  );

  const digest = Array.from(new Uint8Array(signature))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  if (digest !== hmac) {
    return new Response("Invalid HMAC", { status: 403 });
  }

  // =========================
  // 2. STATE VALIDATIE
  // =========================
  const stateRow = await env.DB.prepare(`
    SELECT * FROM oauth_states
    WHERE state = ?1
    LIMIT 1
  `)
    .bind(state)
    .first();

  if (!stateRow) {
    return new Response("Invalid state", { status: 403 });
  }

  if (stateRow.used_at) {
    return new Response("State already used", { status: 403 });
  }

  if (new Date(stateRow.expires_at) < new Date()) {
    return new Response("State expired", { status: 403 });
  }

  // =========================
  // 3. TOKEN OPHALEN
  // =========================
  const tokenRes = await fetch(
    `https://${shop}/admin/oauth/access_token`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        client_id: env.SHOPIFY_API_KEY,
        client_secret: env.SHOPIFY_API_SECRET,
        code,
      }),
    }
  );

  const tokenData = await tokenRes.json();

  const accessToken = tokenData.access_token;

  if (!accessToken) {
    return new Response("Token exchange failed", { status: 500 });
  }

  // =========================
  // 4. OPSLAAN IN SHOPS
  // =========================
  // check bestaande shop
const existing = await env.DB.prepare(`
  SELECT company_id FROM shops WHERE shop_domain = ?1
`)
.bind(shop)
.first();

let companyId;

if (existing && existing.company_id) {
  companyId = existing.company_id;
} else {
  companyId = crypto.randomUUID();
}

// insert/update zonder company_id te slopen
await env.DB.prepare(`
  INSERT INTO shops (
    shop_domain,
    access_token,
    company_id,
    installed_at,
    is_active
  )
  VALUES (?1, ?2, ?3, ?4, 1)
  ON CONFLICT (shop_domain) DO UPDATE SET
    access_token = excluded.access_token,
    is_active = 1
`)
.bind(
  shop,
  accessToken,
  companyId,
  new Date().toISOString()
)
.run();

  // =========================
  // 5. REDIRECT NAAR APP
  // =========================
return Response.redirect(
  `https://${shop}/admin/apps/${env.SHOPIFY_API_KEY}`,
  302
);
} // einde endpoint /auth/callback

// endpoint /api/products

if (url.pathname === "/api/products") {
  const shop = url.searchParams.get("shop");

  if (!shop) {
    return new Response("Missing shop", { status: 400 });
  }

  // check of shop bestaat
  const shopRow = await env.DB.prepare(`
    SELECT 1 FROM shops WHERE shop_domain = ?1 LIMIT 1
  `).bind(shop).first();

  if (!shopRow) {
    return new Response("Unauthorized", { status: 401 });
  }

  const products = await env.DB.prepare(`
    SELECT *
    FROM products
    WHERE shop_domain = ?1
    LIMIT 50
  `)
    .bind(shop)
    .all();

  return new Response(JSON.stringify(products.results), {
    headers: { "Content-Type": "application/json" },
  });
}

//einde endpoint /api/products

    return new Response("Not found", { status: 404 });
  },
};