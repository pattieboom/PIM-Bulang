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

    return new Response("Not found", { status: 404 });
  },
};