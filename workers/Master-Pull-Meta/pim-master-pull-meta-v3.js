// Worker: pim-master-pull-meta-v3
// Requires bindings:
// - env.DB (D1)
// - env.SHOPIFY_SHOP_DOMAIN (var)
// - env.SHOPIFY_ADMIN_TOKEN (secret)

export default {
  async fetch(request, env) {
    if (request.method === "GET") return new Response("OK", { status: 200 });
    return new Response("Method Not Allowed", { status: 405 });
  },

  async queue(batch, env) {
    const shopDomain = env.SHOPIFY_SHOP_DOMAIN;
    const token = env.SHOPIFY_ADMIN_TOKEN;

    const gql = async (query, variables) => {
      const r = await fetch(`https://${shopDomain}/admin/api/2025-01/graphql.json`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": token,
        },
        body: JSON.stringify({ query, variables }),
      });
      const json = await r.json();
      if (!r.ok || json.errors) throw new Error(JSON.stringify(json.errors ?? json));
      return json.data;
    };

    for (const msg of batch.messages) {
      const m = msg.body || {};

      console.log("META_PULL_MESSAGE", {
  body: m
});


      // ignore deletes
      if (m.topic === "products/delete") {
        msg.ack();
        continue;
      }

      const pim_pid = m.pim_pid;
      const shopifyProductId = m.shopify_product_id;
      
      const productGid = `gid://shopify/Product/${shopifyProductId}`;
      
      let variantIds = Array.isArray(m.shopify_variant_ids) ? m.shopify_variant_ids : [];
      
console.log("META_PULL_VARIANT_IDS_BEFORE_FETCH", {
  variantIds
});


if (!variantIds.length) {
  const vData = await gql(
    `
    query ProductVariants($id: ID!) {
      product(id: $id) {
        variants(first: 100) {
          edges { node { id } }
        }
      }
    }
    `,
    { id: productGid }
  );

  variantIds = (vData?.product?.variants?.edges ?? [])
    .map(e => e.node?.id)
    .filter(Boolean)
    .map(id => String(id).split("/").pop());
}


      if (!pim_pid || !shopifyProductId) {
        msg.ack();
        continue;
      }
      const variantGids = variantIds.filter(Boolean).map((id) => `gid://shopify/ProductVariant/${id}`);

      try {
        // gate: custom.sync
        const gateData = await gql(
          `query Gate($id: ID!) { product(id: $id) { metafield(namespace:"custom", key:"sync"){ value } } }`,
          { id: productGid }
        );

        const syncVal = (gateData?.product?.metafield?.value ?? "").trim().toLowerCase();
        const allowed = syncVal === "true" || syncVal === "1" || syncVal === "yes";
        if (!allowed) {
          msg.ack();
          continue;
        }

        // 1) metafields -> products.metafields_json
        const mfData = await gql(
          `
          query ProductMetafields($id: ID!) {
            product(id: $id) {
              metafields(first: 250) {
                edges { node { namespace key value type updatedAt } }
              }
            }
          }`,
          { id: productGid }
        );

        const metafields = (mfData?.product?.metafields?.edges ?? []).map((e) => e.node);
        const nextJson = JSON.stringify(metafields);
        
        // huidige waarde ophalen
        const prevRow = await env.DB.prepare(`
          SELECT metafields_json
          FROM products
          WHERE pim_pid = ?1
          LIMIT 1
        `).bind(pim_pid).first();
        
        const prevJson = prevRow?.metafields_json ?? "";
        
        // ALLEEN bij echte wijziging → delta_products
        if (!prevRow || prevJson !== nextJson) {
          const deltaEventId =
            m.webhook_event_id ||
            (msg && (msg.id || msg.messageId)) ||
            `${new Date().toISOString()}:metafields`;
        
          await env.DB.prepare(`
          INSERT INTO delta_products (
            pim_pid,
            shopify_product_id,
            webhook_event_id,
            updated_at,
            metafields_json
          ) VALUES (?1, ?2, ?3, ?4, ?5)
          ON CONFLICT(pim_pid, webhook_event_id)
          DO UPDATE SET
            metafields_json = excluded.metafields_json,
            updated_at = excluded.updated_at          
          `)
            .bind(
              pim_pid,
              String(shopifyProductId),
              deltaEventId,
              new Date().toISOString(),
              nextJson
            )
            .run();
        }
        
        // snapshot altijd bijwerken
        await env.DB.prepare(`
          UPDATE products
          SET metafields_json = ?1
          WHERE pim_pid = ?2
        `)
          .bind(nextJson, pim_pid)
          .run();
        

        // 2) weight + hscode + country_of_origin -> variants cols (+ delta_variants)
        if (variantGids.length) {
          const invData = await gql(
            `
            query VariantCustoms($ids: [ID!]!) {
              nodes(ids: $ids) {
                ... on ProductVariant {
                  id
                  inventoryItem {
                    harmonizedSystemCode
                    countryCodeOfOrigin
                    measurement {
                      weight { value unit }
                    }
                    unitCost {
                      amount
                    }
                  }
                }
              }
            }`,
            { ids: variantGids }
          );

          const nodes = (invData?.nodes ?? []).filter(Boolean);
          console.log("META_PULL_VARIANT_NODES", JSON.stringify(nodes, null, 2));


          for (const v of nodes) {
            const numericVid = String(v.id).split("/").pop();

            const weightValue = v.inventoryItem?.measurement?.weight?.value ?? null;
            const weightUnit = v.inventoryItem?.measurement?.weight?.unit ?? "";
            const hscode = v.inventoryItem?.harmonizedSystemCode ?? "";
            const origin = v.inventoryItem?.countryCodeOfOrigin ?? "";
            const cost = v.inventoryItem?.unitCost?.amount ?? null;


            console.log("META_PULL_VALUES", {
              pim_pid,
              shopify_variant_id: numericVid,
              weightValue,
              weightUnit,
              hscode,
              origin
            });
            

            // delta event id (queue msg doesn't have Shopify webhook id)
            // const deltaEventId = String(
            //   (msg && (msg.id || msg.messageId)) || `${m.received_at || new Date().toISOString()}:${numericVid}`
            // );

            const deltaEventId = String(
              m.webhook_event_id || (msg && (msg.id || msg.messageId)) || `${m.received_at || new Date().toISOString()}:${numericVid}`
            );
            


            // diff against current DB values BEFORE update
            const prev = await env.DB.prepare(`
  SELECT
    pim_vid,
    weight_value,
    weight_unit,
    hscode,
    country_of_origin,
    cost
  FROM variants
  WHERE pim_pid = ?1
    AND shopify_variant_id = ?2
  LIMIT 1
`)

              .bind(pim_pid, String(numericVid))
              .first();

              console.log("WEIGHT_DEBUG_BEFORE", {
  pim_pid,
  shopify_variant_id: numericVid,
  db_weight_value: prev?.weight_value ?? null,
  db_weight_unit: prev?.weight_unit ?? null,
  shopify_weight_value: weightValue ?? null,
  shopify_weight_unit: weightUnit ?? null,
  db_type: typeof prev?.weight_value,
  shopify_type: typeof weightValue
});

            const changed = {};
            if ((prev?.weight_value ?? null) !== weightValue) changed.weight_value = weightValue;
            if ((prev?.weight_unit ?? "") !== weightUnit) changed.weight_unit = weightUnit;
            if ((prev?.hscode ?? "") !== hscode) changed.hscode = hscode;
            if ((prev?.country_of_origin ?? "") !== origin) changed.country_of_origin = origin;
            if ((prev?.cost ?? null) !== cost) changed.cost = cost;


            if (Object.keys(changed).length) {
              const cols = [
                "pim_pid",
                "pim_vid",
                "shopify_variant_id",
                "webhook_event_id",
                "updated_at",
                ...Object.keys(changed),
              ];
              const placeholders = cols.map((_, i) => `?${i + 1}`).join(", ");
              const sql = `INSERT OR IGNORE INTO delta_variants (${cols.join(", ")}) VALUES (${placeholders})`;

              await env.DB.prepare(sql)
                .bind(
                  pim_pid,
                  prev?.pim_vid ?? null,
                  String(numericVid),
                  deltaEventId,
                  new Date().toISOString(),
                  ...Object.values(changed)
                )
                .run();
            }

            // update variants table
            // ensure variant row exists (new products)
            await env.DB.prepare(`
            INSERT OR IGNORE INTO variants (
              pim_pid,
              shopify_variant_id,
              created_at
            ) VALUES (?1, ?2, ?3)
            `)
            .bind(pim_pid, String(numericVid), new Date().toISOString())
            .run();
     



const res = await env.DB.prepare(`
UPDATE variants
SET
  weight_value      = ?1,
  weight_unit       = ?2,
  hscode            = ?3,
  country_of_origin = ?4,
  cost              = ?5
WHERE pim_pid = ?6
  AND shopify_variant_id = ?7
`)
.bind(weightValue, weightUnit, hscode, origin, cost, pim_pid, String(numericVid))
.run();

console.log("WEIGHT_DEBUG_AFTER", {
  pim_pid,
  shopify_variant_id: numericVid,
  changes: res?.meta?.changes ?? null,
  new_weight_value: weightValue ?? null,
  new_weight_unit: weightUnit ?? null
});

console.log("META_PULL_UPDATE_RESULT", {
pim_pid,
shopify_variant_id: numericVid,
changes: res?.meta?.changes ?? null
});

          }
        }

        msg.ack();
      } catch (e) {
        console.log("metafields_inventory_worker_error", e?.message ?? String(e));
        msg.retry();
      }
    }
  },
};
