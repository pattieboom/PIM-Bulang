// Pull from Master Worker

export default {
  async fetch(request, env, ctx) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let payload;
    try {
      payload = await request.json();
      console.log("shopify_webhook_payload", JSON.stringify(payload));
    } catch {
      return new Response("Bad Request", { status: 400 });
    }
    const variantUpdateMap = new Map(
      (payload.variant_gids ?? []).map(v => [
        v.admin_graphql_api_id.split("/").pop(),
        v.updated_at
      ])
    );
    


    const shopifyProductId = payload?.id;
    const variantIds = Array.isArray(payload?.variants)
      ? payload.variants.map(v => v?.id).filter(Boolean)
      : [];

    if (!shopifyProductId) {
      return new Response("Missing product id", { status: 400 });
    }

    const now = new Date().toISOString();
    const notDeleted = "";

    const topic = request.headers.get("X-Shopify-Topic");
    const webhookEventId = request.headers.get("x-shopify-webhook-id") || "";
    

// === INVENTORY ITEM UPDATE WEBHOOK ===

if (topic === "inventory_items/update") {
  const inventoryItemId = payload?.id;
  if (!inventoryItemId) {
    return new Response("OK", { status: 200 });
  }

  // lookup variant + product via inventory_item_id
  const row = await env.DB.prepare(`
    SELECT
      v.shopify_variant_id,
      p.shopify_product_id,
      p.pim_pid
    FROM variants v
    JOIN products p ON p.pim_pid = v.pim_pid
    WHERE v.inventory_item_id = ?1
    LIMIT 1
  `).bind(String(inventoryItemId)).first();

  if (!row?.shopify_product_id) {
    // inventory item bestaat, maar variant nog niet in PIM
    return new Response("OK", { status: 200 });
  }

  // HERGEBRUIK bestaande flow: queue product pull
  await env.PULL_QUEUE.send({
    pim_pid: row.pim_pid,
    shopify_product_id: String(row.shopify_product_id),
    shopify_variant_ids: [String(row.shopify_variant_id)],
    received_at: new Date().toISOString(),
  });

  // trigger sync worker too (cost-only inventory changes have no product webhook)
await env.SYNC_QUEUE.send({
  topic: "sync",
  webhook_event_id: webhookEventId || `inventory_items/update:${inventoryItemId}:${now}`,
  pim_pid: row.pim_pid,
  shopify_product_id: String(row.shopify_product_id),
  received_at: now,
});


  return new Response("OK", { status: 200 });
}

// === PRODUCT DELETE WEBHOOK ===

      if (topic === "products/delete") {
      const now = new Date().toISOString();
    
      // pim_pid ophalen (BESTAAT hier nog niet)
      const productRow = await env.DB.prepare(`
        SELECT pim_pid
        FROM products
        WHERE shopify_product_id = ?1
        LIMIT 1
      `).bind(String(shopifyProductId)).first();
    
      const pim_pid = productRow?.pim_pid;
    
      // product soft delete
      await env.DB.prepare(`
        UPDATE products
        SET deleted_at = ?1
        WHERE shopify_product_id = ?2
      `)
        .bind(now, String(shopifyProductId))
        .run();
    
      // variants soft delete
      if (pim_pid) {
        await env.DB.prepare(`
          UPDATE variants
          SET deleted_at = ?1
          WHERE pim_pid = ?2
        `)
          .bind(now, pim_pid)
          .run();
      }
    
      // DELTA delete (NU met geldige pim_pid)
      if (pim_pid && webhookEventId) {
        await env.DB.prepare(`
          INSERT OR IGNORE INTO delta_products (
            pim_pid,
            shopify_product_id,
            webhook_event_id,
            updated_at,
            deleted_at
          ) VALUES (?1, ?2, ?3, ?4, ?5)
        `)
          .bind(
            pim_pid,
            String(shopifyProductId),
            webhookEventId,
            now,
            now
          )
          .run();
    
        await env.DB.prepare(`
          INSERT OR IGNORE INTO delta_variants (
            pim_pid,
            shopify_variant_id,
            webhook_event_id,
            updated_at,
            deleted_at
          )
          SELECT
            pim_pid,
            shopify_variant_id,
            ?1,
            ?2,
            ?3
          FROM variants
          WHERE pim_pid = ?4
        `)
          .bind(webhookEventId, now, now, pim_pid)
          .run();
      }
    
      // queue delete event
      await env.PULL_QUEUE.send({
        topic: "products/delete",
        shopify_product_id: String(shopifyProductId),
        received_at: now,
      });
    
      return new Response("OK", { status: 200 });
    }

    // 1) Product garanderen (products heeft wél updated_at)
    await env.DB.prepare(`
      INSERT OR IGNORE INTO products (
        shopify_product_id,
        created_at,
        updated_at,
        deleted_at
      ) VALUES (?1, ?2, ?3, ?4)
    `)
      .bind(String(shopifyProductId), now, now, notDeleted)
      .run();

    await env.DB.prepare(`
      UPDATE products
      SET updated_at = ?1, deleted_at = ?2
      WHERE shopify_product_id = ?3
    `)
      .bind(now, notDeleted, String(shopifyProductId))
      .run();

    const productRow = await env.DB.prepare(`
      SELECT pim_pid
      FROM products
      WHERE shopify_product_id = ?1
      LIMIT 1
    `)
      .bind(String(shopifyProductId))
      .first();

    if (!productRow?.pim_pid) {
      return new Response("DB error", { status: 500 });
    }

    const pim_pid = productRow.pim_pid;

for (const opt of payload.options ?? []) {
  // DELTA product_options (before writing product_options)
  if (webhookEventId) {
    const optionId = String(opt.id);

    const prev = await env.DB.prepare(`
      SELECT name, position
      FROM product_options
      WHERE pim_pid = ?1 AND shopify_option_id = ?2
      LIMIT 1
    `).bind(pim_pid, optionId).first();

    const incomingName = opt.name ?? "";
    const incomingPos = opt.position ?? 0;

    const changed = {};
    if (!prev) {
      changed.name = incomingName;
      changed.position = incomingPos;
    } else {
      if ((prev.name ?? "") !== incomingName) changed.name = incomingName;
      if ((prev.position ?? 0) !== incomingPos) changed.position = incomingPos;
    }

    if (Object.keys(changed).length) {
      const cols = ["pim_pid", "shopify_option_id", "webhook_event_id", ...Object.keys(changed)];
      const placeholders = cols.map((_, i) => `?${i + 1}`).join(", ");
      const sql = `INSERT OR IGNORE INTO delta_product_options (${cols.join(", ")}) VALUES (${placeholders})`;

      await env.DB.prepare(sql)
        .bind(pim_pid, optionId, webhookEventId, ...Object.values(changed))
        .run();
    }
  }


  await env.DB.prepare(`
  INSERT INTO product_options (
    pim_pid,
    shopify_option_id,
    name,
    position
  ) VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT(pim_pid, shopify_option_id) DO UPDATE SET
    name = excluded.name,
    position = excluded.position
`)
  .bind(
    pim_pid,
    String(opt.id),
    opt.name ?? "",
    opt.position ?? 0
  )
  .run();





  for (const val of opt.values ?? []) {

// DELTA product_option_values (only when new value appears) 
if (webhookEventId) {
  const exists = await env.DB.prepare(`
    SELECT 1
    FROM product_option_values
    WHERE pim_pid = ?1 AND shopify_option_id = ?2 AND value = ?3
    LIMIT 1
  `).bind(pim_pid, String(opt.id), val).first();

  if (!exists) {
    await env.DB.prepare(`
      INSERT OR IGNORE INTO delta_product_option_values (
        pim_pid,
        shopify_option_id,
        value,
        webhook_event_id
      ) VALUES (?1, ?2, ?3, ?4)
    `)
      .bind(pim_pid, String(opt.id), val, webhookEventId)
      .run();
  }
}

// jan15C
// REMOVALS product_option_values (soft delete values verdwenen uit Shopify)
if (webhookEventId) {
  const optionId = String(opt.id);
  const incomingVals = new Set((opt.values ?? []).map(v => String(v)));

  // Alleen actieve (deleted_at = '') ophalen
  const { results } = await env.DB.prepare(`
    SELECT value
    FROM product_option_values
    WHERE pim_pid = ?1
      AND shopify_option_id = ?2
      AND (deleted_at IS NULL OR deleted_at = '')
  `).bind(pim_pid, optionId).all();

  const removed = (results ?? [])
    .map(r => String(r.value))
    .filter(v => !incomingVals.has(v));

  for (const removedVal of removed) {
    // delta tombstone (PK bevat webhook_event_id)
    await env.DB.prepare(`
      INSERT OR IGNORE INTO delta_product_option_values (
        pim_pid,
        shopify_option_id,
        value,
        webhook_event_id,
        deleted_at
      ) VALUES (?1, ?2, ?3, ?4, ?5)
     `).bind(pim_pid, optionId, removedVal, webhookEventId, now).run();


    // soft delete in live table
    await env.DB.prepare(`
      UPDATE product_option_values
      SET deleted_at = ?1
      WHERE pim_pid = ?2
        AND shopify_option_id = ?3
        AND value = ?4
        AND (deleted_at IS NULL OR deleted_at = '')
    `).bind(now, pim_pid, optionId, removedVal).run();
  }
}

//einde jan15C


    await env.DB.prepare(`
      INSERT OR IGNORE INTO product_option_values (
        pim_pid,
        shopify_option_id,
        value
      ) VALUES (?1, ?2, ?3)
    `)
      .bind(
        pim_pid,
        String(opt.id),
        val
      )
      .run();
  }
}

// --- DELTA products (incl images/media) BEFORE updating products/images ---
if (webhookEventId) {
  const normArr = (arr) =>
  Array.isArray(arr) ? arr : [];
  

  const prev = await env.DB.prepare(`
    SELECT
      title, body_html, handle, vendor, product_type,
      tags, template_suffix, status, published_scope, published_at,
      images_json, media_json, metafields_json
    FROM products
    WHERE pim_pid = ?1
    LIMIT 1
  `).bind(pim_pid).first();

  const incoming = {
    title: payload.title ?? "",
    body_html: payload.body_html ?? "",
    handle: payload.handle ?? "",
    vendor: payload.vendor ?? "",
    product_type: payload.product_type ?? "",
    tags: payload.tags ?? "",
    template_suffix: payload.template_suffix ?? "",
    status: payload.status ?? "",
    published_scope: payload.published_scope ?? "",
    published_at: payload.published_at ?? "",
    images_json: JSON.stringify(normArr(payload.images)),
    media_json: JSON.stringify(normArr(payload.media)),
    metafields_json: prev?.metafields_json ?? ""
  };

  const changed = {};
  for (const k in incoming) {
    if ((prev?.[k] ?? "") !== incoming[k]) {
      changed[k] = incoming[k];
    }
  }

  if (Object.keys(changed).length) {
    const cols = [
      "pim_pid",
      "shopify_product_id",
      "webhook_event_id",
      "updated_at",
      ...Object.keys(changed),
    ];
    const placeholders = cols.map((_, i) => `?${i + 1}`).join(", ");

    await env.DB.prepare(`
      INSERT OR IGNORE INTO delta_products (${cols.join(", ")})
      VALUES (${placeholders})
    `)
      .bind(
        pim_pid,
        String(shopifyProductId),
        webhookEventId,
        now,
        ...Object.values(changed)
      )
      .run();
  }
}
// --- END DELTA products ---


    //16jan A
  //   await env.DB.prepare(`
  //   UPDATE products
  //   SET
  //     images_json = ?1,
  //     media_json  = ?2,
  //     updated_at  = ?3
  //   WHERE pim_pid = ?4
  // `)
  // .bind(
  //   JSON.stringify(payload.images ?? []),
  //   JSON.stringify(payload.media ?? []),
  //   now,
  //   pim_pid
  // )
  // .run();
  
  const normArr = (arr) =>
  Array.isArray(arr) ? arr : [];
  

await env.DB.prepare(`
  UPDATE products
  SET
    images_json = ?1,
    media_json  = ?2,
    updated_at  = ?3
  WHERE pim_pid = ?4
`)
.bind(
  JSON.stringify(normArr(payload.images)),
  JSON.stringify(normArr(payload.media)),
  now,
  pim_pid
)
.run();


  //einde 16jan A
  
    await env.DB.prepare(`
  UPDATE products
  SET
    admin_graphql_api_id = ?1,
    title               = ?2,
    body_html           = ?3,
    handle              = ?4,
    vendor              = ?5,
    product_type        = ?6,
    tags                = ?7,
    template_suffix     = ?8,
    status              = ?9,
    published_scope     = ?10,
    published_at        = ?11,
    updated_at          = ?12
  WHERE pim_pid = ?13
`)
.bind(
  payload.admin_graphql_api_id ?? "",
  payload.title ?? "",
  payload.body_html ?? "",
  payload.handle ?? "",
  payload.vendor ?? "",
  payload.product_type ?? "",
  payload.tags ?? "",
  payload.template_suffix ?? "",
  payload.status ?? "",
  payload.published_scope ?? "",
  payload.published_at ?? "",
  now,
  pim_pid
)
.run();


    // 2) Variants ONLY insert (geen update, geen updated_at)
    for (const v of payload.variants ?? []) {
      const vid = v?.id;
//test za 17jan of shopify inventory meestuurt
// if (String(vid) === "60872219165002") {
//   console.log("INV_DEBUG", {
//     topic,
//     shopify_product_id: String(shopifyProductId),
//     shopify_variant_id: String(vid),
//     v_inventory_quantity: v?.inventory_quantity,
//     v_updated_at: v?.updated_at,
//     webhook_event_id: webhookEventId,
//   });
// }
// inventory wordt wel degelijk meegestuurd, duurde effe.

//einde test za 17 jan
      const variantUpdatedAt =
      variantUpdateMap.get(String(vid)) ||
      v?.updated_at ||
      now;
    
    if (!vid) continue;
    
      // ensure variant exists
      await env.DB.prepare(`
        INSERT OR IGNORE INTO variants (
          pim_pid,
          shopify_variant_id,
          created_at
        ) VALUES (?1, ?2, ?3)
      `)
        .bind(pim_pid, String(vid), variantUpdatedAt)
        .run();
    
      // update ONLY if Shopify variant actually changed
      await env.DB.prepare(`
  UPDATE variants
  SET updated_at = ?1
  WHERE pim_pid = ?2
    AND shopify_variant_id = ?3
    AND (updated_at IS NULL OR updated_at < ?4)
`)
.bind(
  variantUpdatedAt, // ?1
  pim_pid,          // ?2
  String(vid),      // ?3
  variantUpdatedAt  // ?4
)
.run();


// --- DELTA for variants (before updating variants table) ---
if (webhookEventId) {
  const prev = await env.DB.prepare(`
    SELECT
      admin_graphql_api_id,
      title,
      sku,
      barcode,
      price,
      compare_at_price,
      option1,
      option2,
      option3,
      position,
      inventory_item_id,
      inventory_policy,
      inventory_quantity,
      taxable
    FROM variants
    WHERE pim_pid = ?1 AND shopify_variant_id = ?2
    LIMIT 1
  `).bind(pim_pid, String(vid)).first();


  // was goed:
  // const norm = (x) => (x === null || x === undefined ? "" : String(x));
  // const normInt = (x) => (x === null || x === undefined ? 0 : Number(x));

  // const incoming = {
  //   admin_graphql_api_id: norm(v.admin_graphql_api_id),
  //   title: norm(v.title),
  //   sku: norm(v.sku),
  //   barcode: norm(v.barcode),
  //   price: norm(v.price),
  //   compare_at_price: norm(v.compare_at_price),
  //   option1: norm(v.option1),
  //   option2: norm(v.option2),
  //   option3: norm(v.option3),
  //   position: normInt(v.position ?? 0),
  //   inventory_item_id: norm(v.inventory_item_id),
  //   inventory_policy: norm(v.inventory_policy),
  //   inventory_quantity: normInt(v.inventory_quantity ?? 0),
  //   taxable: v.taxable ? 1 : 0,
  // };

  // const current = prev
  //   ? {
  //       admin_graphql_api_id: norm(prev.admin_graphql_api_id),
  //       title: norm(prev.title),
  //       sku: norm(prev.sku),
  //       barcode: norm(prev.barcode),
  //       price: norm(prev.price),
  //       compare_at_price: norm(prev.compare_at_price),
  //       option1: norm(prev.option1),
  //       option2: norm(prev.option2),
  //       option3: norm(prev.option3),
  //       position: normInt(prev.position),
  //       inventory_item_id: norm(prev.inventory_item_id),
  //       inventory_policy: norm(prev.inventory_policy),
  //       inventory_quantity: normInt(prev.inventory_quantity),
  //       taxable: normInt(prev.taxable),
  //     }
  //   : null;

//za 17jan C dit stuk nodig vanwege voorraad mutaties: delta_variants niet bijwerken alleen bij meerdere gewijzigde velden
// alleen bij een inventory_quantity change:
const norm = (x) => (x === null || x === undefined ? "" : String(x));
const normInt = (x) => (x === null || x === undefined ? 0 : Number(x));

// normalize current eerst
const current = prev
  ? {
      admin_graphql_api_id: norm(prev.admin_graphql_api_id),
      title: norm(prev.title),
      sku: norm(prev.sku),
      barcode: norm(prev.barcode),
      price: norm(prev.price),
      compare_at_price: norm(prev.compare_at_price),
      option1: norm(prev.option1),
      option2: norm(prev.option2),
      option3: norm(prev.option3),
      position: normInt(prev.position),
      inventory_item_id: norm(prev.inventory_item_id),
      inventory_policy: norm(prev.inventory_policy),
      inventory_quantity: normInt(prev.inventory_quantity),
      taxable: normInt(prev.taxable),
    }
  : null;

// helper: als Shopify het veld niet meestuurt -> fallback naar current
const pickStr = (val, fallback) => (val === undefined || val === null ? fallback : String(val));
const pickInt = (val, fallback) => (val === undefined || val === null ? fallback : Number(val));

// incoming met fallback naar current (voorkomt false positives)
const incoming = {
  admin_graphql_api_id: pickStr(v.admin_graphql_api_id, current?.admin_graphql_api_id ?? ""),
  title: pickStr(v.title, current?.title ?? ""),
  sku: pickStr(v.sku, current?.sku ?? ""),
  barcode: pickStr(v.barcode, current?.barcode ?? ""),
  price: pickStr(v.price, current?.price ?? ""),
  compare_at_price: pickStr(v.compare_at_price, current?.compare_at_price ?? ""),
  option1: pickStr(v.option1, current?.option1 ?? ""),
  option2: pickStr(v.option2, current?.option2 ?? ""),
  option3: pickStr(v.option3, current?.option3 ?? ""),
  position: pickInt(v.position, current?.position ?? 0),
  inventory_item_id: pickStr(v.inventory_item_id, current?.inventory_item_id ?? ""),
  inventory_policy: pickStr(v.inventory_policy, current?.inventory_policy ?? ""),
  inventory_quantity: pickInt(v.inventory_quantity, current?.inventory_quantity ?? 0),
  taxable: (v.taxable === undefined || v.taxable === null) ? (current?.taxable ?? 0) : (v.taxable ? 1 : 0),
};

// diff
const changed = {};
if (!current) {
  Object.assign(changed, incoming);
} else {
  for (const k of Object.keys(incoming)) {
    if (incoming[k] !== current[k]) changed[k] = incoming[k];
  }
}



//einde za 17 jan C

  // if variant didn't exist yet -> store full snapshot, else only changed fields
  // const changed = {};
  // if (!current) {
  //   Object.assign(changed, incoming);
  // } else {
  //   for (const k of Object.keys(incoming)) {
  //     if (incoming[k] !== current[k]) changed[k] = incoming[k];
  //   }
  // }
// 17jan B:
  // delta_variants géén inventory-only events, maar de gewone variants tabel blijft wel alles updaten.
  // en bij mixed updates inventory_quantity uit de delta halen (maar variants blijft wél updaten):
// Ignore inventory_quantity in delta:
// - if it's the only change -> no delta row
// - if there are other changes -> drop inventory_quantity from delta payload
// let skipDelta = false;

// if (current && Object.prototype.hasOwnProperty.call(changed, "inventory_quantity")) {
//   if (Object.keys(changed).length === 1) {
//     // inventory-only event -> geen delta, maar variants update moet wél doorgaan
//     skipDelta = true;
//   } else {
//     // mixed change -> inventory_quantity niet in delta
//     delete changed.inventory_quantity;
//   }
// }

// if (!skipDelta && Object.keys(changed).length) {
//   // jouw bestaande INSERT OR IGNORE INTO delta_variants ...
// }
// einde 17jan B

//   if (Object.keys(changed).length) {
//     const cols = ["pim_pid", "shopify_variant_id", "webhook_event_id", "updated_at", ...Object.keys(changed)];
//     const placeholders = cols.map((_, i) => `?${i + 1}`).join(", ");
//     const sql = `INSERT OR IGNORE INTO delta_variants (${cols.join(", ")}) VALUES (${placeholders})`;

//     await env.DB.prepare(sql)
//       .bind(
//         pim_pid,
//         String(vid),
//         webhookEventId,
//         variantUpdatedAt ?? now,
//         ...Object.values(changed)
//       )
//       .run();

// //16janB


// //einde 16janB
// // fill pim_vid on delta row (if column exists)
// await env.DB.prepare(`
//   UPDATE delta_variants
//   SET pim_vid = (
//     SELECT pim_vid
//     FROM variants
//     WHERE pim_pid = ?1 AND shopify_variant_id = ?2
//     LIMIT 1
//   )
//   WHERE pim_vid IS NULL
//     AND pim_pid = ?1
//     AND shopify_variant_id = ?2
//     AND webhook_event_id = ?3
// `)
//   .bind(pim_pid, String(vid), webhookEventId)
//   .run();


//   }

let skipDelta = false;

if (current && Object.prototype.hasOwnProperty.call(changed, "inventory_quantity")) {
  if (Object.keys(changed).length === 1) {
    skipDelta = true; // inventory-only -> geen delta
  } else {
    delete changed.inventory_quantity; // mixed -> inventory niet loggen
  }
}

if (!skipDelta && Object.keys(changed).length) {
  const cols = ["pim_pid", "shopify_variant_id", "webhook_event_id", "updated_at", ...Object.keys(changed)];
  const placeholders = cols.map((_, i) => `?${i + 1}`).join(", ");
  const sql = `INSERT OR IGNORE INTO delta_variants (${cols.join(", ")}) VALUES (${placeholders})`;

  await env.DB.prepare(sql)
    .bind(
      pim_pid,
      String(vid),
      webhookEventId,
      variantUpdatedAt ?? now,
      ...Object.values(changed)
    )
    .run();

  await env.DB.prepare(`
    UPDATE delta_variants
    SET pim_vid = (
      SELECT pim_vid
      FROM variants
      WHERE pim_pid = ?1 AND shopify_variant_id = ?2
      LIMIT 1
    )
    WHERE pim_vid IS NULL
      AND pim_pid = ?1
      AND shopify_variant_id = ?2
      AND webhook_event_id = ?3
  `)
    .bind(pim_pid, String(vid), webhookEventId)
    .run();
}

}
// --- end DELTA for variants ---

await env.DB.prepare(`
  UPDATE variants
  SET
    admin_graphql_api_id = ?1,
    title               = ?2,
    sku                 = ?3,
    barcode             = ?4,
    price               = ?5,
    compare_at_price    = ?6,
    option1             = ?7,
    option2             = ?8,
    option3             = ?9,
    position            = ?10,
    inventory_item_id   = ?11,
    inventory_policy    = ?12,
    inventory_quantity  = ?13,
    taxable             = ?14
  WHERE pim_pid = ?15
    AND shopify_variant_id = ?16
`)
.bind(
  v.admin_graphql_api_id ?? "",
  v.title ?? "",
  v.sku ?? "",
  v.barcode ?? "",
  v.price ?? "",
  v.compare_at_price ?? "",
  v.option1 ?? "",
  v.option2 ?? "",
  v.option3 ?? "",
  v.position ?? 0,
  String(v.inventory_item_id ?? ""),
  v.inventory_policy ?? "",
  v.inventory_quantity ?? 0,
  v.taxable ? 1 : 0,
  pim_pid,
  String(v.id)
)
.run();


// set deleted_at voor verwijderde varianten
const activeVariantIds = new Set(
  (payload.variants ?? []).map(v => String(v.id))
);

await env.DB.prepare(`
  UPDATE variants
  SET deleted_at = ?1
  WHERE pim_pid = ?2
    AND shopify_variant_id NOT IN (${[...activeVariantIds].map(() => '?').join(',')})
    AND (deleted_at IS NULL OR deleted_at = '')
`)
.bind(
  now,
  pim_pid,
  ...activeVariantIds
)
.run();

//15jan B
// DELTA variant removals (tombstones) : update ook het deleted_at veld in delta_variants
// set deleted_at + delta voor verwijderde varianten (ook als activeVariantIds leeg is)
const activeVariantIdsArr = (payload.variants ?? []).map(v => String(v.id)).filter(Boolean);

if (activeVariantIdsArr.length === 0) {
  // alles is weg -> markeer alle varianten deleted
  await env.DB.prepare(`
    UPDATE variants
    SET deleted_at = ?1
    WHERE pim_pid = ?2
      AND (deleted_at IS NULL OR deleted_at = '')
  `).bind(now, pim_pid).run();

  // delta tombstones voor alle varianten
  if (webhookEventId) {
    await env.DB.prepare(`
      INSERT OR IGNORE INTO delta_variants (
        pim_pid, shopify_variant_id, webhook_event_id, updated_at, deleted_at
      )
      SELECT pim_pid, shopify_variant_id, ?1, ?2, ?3
      FROM variants
      WHERE pim_pid = ?4
        AND deleted_at = ?3
    `).bind(webhookEventId, now, now, pim_pid).run();
  }
} else {
  // alleen varianten die niet meer in payload zitten
  const placeholders = activeVariantIdsArr.map(() => "?").join(",");

  await env.DB.prepare(`
    UPDATE variants
    SET deleted_at = ?1
    WHERE pim_pid = ?2
      AND shopify_variant_id NOT IN (${placeholders})
      AND (deleted_at IS NULL OR deleted_at = '')
  `).bind(now, pim_pid, ...activeVariantIdsArr).run();

  if (webhookEventId) {
    await env.DB.prepare(`
      INSERT OR IGNORE INTO delta_variants (
        pim_pid, shopify_variant_id, webhook_event_id, updated_at, deleted_at
      )
      SELECT pim_pid, shopify_variant_id, ?1, ?2, ?3
      FROM variants
      WHERE pim_pid = ?4
        AND shopify_variant_id NOT IN (${placeholders})
        AND deleted_at = ?3
    `).bind(webhookEventId, now, now, pim_pid, ...activeVariantIdsArr).run();
  }
}


//einde 15jan b

    }
    



    // 3a) Queue voor Meta pull worker
    await env.PULL_QUEUE.send({
      pim_pid,
      shopify_product_id: String(shopifyProductId),
      shopify_variant_ids: variantIds.map(String),
      received_at: now,
      webhook_event_id: webhookEventId, 
    });

    // 3b) Sync trigger (nieuw)
    await env.SYNC_QUEUE.send({
      topic: "sync",
      webhook_event_id: webhookEventId,
      pim_pid,
      shopify_product_id: String(shopifyProductId),
      received_at: now,
    });

    return new Response("OK", { status: 200 });
  },
};
