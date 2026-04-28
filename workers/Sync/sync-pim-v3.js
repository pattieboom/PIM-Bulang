// Worker: sync-pim-v3
// Bindings:
// - DB (D1)
// - SYNC_QUEUE
// - TARGET_NL / TARGET_DE / TARGET_EU / TARGET_INT / TARGET_USA / TARGET_TEST
// - TOKEN_NL  / TOKEN_DE  / TOKEN_EU  / TOKEN_INT  / TOKEN_USA  / TOKEN_TEST

export default {
  async fetch() {
    return new Response("OK");
  },

  async queue(batch, env) {
// ==============================
// SHOPIFY THROTTLE PROTECTION
// ==============================

let throttleState = {
  currentlyAvailable: 1000,
  restoreRate: 50
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const throttleGuard = async (cost = 50) => {
  const safetyBuffer = 100;

  if (throttleState.currentlyAvailable - cost < safetyBuffer) {
    const deficit = safetyBuffer - (throttleState.currentlyAvailable - cost);
    const waitMs = Math.ceil((deficit / throttleState.restoreRate) * 1000);

    console.log("SYNC_THROTTLE_WAIT", {
      currentlyAvailable: throttleState.currentlyAvailable,
      restoreRate: throttleState.restoreRate,
      waiting_ms: waitMs
    });

    await sleep(waitMs);
  }
};

const gql = async (shop, token, query, variables, retry = 0) => {

  await throttleGuard(50);

  const r = await fetch(`https://${shop}/admin/api/2025-01/graphql.json`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token,
    },
    body: JSON.stringify({ query, variables }),
  });

  // 429 protection (extra veiligheid)
  if (r.status === 429 && retry < 3) {
    const wait = 1000 * (retry + 1);
    console.log("SYNC_429_RETRY", { wait });
    await sleep(wait);
    return gql(shop, token, query, variables, retry + 1);
  }

  const j = await r.json();

  if (!r.ok || j.errors) {
    console.log("SYNC_GQL_ERROR", j.errors);
    throw j.errors || j;
  }

  // update throttle state vanuit Shopify response
  const throttle = j?.extensions?.cost?.throttleStatus;
  if (throttle) {
    throttleState.currentlyAvailable = throttle.currentlyAvailable;
    throttleState.restoreRate = throttle.restoreRate;
  }

  return j.data;
};


    const canAutoCreate = async (gql, shop, token, pim_pid) => {
      const row = await env.DB.prepare(`
        SELECT shopify_product_id
        FROM products
        WHERE pim_pid = ?1
        LIMIT 1
      `).bind(pim_pid).first();
    
      if (!row?.shopify_product_id) return false;
    
      const data = await gql(
        shop,
        token,
        `query ($id: ID!) {
          product(id: $id) {
            metafield(namespace: "custom", key: "create") {
              value
            }
          }
        }`,
        { id: `gid://shopify/Product/${row.shopify_product_id}` }
      );
    
      const val = (data?.product?.metafield?.value ?? "").toLowerCase().trim();
      return val === "true" || val === "1" || val === "yes";
    };
    

    const ensureProductLink = async (env, gql, pim_pid, target_store, target_token) => {
      // 1) master handle ophalen
      const prod = await env.DB.prepare(`
      SELECT
      handle,
      title,
      body_html,
      vendor,
      product_type,
      tags,
      status,
      template_suffix
    FROM products
    WHERE pim_pid = ?1
    LIMIT 1
    
      `).bind(pim_pid).first();
    
      const handle = prod?.handle;
      if (!handle) return null;
    
// 2) target product zoeken op handle
const data = await gql(
  target_store,
  target_token,
  `query ($q:String!){
    products(first:1, query:$q){
      nodes{ id }
    }
  }`,
  { q: `handle:${handle}` }
);

let gid = data?.products?.nodes?.[0]?.id;

// 2b) NIET gevonden → product aanmaken
if (!gid) {
  const created = await gql(
    target_store,
    target_token,
    `mutation ($input: ProductInput!) {
      productCreate(input: $input) {
        product { id }
        userErrors { field message }
      }
    }`,
    {
      input: {
        handle,
        title: prod?.title || handle,
        descriptionHtml: prod?.body_html || undefined,
        vendor: prod?.vendor || undefined,
        productType: prod?.product_type || undefined,
        tags: prod?.tags || undefined,
        status: prod?.status ? prod.status.toUpperCase() : "DRAFT",
        templateSuffix: prod?.template_suffix || undefined
      }
      
    }
  );

  gid = created?.productCreate?.product?.id;
  if (!gid) return null;
}

    
      const targetPid = String(gid).split("/").pop();
    
      // 3) link opslaan
      await env.DB.prepare(`
        INSERT OR REPLACE INTO product_links
        (pim_pid, target_store, shopify_product_id, created_at, updated_at)
        VALUES (?1, ?2, ?3, datetime('now'), datetime('now'))
      `).bind(pim_pid, target_store, targetPid).run();
    
      return targetPid;
    };
    
    const ensureVariantLink = async (env, gql, pim_vid, pim_pid, target_store, target_token) => {
      // 1. Bestaat link al?
      const existing = await env.DB.prepare(`
        SELECT shopify_variant_id
        FROM variant_links
        WHERE pim_vid = ?1 AND target_store = ?2
        LIMIT 1
      `).bind(pim_vid, target_store).first();
    
      if (existing?.shopify_variant_id) {
        return existing.shopify_variant_id;
      }
    
      // 2. Master variant index bepalen (ORDER BY position, pim_vid)
      const masterVariants = await env.DB.prepare(`
        SELECT pim_vid
        FROM variants
        WHERE pim_pid = ?1
        ORDER BY position ASC, pim_vid ASC
      `).bind(pim_pid).all();
    
      const index = masterVariants.results
        .map(v => v.pim_vid)
        .indexOf(pim_vid);
    
      if (index === -1) return null;
    
      // 3. Target product id ophalen
      const pl = await env.DB.prepare(`
        SELECT shopify_product_id
        FROM product_links
        WHERE pim_pid = ?1 AND target_store = ?2
        LIMIT 1
      `).bind(pim_pid, target_store).first();
    
      if (!pl?.shopify_product_id) return null;
    
      const targetPid = pl.shopify_product_id;
    
      // 4. Target variants ophalen (in vaste volgorde)
      const data = await gql(
        target_store,
        target_token,
        `query ($id: ID!) {
          product(id: $id) {
            variants(first: 100) {
              nodes { id }
            }
          }
        }`,
        { id: `gid://shopify/Product/${targetPid}` }
      );
    
      const targetVariants = data?.product?.variants?.nodes ?? [];
    
      // 5. Bestaat target variant op dezelfde index?
      const targetNode = targetVariants[index];
      if (!targetNode?.id) return null;
    
      const targetVid = targetNode.id.split("/").pop();
    
      // 6. Link vastleggen
      await env.DB.prepare(`
        INSERT OR REPLACE INTO variant_links
        (pim_vid, target_store, shopify_variant_id)
        VALUES (?1, ?2, ?3)
      `).bind(pim_vid, target_store, targetVid).run();
    
      return targetVid;
    };
    

    const targets = [
      { store: env.TARGET_NL, token: env.TOKEN_NL },
      { store: env.TARGET_DE, token: env.TOKEN_DE },
      { store: env.TARGET_EU, token: env.TOKEN_EU },
      { store: env.TARGET_INT, token: env.TOKEN_INT },
      { store: env.TARGET_USA, token: env.TOKEN_USA },
      { store: env.TARGET_TEST, token: env.TOKEN_TEST },
    ].filter(t => t.store && t.token);

    for (const msg of batch.messages) {
      const { pim_pid } = msg.body || {};
      console.log("SYNC_START", {
        pim_pid,
        body: msg.body
      });
      
      if (!pim_pid) {
        msg.ack();
        continue;
      }

      try {
        for (const t of targets) {
          const state =
            (await env.DB.prepare(
              `SELECT last_delta_product_id,last_delta_variant_id
               FROM sync_state WHERE target_store=?1`
            ).bind(t.store).first()) || {
              last_delta_product_id: 0,
              last_delta_variant_id: 0,
            };

            console.log("SYNC_STATE", {
              target: t.store,
              last_delta_product_id: state.last_delta_product_id,
              last_delta_variant_id: state.last_delta_variant_id
            });

            // TRACKERS VOOR DEZE TARGET STORE
let maxProductDeltaId = state.last_delta_product_id;
let maxVariantDeltaId = state.last_delta_variant_id;

            

            const products = await env.DB.prepare(
              `SELECT dp.*, pl.shopify_product_id AS target_pid
               FROM delta_products dp
               LEFT JOIN product_links pl
                 ON pl.pim_pid = dp.pim_pid
                AND pl.target_store = ?2
               WHERE dp.pim_pid = ?1
                 AND dp.delta_id > ?3
               ORDER BY dp.delta_id ASC`
            )
              .bind(
                pim_pid,
                t.store,
                state.last_delta_product_id
              )
              .all();

            // === DELTA METAFIELDS: neem per product alleen de LAATSTE snapshot ===
const latestMetafieldDeltaByProduct = new Map();

for (const p of products.results ?? []) {
  if (p.metafields_json !== undefined) {
    latestMetafieldDeltaByProduct.set(p.pim_pid, p);
  }
}
  
            

              const variants = await env.DB.prepare(`
              SELECT
                dv.*,
                COALESCE(dv.pim_vid, v.pim_vid) AS pim_vid,
                vl.shopify_variant_id AS target_vid
              FROM delta_variants dv
              LEFT JOIN variants v
                ON v.pim_pid = dv.pim_pid
               AND v.shopify_variant_id = dv.shopify_variant_id
              LEFT JOIN variant_links vl
                ON vl.pim_vid = COALESCE(dv.pim_vid, v.pim_vid)
               AND vl.target_store = ?2
              WHERE dv.pim_pid = ?1
                AND dv.delta_id > ?3
              ORDER BY dv.delta_id ASC
            `)
            .bind(pim_pid, t.store, state.last_delta_variant_id)
            .all();
            

            // target product id ophalen (voor variant price sync)
let pl = await env.DB.prepare(`
SELECT shopify_product_id
FROM product_links
WHERE pim_pid = ?1 AND target_store = ?2
LIMIT 1
`).bind(pim_pid, t.store).first();

let targetPid = pl?.shopify_product_id;

// extra check: bestaat product nog in Shopify?
if (targetPid) {
  try {
    const exists = await gql(
      t.store,
      t.token,
      `query ($id: ID!) { product(id: $id) { id } }`,
      { id: `gid://shopify/Product/${targetPid}` }
    );

    // Shopify kan "product: null" teruggeven zonder errors -> behandel als niet-bestaand
    if (!exists?.product?.id) targetPid = null;

  } catch {
    // echte GraphQL errors / network -> ook ongeldig
    targetPid = null;
  }
}


if (!targetPid) {

  const allowed = await canAutoCreate(gql, env.SHOPIFY_SHOP_DOMAIN, env.SHOPIFY_ADMIN_TOKEN, pim_pid);

  if (!allowed) {
    console.log("AUTO_CREATE_BLOCKED_NO_FLAG", {
      pim_pid,
      target_store: t.store
    });
    continue;
  }

  const created = await ensureProductLink(env, gql, pim_pid, t.store, t.token);
  if (!created) {
    console.log("AUTO_CREATE_FAILED", {
      pim_pid,
      target_store: t.store
    });
    continue;
  }

  targetPid = created;

// === INIT PRODUCT METAFIELDS (FILTERED) ===
const mfRow = await env.DB.prepare(`
  SELECT metafields_json
  FROM products
  WHERE pim_pid = ?1
  LIMIT 1
`).bind(pim_pid).first();

const metafields = JSON.parse(mfRow?.metafields_json || "[]");

if (metafields.length) {

  // 1) haal metafield definitions op uit target store
  const defRes = await gql(
    t.store,
    t.token,
    `query {
      metafieldDefinitions(ownerType: PRODUCT, first: 250) {
        nodes {
          namespace
          key
          type { name }
        }
      }
    }`
  );

  const allowed = new Set(
    (defRes?.metafieldDefinitions?.nodes ?? []).map(
      d => `${d.namespace}:${d.key}:${d.type.name}`
    )
  );

  // 2) filter master metafields
  const filtered = metafields.filter(m =>
    allowed.has(`${m.namespace}:${m.key}:${m.type}`)
  );

  // 3) alleen geldige metafields zetten
  if (filtered.length) {
    await gql(
      t.store,
      t.token,
      `mutation ($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          userErrors { field message }
        }
      }`,
      {
        metafields: filtered.map(m => ({
          ownerId: `gid://shopify/Product/${targetPid}`,
          namespace: m.namespace,
          key: m.key,
          type: m.type,
          value: m.value,
        })),
      }
    );
  }
}



  // === INIT PRODUCT IMAGES IN TARGET STORE (CREATE PATH) ===
const masterImagesRow = await env.DB.prepare(`
SELECT images_json
FROM products
WHERE pim_pid = ?1
LIMIT 1
`).bind(pim_pid).first();

const masterImages = JSON.parse(masterImagesRow?.images_json || "[]");

// 1) Huidige target media ophalen
let targetMediaRes = await gql(
t.store,
t.token,
`query ($id: ID!) {
  product(id: $id) {
    media(first: 50) {
      nodes {
        ... on MediaImage {
          id
          image { url }
        }
      }
    }
  }
}`,
{ id: `gid://shopify/Product/${targetPid}` }
);

let targetMedia = targetMediaRes?.product?.media?.nodes ?? [];
// === IMAGE ADD LOGIC (STABIEL, SHOPIFY-SAFE) ===
// Voeg alleen images toe als target er minder heeft dan master
const existingCount = targetMedia.length;
const toAdd = masterImages.slice(existingCount);


if (toAdd.length) {
await gql(
  t.store,
  t.token,
  `mutation ($id: ID!, $media: [CreateMediaInput!]!) {
    productCreateMedia(productId: $id, media: $media) {
      media { id }
      userErrors { field message }
    }
  }`,
  {
    id: `gid://shopify/Product/${targetPid}`,
    media: toAdd.map(i => ({
      mediaContentType: "IMAGE",
      originalSource: i.src,
    })),
  }
);

// BELANGRIJK: opnieuw ophalen zodat nieuwe media IDs ook mee kunnen in reorder
targetMediaRes = await gql(
  t.store,
  t.token,
  `query ($id: ID!) {
    product(id: $id) {
      media(first: 50) {
        nodes {
          ... on MediaImage {
            id
            image { url }
          }
        }
      }
    }
  }`,
  { id: `gid://shopify/Product/${targetPid}` }
);
targetMedia = targetMediaRes?.product?.media?.nodes ?? [];
}

// 3) Reorder volgens master volgorde (CORRECT)
const masterOrder = masterImages
  .map(i => i.src)
  .filter(Boolean);

const orderedIds = masterOrder
  .map(src =>
    targetMedia.find(m => m.image?.url === src)?.id
  )
  .filter(Boolean);

if (orderedIds.length > 1) {
  const moves = orderedIds.map((mediaId, index) => ({
    id: mediaId,
    newPosition: String(index),
  }));

  await gql(
    t.store,
    t.token,
    `mutation ($id: ID!, $moves: [MoveInput!]!) {
      productReorderMedia(id: $id, moves: $moves) {
        userErrors { field message }
      }
    }`,
    {
      id: `gid://shopify/Product/${targetPid}`,
      moves,
    }
  );
}


// === INIT PRODUCT OPTIONS IN TARGET STORE ===
const masterOptions = await env.DB.prepare(`
  SELECT
    po.name,
    po.shopify_option_id,
    GROUP_CONCAT(pov.value, '||') AS option_values
  FROM product_options po
  JOIN product_option_values pov
    ON pov.pim_pid = po.pim_pid
   AND pov.shopify_option_id = po.shopify_option_id
  WHERE po.pim_pid = ?1
    AND (pov.deleted_at IS NULL OR pov.deleted_at = '')
  GROUP BY po.shopify_option_id, po.name
  ORDER BY po.position ASC
`).bind(pim_pid).all();

if (masterOptions.results?.length) {
  await gql(
    t.store,
    t.token,
    `mutation ($productId: ID!, $options: [OptionCreateInput!]!) {
      productOptionsCreate(
        productId: $productId,
        options: $options
      ) {
        userErrors { field message }
      }
    }`,
    {
      productId: `gid://shopify/Product/${targetPid}`,
      options: masterOptions.results.map(o => ({
        name: o.name,
        values: (o.option_values ?? "")
          .split("||")
          .filter(Boolean)
          .map(v => ({ name: v }))
      }))
    }
  );
}


// === INIT VARIANTS NA AUTO-CREATE ===

// 1) Opties (naam + volgorde)
const options = await env.DB.prepare(`
SELECT shopify_option_id, name, position
FROM product_options
WHERE pim_pid = ?1
ORDER BY position ASC
`).bind(pim_pid).all();

// 2) Optie-waarden (actief)
const values = await env.DB.prepare(`
SELECT shopify_option_id, value
FROM product_option_values
WHERE pim_pid = ?1
  AND (deleted_at IS NULL OR deleted_at = '')
`).bind(pim_pid).all();

// 3) Waarden groeperen per optie
const valuesByOption = {};
for (const v of values.results ?? []) {
if (!valuesByOption[v.shopify_option_id]) {
  valuesByOption[v.shopify_option_id] = [];
}
valuesByOption[v.shopify_option_id].push(v.value);
}

// 4) Cartesian product = variant-structuur
const cartesian = (arrs) =>
arrs.reduce(
  (a, b) => a.flatMap(x => b.map(y => x.concat([y]))),
  [[]]
);

const optionValueSets = cartesian(
options.results.map(o => valuesByOption[o.shopify_option_id] || [])
);

// 5) Variant structuur payload (ALLEEN optionValues)
const variantInputs = optionValueSets.map(valuesSet => ({
optionValues: valuesSet.map((val, i) => ({
  optionName: options.results[i].name,
  name: val
}))
}));

// === REMOVE SHOPIFY DEFAULT VARIANT (NA OPTIONS, VOOR VARIANTS) ===
const existingVariants = await gql(
  t.store,
  t.token,
  `query ($id: ID!) {
    product(id: $id) {
      variants(first: 10) {
        nodes { id }
      }
    }
  }`,
  { id: `gid://shopify/Product/${targetPid}` }
);

const nodes = existingVariants?.product?.variants?.nodes ?? [];

if (nodes.length === 1) {
  const defaultVariantId = nodes[0].id;

  const optRes = await gql(
    t.store,
    t.token,
    `mutation ($productId: ID!, $options: [OptionCreateInput!]!) {
      productOptionsCreate(
        productId: $productId,
        options: $options
      ) {
        userErrors { field message }
      }
    }`,
    {
      productId: `gid://shopify/Product/${targetPid}`,
      options: masterOptions.results.map(o => ({
        name: o.name,
        values: (o.option_values ?? "")
          .split("||")
          .filter(Boolean)
          .map(v => ({ name: v }))
      }))
    }
  );
  
  const optErrors = optRes?.productOptionsCreate?.userErrors ?? [];

  const fatalErrors = optErrors.filter(
    e => !String(e.message).includes("already exists")
  );
  
  if (fatalErrors.length) {
    console.log("INIT_PRODUCT_OPTIONS_FAILED", fatalErrors);
    continue;
  }
  
  
  
}


// 6) Variants aanmaken in Shopify
const { productVariantsBulkCreate } = await gql(
  t.store,
  t.token,
  `mutation ($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkCreate(
      productId: $productId,
      variants: $variants
    ) {
      productVariants { id }
      userErrors { field message }
    }
  }`,
  {
    productId: `gid://shopify/Product/${targetPid}`,
    variants: variantInputs.slice(1)
  }
);

// Log bulkCreate errors (maar ga door: we halen varianten daarna opnieuw op)
if (productVariantsBulkCreate?.userErrors?.length) {
  console.log("INIT_VARIANT_ERRORS", productVariantsBulkCreate.userErrors);
}

// Haal ALLE varianten op (incl. default) + selectedOptions voor mapping
const allTargetVariantsRes = await gql(
  t.store,
  t.token,
  `query ($id: ID!) {
    product(id: $id) {
      variants(first: 100) {
        nodes {
          id
          selectedOptions {
            name
            value
          }
        }
      }
    }
  }`,
  { id: `gid://shopify/Product/${targetPid}` }
);

const createdVariants = allTargetVariantsRes?.product?.variants?.nodes ?? [];



// 7) Master variants ophalen + indexeren op option value
const masterVariants = await env.DB.prepare(`
SELECT
  pim_vid,
  option1,
  sku,
  price,
  weight_value,
  weight_unit,
  hscode,
  country_of_origin,
  cost
FROM variants
WHERE pim_pid = ?1
`).bind(pim_pid).all();

// map: option value -> master variant
const masterByOptionValue = {};
for (const mv of masterVariants.results ?? []) {
  if (mv.option1) {
    masterByOptionValue[mv.option1] = mv;
  }
}

// 8) variant_links vastleggen + DIRECT data syncen
for (let i = 0; i < createdVariants.length; i++) {
  const targetVid = createdVariants[i].id.split("/").pop();

  const optionValue =
  createdVariants[i]?.selectedOptions?.[0]?.value;

let v = null;

// normale case: variant met option value
if (optionValue && masterByOptionValue[optionValue]) {
  v = masterByOptionValue[optionValue];
}

// fallback: default variant + exact 1 master variant
if (!v && masterVariants.results.length === 1) {
  v = masterVariants.results[0];
}

if (!v) continue;



// 8a) variant_links
await env.DB.prepare(`
  INSERT OR REPLACE INTO variant_links
  (pim_vid, target_store, shopify_variant_id)
  VALUES (?1, ?2, ?3)
`).bind(v.pim_vid, t.store, targetVid).run();

// 8b) inventory item ophalen
const inv = await gql(
  t.store,
  t.token,
  `query ($id: ID!) {
    productVariant(id: $id) {
      inventoryItem { id }
    }
  }`,
  { id: `gid://shopify/ProductVariant/${targetVid}` }
);

const inventoryItemId = inv?.productVariant?.inventoryItem?.id;
if (!inventoryItemId) continue;

// 8c) inventory attributes (sku / HS / weight / COO / cost)
const inventoryInput = {};

// cost (ALLEEN als gevuld, respecteert delta & API limits)
if (v.cost !== null && v.cost !== undefined) {
  inventoryInput.cost = Number(v.cost);
}

// SKU
if (v.sku !== null && v.sku !== undefined && v.sku !== "") {
  inventoryInput.sku = String(v.sku);
}

// HS code
if (v.hscode) {
  inventoryInput.harmonizedSystemCode = v.hscode;
}

// Country of origin
if (v.country_of_origin) {
  inventoryInput.countryCodeOfOrigin = v.country_of_origin;
}

// Weight
if (v.weight_value) {
  inventoryInput.measurement = {
    weight: {
      value: v.weight_value,
      unit: v.weight_unit || "KILOGRAMS",
    },
  };
}

// Alleen updaten als er iets te sturen is
if (Object.keys(inventoryInput).length > 0) {
  const invUp = await gql(
    t.store,
    t.token,
    `mutation ($id: ID!, $input: InventoryItemInput!) {
      inventoryItemUpdate(id: $id, input: $input) {
        inventoryItem { id unitCost { amount } }
        userErrors { field message }
      }
    }`,
    {
      id: inventoryItemId,
      input: inventoryInput,
    }
  );

  const errs = invUp?.inventoryItemUpdate?.userErrors ?? [];
  if (errs.length) {
    console.log("INIT_INVENTORYITEMUPDATE_USERERRORS", {
      target_store: t.store,
      pim_pid,
      targetVid,
      inventoryItemId,
      inventoryInput,
      userErrors: errs,
    });
  } else {
    console.log("INIT_INVENTORYITEMUPDATE_OK", {
      target_store: t.store,
      pim_pid,
      targetVid,
      inventoryItemId,
      sent_cost: inventoryInput.cost ?? null,
      returned_unitCost: invUp?.inventoryItemUpdate?.inventoryItem?.unitCost?.amount ?? null,
    });
  }
}



// 8d) prijs
await gql(
  t.store,
  t.token,
  `mutation ($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(
      productId: $productId,
      variants: $variants
    ) {
      productVariants { id }
    }
  }`,
  {
    productId: `gid://shopify/Product/${targetPid}`,
    variants: [
      {
        id: `gid://shopify/ProductVariant/${targetVid}`,
// INT ex-VAT behalve Watch
...(t.store === env.TARGET_INT &&
  String(prod?.product_type || "").toLowerCase() !== "watch" &&
  v.price
  ? { price: (Number(v.price) * (100 / 121)).toFixed(2) }
  : { price: v.price || undefined }),

...(t.store === env.TARGET_INT &&
  String(prod?.product_type || "").toLowerCase() !== "watch" &&
  v.compare_at_price
  ? { compareAtPrice: (Number(v.compare_at_price) * (100 / 121)).toFixed(2) }
  : { compareAtPrice: v.compare_at_price || undefined }),

barcode: v.barcode || undefined,

      },
    ],
  }
);
}
}               
          
for (const p of products.results ?? []) {
 if (!p.target_pid) {
  p.target_pid = await ensureProductLink(env, gql, pim_pid, t.store, t.token);
                if (!p.target_pid) continue;
              }
              

              await gql(
                t.store,
                t.token,
                `mutation ($input:ProductInput!){
                   productUpdate(input:$input){product{id}}}`,
                {
                  input: {
                    id: `gid://shopify/Product/${p.target_pid}`,
                    title: p.title || undefined,
                    descriptionHtml: p.body_html || undefined,
                    handle: p.handle || undefined,
                    vendor: p.vendor || undefined,
                    productType: p.product_type || undefined,
                    tags: p.tags || undefined,
                    status: p.status ? p.status.toUpperCase() : undefined,
                    templateSuffix: p.template_suffix || undefined,
                },
              }
            );

            

// === IMAGE SYNC GUARD ===
// Skip image-sync als product in DEZE run is aangemaakt (INIT-pad heeft images al gedaan)
if (!p.target_pid) {
  console.log("IMAGE_SYNC_SKIPPED_CREATE_PATH", {
    pim_pid,
    target_store: t.store,
    delta_id: p.delta_id
  });
} else {
   // ===== IMAGE SYNC (RE:contentReference[oaicite:4]{index=4}=
  // Master images ophalen (altijd uit products tabel; delta_products heeft defaults)
  const masterImagesRow = await env.DB.prepare(`
    SELECT images_json
    FROM products
    WHERE pim_pid = ?1
    LIMIT 1
  `).bind(pim_pid).first();

  const masterImages = JSON.parse(masterImagesRow?.images_json || "[]");

  // Target media ophalen
  let targetRes = await gql(
    t.store,
    t.token,
    `query ($id: ID!) {
      product(id: $id) {
        media(first: 50) {
          nodes {
            ... on MediaImage {
              id
              image { url }
            }
          }
        }
      }
    }`,
    { id: `gid://shopify/Product/${p.target_pid}` }
  );

  let targetMedia = targetRes?.product?.media?.nodes ?? [];

  // Als Shopify nog media zonder URL teruggeeft: skip (anders thrash)
  const hasUrlLess = targetMedia.some(m => !m.image || !m.image.url);
  if (hasUrlLess) {
    console.log("IMAGE_REBUILD_SKIPPED_URLS_NOT_READY", {
      pim_pid,
      target_store: t.store,
      product_id: p.target_pid,
      target_media_count: targetMedia.length
    });
  } else {
    // Stabiele key op basis van filename (zonder Shopify suffix/params)
    const imageKey = (url) => {
      if (!url) return "";
      const clean = String(url).split("?")[0];
      const name = clean.substring(clean.lastIndexOf("/") + 1);
      return name
        .replace(/_[a-f0-9-]{36}(?=\.)/i, "")
        .toLowerCase();
    };

    const masterKeys = masterImages.map(i => imageKey(i?.src)).filter(Boolean);
    const targetKeys = targetMedia.map(m => imageKey(m?.image?.url)).filter(Boolean);

    // mismatch in count of order => rebuild
    const needsRebuild =
      masterKeys.length !== targetKeys.length ||
      masterKeys.some((k, idx) => k !== targetKeys[idx]);

    if (!needsRebuild) {
      console.log("IMAGE_REBUILD_NOT_NEEDED", {
        pim_pid,
        target_store: t.store,
        product_id: p.target_pid
      });
    } else {
      console.log("IMAGE_REBUILD_START", {
        pim_pid,
        target_store: t.store,
        product_id: p.target_pid,
        master_count: masterImages.length,
        target_count: targetMedia.length
      });

      // 1) DELETE ALL target media (chunked)
      const allMediaIds = targetMedia.map(m => m.id).filter(Boolean);
      const chunk = (arr, n) => {
        const out = [];
        for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
        return out;
      };

      for (const ids of chunk(allMediaIds, 25)) {
        await gql(
          t.store,
          t.token,
          `mutation ($mediaIds: [ID!]!, $productId: ID!) {
            productDeleteMedia(mediaIds: $mediaIds, productId: $productId) {
              deletedMediaIds
              mediaUserErrors { field message }
            }
          }`,
          {
            productId: `gid://shopify/Product/${p.target_pid}`,
            mediaIds: ids
          }
        );
      }

      // 2) RE-ADD master images (chunked)
      const createInputs = masterImages
        .map(i => i?.src)
        .filter(Boolean)
        .map(src => ({
          mediaContentType: "IMAGE",
          originalSource: src
        }));

      for (const mediaChunk of chunk(createInputs, 10)) {
        await gql(
          t.store,
          t.token,
          `mutation ($id: ID!, $media: [CreateMediaInput!]!) {
            productCreateMedia(productId: $id, media: $media) {
              media { id }
              userErrors { field message }
            }
          }`,
          {
            id: `gid://shopify/Product/${p.target_pid}`,
            media: mediaChunk
          }
        );
      }

      // 3) REFRESH + REORDER volgens master volgorde (op basis van key)
      targetRes = await gql(
        t.store,
        t.token,
        `query ($id: ID!) {
          product(id: $id) {
            media(first: 50) {
              nodes {
                ... on MediaImage {
                  id
                  image { url }
                }
              }
            }
          }
        }`,
        { id: `gid://shopify/Product/${p.target_pid}` }
      );

      targetMedia = targetRes?.product?.media?.nodes ?? [];

      const targetByKey = new Map(
        targetMedia
          .map(m => [imageKey(m.image?.url), m.id])
          .filter(([k, id]) => k && id)
      );

      const orderedIds = masterImages
        .map(i => i?.src)
        .filter(Boolean)
        .map(src => targetByKey.get(imageKey(src)))
        .filter(Boolean);

      if (orderedIds.length > 1) {
        const moves = orderedIds.map((mediaId, index) => ({
          id: mediaId,
          newPosition: String(index),
        }));

        await gql(
          t.store,
          t.token,
          `mutation ($id: ID!, $moves: [MoveInput!]!) {
            productReorderMedia(id: $id, moves: $moves) {
              userErrors { field message }
            }
          }`,
          {
            id: `gid://shopify/Product/${p.target_pid}`,
            moves
          }
        );
      }

      console.log("IMAGE_REBUILD_DONE", {
        pim_pid,
        target_store: t.store,
        product_id: p.target_pid,
        ordered_count: orderedIds.length
      });
    }
  }

  // ===== END IMAGE SYNC (REBUILD ON CHANGE) =====


  
 
  // DIRECT VERIFICATIE: opnieuw ophalen
  const verify = await gql(
    t.store,
    t.token,
    `query ($id: ID!) {
      product(id: $id) {
        media(first: 50) {
          nodes {
            ... on MediaImage {
              id
              image { url }
            }
          }
        }
      }
    }`,
    { id: `gid://shopify/Product/${p.target_pid}` }
  );
  
  console.log("IMAGE_REORDER_VERIFY", {
    pim_pid,
    target_store: t.store,
    product_id: p.target_pid,
    after_order: verify.product.media.nodes.map(m => ({
      id: m.id,
      url: m.image?.url
    }))
  });
  
}

// PRODUCT DELTA SUCCESVOL VERWERKT → TRACKER BIJWERKEN
maxProductDeltaId = Math.max(maxProductDeltaId, p.delta_id);


// === DELTA PRODUCT METAFIELDS (SOURCE = PRODUCTS, LIKE IMAGES) ===
const mfRow = await env.DB.prepare(`
  SELECT metafields_json
  FROM products
  WHERE pim_pid = ?1
  LIMIT 1
`).bind(pim_pid).first();

const metafields = JSON.parse(mfRow?.metafields_json || "[]");

if (metafields.length) {

  const defRes = await gql(
    t.store,
    t.token,
    `query {
      metafieldDefinitions(ownerType: PRODUCT, first: 250) {
        nodes {
          namespace
          key
          type { name }
        }
      }
    }`
  );

  const allowed = new Set(
    (defRes?.metafieldDefinitions?.nodes ?? []).map(
      d => `${d.namespace}:${d.key}:${d.type.name}`
    )
  );

  const filtered = metafields.filter(m =>
    allowed.has(`${m.namespace}:${m.key}:${m.type}`)
  );

  if (filtered.length) {
    await gql(
      t.store,
      t.token,
      `mutation ($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          userErrors { field message }
        }
      }`,
      {
        metafields: filtered.map(m => ({
          ownerId: `gid://shopify/Product/${p.target_pid}`,
          namespace: m.namespace,
          key: m.key,
          type: m.type,
          value: String(m.value),
        })),
      }
    );
  }
}
     }

          if (!targetPid) {
            console.log("VARIANT_SYNC_SKIP_NO_TARGET_PRODUCT", {
              pim_pid,
              target_store: t.store
            });
            continue;
          }
// INT price rule needs product_type (exclude Watch)
const prodTypeRow = await env.DB.prepare(`
  SELECT product_type
  FROM products
  WHERE pim_pid = ?1
  LIMIT 1
`).bind(pim_pid).first();

const productTypeLower = String(prodTypeRow?.product_type || "").trim().toLowerCase();
const isWatch = productTypeLower === "watch";          

for (const v of variants.results ?? []) {

// ENSURE variant_links (zoek of maak variant)
if (!v.target_vid) {
  v.target_vid = await ensureVariantLink(
    env,
    gql,
    v.pim_vid,
    pim_pid,
    t.store,
    t.token
  );

  // variant bestaat nog niet → aanmaken
  if (!v.target_vid) {
    const created = await gql(
      t.store,
      t.token,
      `mutation ($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkCreate(
          productId: $productId,
          variants: $variants
        ) {
          productVariants { id }
          userErrors { field message }
        }
      }`,
      {
        productId: `gid://shopify/Product/${targetPid}`,
        variants: [{}]
      }
    );
    

    v.target_vid = created?.productVariantsBulkCreate?.productVariants?.[0]?.id
    ?.split("/")
    .pop();
  

    if (!v.target_vid) continue;
  }
}


await gql(
  t.store,
  t.token,
  `mutation ($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(
  productId: $productId,
  variants: $variants
  ) {
    productVariants { id }
    }
     }`,
     {
       productId: `gid://shopify/Product/${targetPid}`,
       variants: [{
         id: `gid://shopify/ProductVariant/${v.target_vid}`,
// store-specific price
...(t.store === env.TARGET_INT && !isWatch && v.price
  ? { price: (Number(v.price) * (100 / 121)).toFixed(2) }
  : { price: v.price || undefined }),

...(t.store === env.TARGET_INT && !isWatch && v.compare_at_price
  ? { compareAtPrice: (Number(v.compare_at_price) * (100 / 121)).toFixed(2) }
  : { compareAtPrice: v.compare_at_price || undefined }),

barcode: v.barcode || undefined,
        }]
       }
      );

// VARIANT DELTA SUCCESVOL VERWERKT → TRACKER BIJWERKEN
maxVariantDeltaId = Math.max(maxVariantDeltaId, v.delta_id);



            console.log("PRICE_SYNC_AFTER_SHOPIFY", {
              target_vid: v.target_vid,
              price: v.price
            });
            
            // === INVENTORY ITEM UPDATE (weight / HS / COO / SKU) ===
if (
  v.weight_value !== null ||
  v.hscode ||
  v.country_of_origin ||
  v.sku ||
  v.cost !== null
) {
  // 1) inventoryItemId ophalen via variant
  const invRes = await gql(
    t.store,
    t.token,
    `query ($id: ID!) {
      productVariant(id: $id) {
        inventoryItem { id }
      }
    }`,
    {
      id: `gid://shopify/ProductVariant/${v.target_vid}`,
    }
  );

  const inventoryItemId =
    invRes?.productVariant?.inventoryItem?.id;

  // 2) inventoryItemUpdate uitvoeren
  if (inventoryItemId) {
    await gql(
      t.store,
      t.token,
      `mutation ($id: ID!, $input: InventoryItemInput!) {
        inventoryItemUpdate(id: $id, input: $input) {
          inventoryItem { id }
        }
      }`,
      {
        id: inventoryItemId,
        input: {
          sku: v.sku || undefined,
          harmonizedSystemCode: v.hscode || undefined,
          countryCodeOfOrigin: v.country_of_origin || undefined,
        
          cost:
  v.cost !== null && v.cost !== undefined
    ? Number(v.cost)
    : undefined,

        
          measurement: v.weight_value !== null
            ? {
                weight: {
                  value: v.weight_value,
                  unit: v.weight_unit || "KILOGRAMS",
                },
              }
            : undefined,
        },                
      }
    );
  }
}
// VARIANT DELTA SUCCESVOL VERWERKT → delta_id vastleggen
maxVariantDeltaId = Math.max(maxVariantDeltaId, v.delta_id);

            
          }
                         await env.DB.prepare(
          `INSERT OR REPLACE INTO sync_state
           (target_store, last_delta_product_id, last_delta_variant_id, updated_at)
           VALUES (?1, ?2, ?3, datetime('now'))`
        )
        .bind(
          t.store,
          maxProductDeltaId,
          maxVariantDeltaId
        )
        .run();
 
        }

        msg.ack();
      } catch (e) {
        console.log("SYNC_ERROR", {
          message: e?.message,
          stack: e?.stack,
          raw: e
        });
        msg.retry();
      }
      
      
    }
  },
};
