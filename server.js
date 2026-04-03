/**
 * Webhook Server: Phantombuster → Claude AI → Lemlist
 * Genera mensajes outbound personalizados a partir de actividad LinkedIn
 */

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// ─── CONFIG (se leen desde variables de entorno) ─────────────────────────────
const ANTHROPIC_API_KEY  = process.env.ANTHROPIC_API_KEY;
const LEMLIST_API_KEY    = process.env.LEMLIST_API_KEY;
const PHANTOMBUSTER_ORG  = process.env.PHANTOMBUSTER_ORG  || '4237829874326193';
const PHANTOM_AGENT_ID   = process.env.PHANTOM_AGENT_ID   || '5621422771951702';
const WEBHOOK_SECRET     = process.env.WEBHOOK_SECRET     || 'native-outbound-2026';
const PORT               = process.env.PORT               || 3000;

// POST_FRESHNESS_DAYS: posts más antiguos que esto se tratan como "sin contexto reciente"
const POST_FRESHNESS_DAYS = parseInt(process.env.POST_FRESHNESS_DAYS || '60');

// Archivo local para trackear contactos ya procesados
const PROCESSED_FILE = path.join(__dirname, 'processed_contacts.json');

// ─── HELPERS ─────────────────────────────────────────────────────────────────

function loadProcessed() {
  try {
    return JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveProcessed(data) {
  fs.writeFileSync(PROCESSED_FILE, JSON.stringify(data, null, 2));
}

// Normaliza una URL de LinkedIn para comparación: extrae "linkedin.com/in/username"
function normalizeLinkedinUrl(url) {
  if (!url || typeof url !== 'string') return '';
  const match = url.toLowerCase().match(/linkedin\.com\/in\/([^/?#\s]+)/);
  if (match) return `linkedin.com/in/${match[1].replace(/\/$/, '')}`;
  return '';
}

// Normaliza una URL de LinkedIn Sales Navigator: extrae el entity ID
// Ej: "https://www.linkedin.com/sales/lead/ACwAAB-DYL0B..." → "salesnav:ACwAAB-DYL0B"
function normalizeSalesNavUrl(url) {
  if (!url || typeof url !== 'string') return '';
  const match = url.match(/linkedin\.com\/sales\/lead\/([^,/?#\s]+)/i);
  if (match) return `salesnav:${match[1]}`;
  return '';
}actEmailCache = {};

/**
 * Busca un contacto en LemCRM por nombre + verificación de LinkedIn URL.
 *
 * Endpoint: GET https://api.lemlist.com/api/contacts
 * Auth:     Basic (username='', password=LEMLIST_API_KEY)
 * Response: { contacts: [...], total, limit, offset }
 *
 * Strategy:
 *   1. Busca por "firstName lastName" → filtra por linkedinUrl exacto
 *   2. Si no hay coincidencia exacta pero hay un único resultado, lo usa
 *   3. Cachea resultado (incluyendo null) para evitar re-llamadas
 */
async function findContact(profileUrl, firstName, lastName) {
  const normalized    = normalizeLinkedinUrl(profileUrl);
  const normalizedSN  = normalizeSalesNavUrl(profileUrl);

  // Cache hit (null también se cachea para evitar re-intentos)
  const cacheKey = normalized || normalizedSN || `${firstName}|${lastName}`.toLowerCase();
  if (Object.prototype.hasOwnProperty.call(contactEmailCache, cacheKey)) {
    return contactEmailCache[cacheKey];
  }

  // Construir término de búsqueda: nombre completo funciona mejor que el slug de LinkedIn
  const searchTerm = [firstName, lastName].filter(Boolean).join(' ').trim();
  if (!searchTerm) {
    contactEmailCache[cacheKey] = null;
    return null;
  }

  try {
    const resp = await axios.get('https://api.lemlist.com/api/contacts', {
      auth: { username: '', password: LEMLIST_API_KEY },
      params: { search: searchTerm, limit: 10 },
      timeout: 10000
    });

    // La respuesta puede ser { contacts: [...] } o un array plano
    const contacts = Array.isArray(resp.data)
      ? resp.data
      : (Array.isArray(resp.data?.contacts) ? resp.data.contacts : []);

    // 1. Intentar match exacto por LinkedIn URL (regular o Sales Navigator)
    let found = null;
    if (normalized || normalizedSN) {
      found = contacts.find(c => {
        // Comparar contra linkedinUrl (URL regular)
        const cRegular = c.linkedinUrl || c.linkedin || c.linkedInUrl || c.linkedinProfile || '';
        if (normalized && normalizeLinkedinUrl(cRegular) === normalized) return true;
        // Comparar contra linkedinUrlSalesNav (Sales Navigator)
        const cSalesNav = c.linkedinUrlSalesNav || c.salesNavUrl || '';
        if (normalizedSN && normalizeSalesNavUrl(cSalesNav) === normalizedSN) return true;
        // Comparar cruzado: profileUrl regular contra SalesNav del contacto, o viceversa
        if (normalized && normalizeSalesNavUrl(cSalesNav) && false) return false; // future
        return false;
      });
    }

    // 2. Fallback: único resultado en la búsqueda
    if (!found && contacts.length === 1) {
      found = contacts[0];
    }

    const email = found?.email || null;
    contactEmailCache[cacheKey] = email;

    if (email) {
      console.log(`   ✅ Contacto encontrado en LemCRM: ${email} (buscado: "${searchTerm}")`);
    } else {
      console.log(`   ⚠️  No encontrado en LemCRM (${contacts.length} resultados para "${searchTerm}")`);
    }

    return email;
  } catch (err) {
    console.error('   ⚠️  findContact error:', err.message);
    return null;
  }
}

// Resuelve el email de un lead dado su profileUrl y nombre
async function resolveEmailFromLinkedIn(profileUrl, firstName, lastName) {
  return await findContact(profileUrl, firstName, lastName);
}

// ─── FETCH PHANTOMBUSTER RESULTS ─────────────────────────────────────────────

async function fetchPhantombusterResults() {
  const url = `https://api.phantombuster.com/api/v2/agents/fetch-output?id=${PHANTOM_AGENT_ID}`;
  const res = await axios.get(url, {
    headers: { 'X-Phantombuster-Org': PHANTOMBUSTER_ORG }
  });

  const output = res.data.output || '';

  // Intentar obtener el CSV de resultados desde S3
  const csvUrlMatch = output.match(/https:\/\/phantombuster\.s3[^\s"]+\.csv/);
  if (!csvUrlMatch) {
    console.log('No CSV URL found in output, using JSON results from API');
    return null;
  }

  const csvRes = await axios.get(csvUrlMatch[0]);
  return parseCsv(csvRes.data);
}

function parseCsv(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
  return lines.slice(1).map(line => {
    // Soporte para valores con comas dentro de comillas
    const values = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += ch;
      }
    }
    values.push(current.trim());

    const obj = {};
    headers.forEach((h, i) => { obj[h] = values[i] || ''; });
    return obj;
  });
}

// ─── CLAUDE AI: GENERAR MENSAJES PERSONALIZADOS ───────────────────────────────

async function generatePersonalizedMessages(contact, postsAreRecent) {
  const { firstName, lastName, jobTitle, companyName, posts, profileUrl } = contact;

  let postsText;

  if (!postsAreRecent || !posts || posts.length === 0) {
    // Posts muy viejos o sin posts → contexto genérico
    postsText = `⚠️  Sin actividad reciente disponible (posts >60 días o sin posts)
→ Genera mensajes basados en su cargo y empresa. Menciona el canal tradicional de forma genérica.
→ NO inventes ni parafrasees posts específicos que no tienes.`;
  } else {
    postsText = posts.map((p, i) => {
      const engagement = [];
      if (p.likeCount)    engagement.push(`${p.likeCount} likes`);
      if (p.commentCount) engagement.push(`${p.commentCount} comentarios`);
      if (p.repostCount)  engagement.push(`${p.repostCount} reposts`);
      const engStr = engagement.length ? ` [Engagement: ${engagement.join(', ')}]` : '';
      const isRepost = p.action && p.action.toLowerCase().includes('repost');
      const postType = isRepost
        ? 'REPOST (contenido que decidio amplificar)'
        : `POST PROPIO (${p.postType || 'texto'})`;

      return `--- Post ${i + 1} ---
Tipo: ${postType}${engStr}
Fecha: ${p.postDate || 'reciente'}
Contenido completo:
"${p.postContent || ''}"`;
    }).join('\n\n');
  }

  const systemPrompt = `Eres un SDR senior especializado en ventas B2B consultivas para el canal tradicional (retail tradicional / trade) en LATAM.

Representas a Native, plataforma de Computer Vision + AI Agents para marcas FMCG/CPG.

LO QUE HACE NATIVE (úsalo selectivamente, nunca todo junto):
• Visibilidad del 100% del punto de venta tradicional mediante Computer Vision
• Detecta oportunidades de distribución, quiebre de stock y share of shelf en tiempo real
• Convierte datos granulares (tienda por tienda, SKU por SKU) en decisiones de ejecución
• Elimina puntos ciegos del canal: los equipos saben exactamente dónde y cuándo actuar
• Clientes activos en México, Colombia, Perú, Chile, Ecuador (canal tradicional)

TU MISIÓN: escribir mensajes que parezcan escritos a mano por alguien que REALMENTE leyó sus posts.

PROCESO OBLIGATORIO antes de escribir:
1. Identifica el TEMA CENTRAL que mueve a esta persona (¿qué lo/la apasiona? ¿qué problema menciona?)
2. Encuentra UNA frase, idea o dato específico de sus posts que puedas mencionar literalmente
3. Detecta su tono (técnico, inspiracional, operativo, estratégico) y espéjalo
4. Conecta su preocupación real con el ángulo más relevante de Native (sin mencionar Native aún)

REGLAS DE ESCRITURA:
- Primera línea: referencia directa y específica a algo de sus posts (o, si no hay posts recientes, referencia a su cargo/industria de forma concreta)
- Email: máx 120 palabras, sin bullets, fluido como conversación
- LinkedIn DM: máx 75 palabras, más casual y directo
- Follow-ups: ángulos distintos, no repetir el mismo gancho
- NUNCA empieces con "Vi tu post sobre..." — sé más creativo
- NUNCA menciones "Native" en el primer contacto — solo genera curiosidad
- Idioma: detecta si escribe en español o inglés y úsalo

SEÑALES DE PERSONALIZACIÓN REAL (al menos UNA por mensaje):
• Citar una frase textual o parafrasearla de forma reconocible
• Referenciar un resultado o métrica que mencionó
• Mencionar un país/mercado específico que nombró
• Aludir a un reto o aprendizaje que compartió`;

  const userPrompt = `PROSPECTO:
• Nombre: ${firstName} ${lastName}
• Cargo: ${jobTitle || 'No especificado'}
• Empresa: ${companyName || 'No especificada'}
• LinkedIn: ${profileUrl || 'N/A'}

═══════════════════════════════════════
ACTIVIDAD LINKEDIN RECIENTE (LEE CON ATENCIÓN):
═══════════════════════════════════════
${postsText}

═══════════════════════════════════════
ANÁLISIS PREVIO (piensa en voz alta antes de escribir):
Antes de generar los mensajes, incluye brevemente en tu respuesta JSON un campo "analysis" con:
- El tema central que identifiques
- La frase/dato específico que usarás como gancho
- El ángulo de Native más relevante para este perfil

Luego genera los mensajes con exactamente estas claves:
═══════════════════════════════════════

{
  "analysis": {
    "centralTheme": "¿de qué trata principalmente su actividad?",
    "hook": "la frase/dato específico que usarás",
    "nativeAngle": "qué aspecto de Native conecta mejor con este perfil"
  },
  "customSubject": "asunto del email (máx 55 chars, sin clickbait, que genere curiosidad real — puede referenciar algo de sus posts)",
  "customEmailBody": "cuerpo del email (máx 120 palabras, primera línea con referencia específica a sus posts, segunda parte abre una pregunta o tensión relevante para su rol, cierre con CTA suave)",
  "customLinkedinDm": "mensaje directo LinkedIn (máx 75 palabras, tono más casual, como si ya se conocieran de haber leído sus posts, termina con pregunta abierta)",
  "customFollowup1": "follow-up 1 — día 4 (máx 80 palabras, ángulo diferente: ahora sí puedes mencionar qué hace Native de forma concisa, pero conectado a algo que él/ella mencionó)",
  "customFollowup2": "follow-up 2 — día 8 (máx 55 palabras, muy breve, admite que no ha respondido con humor suave, deja la puerta abierta)"
}

Responde SOLO con el JSON válido, sin texto adicional fuera de él.`;

  const response = await axios.post(
    'https://api.anthropic.com/v1/messages',
    {
      model: 'claude-sonnet-4-6',
      max_tokens: 1500,
      messages: [{ role: 'user', content: userPrompt }],
      system: systemPrompt
    },
    {
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      }
    }
  );

  const text = response.data.content[0].text;
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error('Claude no devolvio JSON valido');
  return JSON.parse(jsonMatch[0]);
}

// ─── LEMLIST: ACTUALIZAR LEAD ─────────────────────────────────────────────────

async function updateLemlistLead(email, variables) {
  try {
    // PATCH /api/leads/:email/variables — correct Lemlist endpoint for custom variables
    const updateRes = await axios.patch(
      `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}/variables`,
      variables,
      { auth: { username: '', password: LEMLIST_API_KEY } }
    );
    return updateRes.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`   Lead no encontrado en Lemlist: ${email}`);
      if (err.response?.data) console.error(`   ❌ Lemlist 404 detail:`, JSON.stringify(err.response.data));
      return null;
    }
    console.error(`   ❌ Lemlist PATCH error ${err.response?.status}:`, err.response?.data || err.message);
    throw err;
  }
}

// ─── PROCESAMIENTO PRINCIPAL ──────────────────────────────────────────────────

async function processNewContacts(results) {
  const processed = loadProcessed();
  let newCount = 0;
  let errorCount = 0;
  let noEmailCount = 0;

  // Agrupar resultados por profileUrl (cada contacto tiene hasta N posts)
  const contactMap = {};
  for (const row of results) {
    const profileUrl = row.profileUrl || row.profile_url || row['Profile Url'] || row.authorUrl || row['Author Url'] || '';
    const email      = row.email || row.Email || '';
    const key        = profileUrl || email;
    if (!key) continue;

    if (!contactMap[key]) {
      const authorFull  = row.Author || row.author || '';
      const authorParts = authorFull.split(' ');
      contactMap[key] = {
        email: email || null,
        profileUrl,
        firstName:   row.firstName   || row.first_name   || authorParts[0] || '',
        lastName:    row.lastName    || row.last_name    || authorParts.slice(1).join(' ') || '',
        jobTitle:    row.jobTitle    || row.job_title    || row['Job Title'] || '',
        companyName: row.companyName || row.company_name || row['Company']   || '',
        posts: []
      };
    }

    const postContent = row.postContent || row.post_content || row['Post Content'] || '';
    if (postContent) {
      contactMap[key].posts.push({
        postContent,
        postDate:     row.postDate     || row.post_date     || row['Post Date']     || '',
        postUrl:      row.postUrl      || row.post_url      || row['Post Url']      || '',
        postType:     row.type         || row.Type          || row['Type']          || '',
        action:       row.action       || row.Action        || '',
        likeCount:    row.likeCount    || row.like_count    || row['Like Count']    || '',
        commentCount: row.commentCount || row.comment_count || row['Comment Count'] || '',
        repostCount:  row.repostCount  || row.repost_count  || row['Repost Count']  || '',
        viewCount:    row.viewCount    || row.view_count    || row['View Count']    || ''
      });
    }
  }

  console.log(`\n📋 Total contactos en resultados: ${Object.keys(contactMap).length}`);
  console.log(`✅ Ya procesados: ${Object.keys(processed).length}`);
  console.log(`🗺️  Contactos en caché LinkedIn→Email: ${Object.keys(contactEmailCache).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;

    // Resolver email: primero del CSV (suele estar vacío en Phantombuster),
    // luego búsqueda en LemCRM por nombre + verificación de LinkedIn URL
    let email = contact.email;
    if (!email && (contact.firstName || contact.lastName)) {
      email = await resolveEmailFromLinkedIn(contact.profileUrl, contact.firstName, contact.lastName);
      if (email) {
        contact.email = email;
        console.log(`\n🔗 Email resuelto para ${contact.firstName} ${contact.lastName}: ${email}`);
      }
    }

    console.log(`\n🔄 Procesando: ${contact.firstName} ${contact.lastName} | ${contact.profileUrl || email || 'sin ID'}`);

    // Verificar frescura de posts
    const postsAreRecent = hasRecentPosts(contact.posts, POST_FRESHNESS_DAYS);
    if (!postsAreRecent && contact.posts.length > 0) {
      console.log(`   ⏰ Posts más antiguos de ${POST_FRESHNESS_DAYS} días → usando mensaje genérico`);
    }

    try {
      // 1. Generar mensajes con Claude
      const messages = await generatePersonalizedMessages(contact, postsAreRecent);
      if (messages.analysis) {
        console.log(`   🧠 Tema: "${messages.analysis.centralTheme}"`);
        console.log(`   🪝 Hook: "${messages.analysis.hook}"`);
        console.log(`   🎯 Angulo Native: "${messages.analysis.nativeAngle}"`);
      }
      console.log(`   ✍️  Mensajes generados por Claude`);

      // 2. Actualizar Lemlist (si tenemos email)
      if (email) {
        try {
          const lemlistResult = await updateLemlistLead(email, {
            customSubject:        messages.customSubject        || '',
            customEmailBody:      messages.customEmailBody      || '',
            customLinkedinDm:     messages.customLinkedinDm     || '',
            customFollowup1:      messages.customFollowup1      || '',
            customFollowup2:      messages.customFollowup2      || '',
            customPersonalHook:   messages.analysis?.hook        || '',
            customNativeAngle:    messages.analysis?.nativeAngle || '',
            linkedinActivityProcessed: new Date().toISOString(),
            postsWereRecent: postsAreRecent ? 'yes' : 'no'
          });

          if (lemlistResult) {
            console.log(`   ✅ Lemlist actualizado: ${email}`);
          } else {
            console.log(`   ⚠️  Lead no encontrado en Lemlist: ${email}`);
          }
        } catch (lemErr) {
          console.error(`   ❌ Error actualizando Lemlist: ${lemErr.message}`);
        }
      } else {
        console.log(`   ⚠️  Sin email — no se actualizo Lemlist (profileUrl: ${contact.profileUrl})`);
        noEmailCount++;
      }

      // 3. Guardar en procesados (incluso sin email, para no re-procesar)
      processed[key] = {
        processedAt:    new Date().toISOString(),
        name:           `${contact.firstName} ${contact.lastName}`,
        email:          email || '',
        profileUrl:     contact.profileUrl || '',
        postsCount:     contact.posts.length,
        postsWereRecent: postsAreRecent,
        lemlistUpdated: !!email
      };

      newCount++;
      await new Promise(r => setTimeout(r, 1000)); // Rate limiting

    } catch (err) {
      console.error(`   ❌ Error procesando ${key}:`, err.message);
      if (err.response?.data) console.error(`   ❌ API error detail:`, JSON.stringify(err.response.data));
      errorCount++;
    }
  }

  saveProcessed(processed);
  return {
    newCount,
    errorCount,
    noEmailCount,
    totalContacts: Object.keys(contactMap).length
  };
}

// ─── RUTAS HTTP ───────────────────────────────────────────────────────────────

// Health check
app.get('/', (req, res) => {
  const processed = loadProcessed();
  res.json({
    status: 'ok',
    service: 'Native Outbound',
    ts: new Date().toISOString(),
    totalProcessed: Object.keys(processed).length,
    contactCacheSize: Object.keys(contactEmailCache).length,
    contactLookup: 'ON-DEMAND via LemCRM API (api.lemlist.com/api/contacts, Basic auth)'
  });
});

// Webhook principal — Phantombuster llama aquí al terminar cada run
app.post('/webhook', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  console.log('\n🚀 Webhook recibido de Phantombuster:', new Date().toISOString());
  res.json({ status: 'processing', message: 'Procesando resultados en background' });

  setImmediate(async () => {
    try {
      const results = await fetchPhantombusterResults();
      if (!results || results.length === 0) {
        console.log('⚠️  No se encontraron resultados CSV, intentando body del webhook...');
        if (req.body && Array.isArray(req.body.results)) {
          const stats = await processNewContacts(req.body.results);
          console.log(`\n✅ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
        }
        return;
      }
      const stats = await processNewContacts(results);
      console.log(`\n✅ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
    } catch (err) {
      console.error('❌ Error en procesamiento:', err.message);
    }
  });
});

// Trigger manual para testing
app.post('/process', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    console.log('\n🔧 Trigger manual de procesamiento...');
    const results = await fetchPhantombusterResults();
    if (!results) {
      return res.status(404).json({ error: 'No se encontraron resultados en Phantombuster' });
    }
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) {
    console.error('❌ Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Procesar datos enviados directamente en el body (testing)
app.post('/process-direct', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const { results } = req.body;
    if (!results || !Array.isArray(results)) {
      return res.status(400).json({ error: 'Se requiere { results: [...] }' });
    }
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Limpiar caché de contactos (fuerza re-búsqueda en el próximo procesamiento)
app.post('/rebuild-map', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const prev = Object.keys(contactEmailCache).length;
  Object.keys(contactEmailCache).forEach(k => delete contactEmailCache[k]);
  res.json({
    success: true,
    message: `Caché limpiado (${prev} entradas eliminadas). Los contactos se buscarán on-demand.`,
    cacheSize: 0
  });
});

// Ver estadísticas de procesados
app.get('/stats', (req, res) => {
  const processed = loadProcessed();
  const list = Object.entries(processed);
  const withEmail    = list.filter(([, v]) => v.lemlistUpdated).length;
  const withoutEmail = list.filter(([, v]) => !v.lemlistUpdated).length;
  const freshPosts   = list.filter(([, v]) => v.postsWereRecent).length;

  res.json({
    totalProcessed: list.length,
    lemlistUpdated: withEmail,
    noEmail:        withoutEmail,
    freshPosts,
    contactCacheSize: Object.keys(contactEmailCache).length,
    contacts: processed
  });
});

// Debug: probar PATCH variables en Lemlist
app.get('/debug-lemlist', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });

  try {
    // 1. Obtener campañas
    const campsRes = await axios.get('https://api.lemlist.com/api/campaigns',
      { auth: { username: '', password: LEMLIST_API_KEY } });
    const master = (campsRes.data || []).find(c => c.name === 'Master Campaign 2.0');
    if (!master) return res.json({ error: 'Master Campaign 2.0 not found', campaigns: (campsRes.data || []).map(c => c.name) });

    // 2. Obtener primeros leads
    const leadsRes = await axios.get(`https://api.lemlist.com/api/campaigns/${master._id}/leads`,
      { auth: { username: '', password: LEMLIST_API_KEY }, params: { limit: 100, offset: 0 } });
    const leads = leadsRes.data || [];
    const emails = leads.map(l => l.email).filter(Boolean);

    // 3. Probar PATCH /variables en el primer email
    let patchResult = null;
    let patchError = null;
    if (emails[0]) {
      try {
        const pr = await axios.patch(
          `https://api.lemlist.com/api/leads/${encodeURIComponent(emails[0])}/variables`,
          { debugTest: 'patch_variables_test_' + Date.now() },
          { auth: { username: '', password: LEMLIST_API_KEY } }
        );
        patchResult = pr.data;
      } catch (e) {
        patchError = { status: e.response?.status, data: e.response?.data, message: e.message };
      }
    }

    res.json({ campaignId: master._id, emails, patchResult, patchError, rawLeads: leads.slice(0, 2) });
  } catch (err) {
    res.status(500).json({ error: err.message, detail: err.response?.data });
  }
});

// Debug: buscar contacto en LemCRM por nombre o término
// GET /debug-contacts?secret=...&search=claudia+ventura  → busca por nombre (recomendado)
// GET /debug-contacts?secret=...&search=email@empresa.com → busca por email
app.get('/debug-contacts', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });

  const searchTerm = req.query.search || req.query.q || '';
  if (!searchTerm) {
    return res.status(400).json({ error: 'Param ?search= requerido. Ejemplo: ?search=claudia+ventura' });
  }

  try {
    const resp = await axios.get('https://api.lemlist.com/api/contacts', {
      auth: { username: '', password: LEMLIST_API_KEY },
      params: { search: searchTerm, limit: 10 },
      timeout: 10000
    });

    const contacts = Array.isArray(resp.data)
      ? resp.data
      : (Array.isArray(resp.data?.contacts) ? resp.data.contacts : []);

    const sample = contacts.slice(0, 5).map(c => ({
      email:               c.email,
      firstName:           c.firstName,
      lastName:            c.lastName,
      linkedinUrl:         c.linkedinUrl || c.linkedin || c.linkedInUrl || null,
      linkedinUrlSalesNav: c.linkedinUrlSalesNav || null,
      jobTitle:            c.jobTitle || null,
      companyId:           c.companyId || null
    }));

    res.json({
      searchTerm,
      total: contacts.length,
      rawTotal: resp.data?.total,
      sample,
      note: 'Usando api.lemlist.com/api/contacts con Basic auth. Busca por nombre/email.'
    });
  } catch (err) {
    res.status(500).json({ error: err.message, status: err.response?.status, detail: err.response?.data });
  }
});

// Lista leads de una campaña con sus datos completos de contacto (email, LinkedIn, Sales Nav)
// GET /list-campaign-contacts?secret=...&campaign=Master+Campaign+2.0&limit=20
app.get('/list-campaign-contacts', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });

  const campaignName = req.query.campaign || 'Master Campaign 2.0';
  const limit = parseInt(req.query.limit || '20');
  const auth = { username: '', password: LEMLIST_API_KEY };

  try {
    // 1. Encontrar la campaña
    const campsRes = await axios.get('https://api.lemlist.com/api/campaigns', { auth });
    const campaign = (campsRes.data || []).find(c => c.name === campaignName);
    if (!campaign) {
      return res.json({
        error: `Campaña "${campaignName}" no encontrada`,
        available: (campsRes.data || []).map(c => c.name)
      });
    }

    // 2. Obtener leads de la campaña
    const leadsRes = await axios.get(
      `https://api.lemlist.com/api/campaigns/${campaign._id}/leads`,
      { auth, params: { limit: limit + 20, offset: 0 } }
    );
    const leads = Array.isArray(leadsRes.data) ? leadsRes.data : [];

    // 3. Resolver datos completos de cada lead
    const enriched = [];
    for (const lead of leads.slice(0, limit)) {
      let contactData = {};

      // Intentar obtener datos del contacto via contactId
      if (lead.contactId) {
        try {
          const cr = await axios.get(
            `https://api.lemlist.com/api/contacts/${lead.contactId}`,
            { auth, timeout: 5000 }
          );
          contactData = cr.data || {};
        } catch (e) {
          // Ignorar errores individuales
        }
      }

      enriched.push({
        leadId:              lead._id,
        contactId:           lead.contactId || null,
        state:               lead.state || null,
        email:               lead.email || contactData.email || null,
        firstName:           lead.firstName || contactData.firstName || null,
        lastName:            lead.lastName || contactData.lastName || null,
        jobTitle:            lead.jobTitle || contactData.jobTitle || null,
        companyName:         lead.companyName || contactData.companyName || null,
        linkedinUrl:         contactData.linkedinUrl || lead.linkedinUrl || null,
        linkedinUrlSalesNav: contactData.linkedinUrlSalesNav || lead.linkedinUrlSalesNav || null,
        hasEmail:            !!(lead.email || contactData.email),
        hasLinkedin:         !!(contactData.linkedinUrl || lead.linkedinUrl),
        hasSalesNav:         !!(contactData.linkedinUrlSalesNav || lead.linkedinUrlSalesNav)
      });
    }

    const withEmail    = enriched.filter(l => l.hasEmail).length;
    const withLinkedin = enriched.filter(l => l.hasLinkedin).length;
    const withSalesNav = enriched.filter(l => l.hasSalesNav).length;

    res.json({
      campaign: campaignName,
      campaignId: campaign._id,
      totalLeads: leads.length,
      returned: enriched.length,
      stats: { withEmail, withLinkedin, withSalesNav },
      contacts: enriched
    });
  } catch (err) {
    res.status(500).json({ error: err.message, detail: err.response?.data });
  }
});

// ─── START ────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`\n🎯 Native Outbound Server corriendo en puerto ${PORT}`);
  console.log(`   Webhook URL:     POST /webhook?secret=${WEBHOOK_SECRET}`);
  console.log(`   Process URL:     POST /process?secret=${WEBHOOK_SECRET}`);
  console.log(`   Direct Process:  POST /process-direct?secret=${WEBHOOK_SECRET}`);
  console.log(`   Rebuild Map:     POST /rebuild-map?secret=${WEBHOOK_SECRET}`);
  console.log(`   Stats URL:       GET  /stats`);
  console.log(`   Debug Contacts:  GET  /debug-contacts?secret=${WEBHOOK_SECRET}&search=nombre`);
  console.log(`   Contact lookup:  ON-DEMAND via api.lemlist.com/api/contacts (Basic auth, name search)`);
});
