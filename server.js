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

// Calcula los días de antigüedad de una fecha
function daysAgo(dateStr) {
  if (!dateStr) return Infinity;
  // Fechas absolutas: "2025-12-15", "Dec 15, 2025", ISO, etc.
  const parsed = new Date(dateStr);
  if (!isNaN(parsed.getTime())) {
    return (Date.now() - parsed.getTime()) / (1000 * 60 * 60 * 24);
  }
  // Fechas relativas: "2 weeks ago", "1 month ago", "3 days ago"
  const relMatch = dateStr.match(/(\d+)\s*(second|minute|hour|day|week|month|year)/i);
  if (relMatch) {
    const num = parseInt(relMatch[1]);
    const unit = relMatch[2].toLowerCase();
    if (unit.startsWith('second') || unit.startsWith('minute') || unit.startsWith('hour')) return 0;
    if (unit.startsWith('day'))   return num;
    if (unit.startsWith('week'))  return num * 7;
    if (unit.startsWith('month')) return num * 30;
    if (unit.startsWith('year'))  return num * 365;
  }
  return Infinity;
}

// Devuelve true si al menos un post es más reciente que maxDaysOld
function hasRecentPosts(posts, maxDaysOld) {
  if (!posts || posts.length === 0) return false;
  return posts.some(p => daysAgo(p.postDate) <= maxDaysOld);
}

// ─── LEMLIST EMAIL MAP (LinkedIn URL → email) ─────────────────────────────────

let lemlistEmailMap = {}; // normalizedLinkedinUrl → email
let lemlistMapBuiltAt = null;

apync function buildLemlistEmailMap() {
  console.log('\n📧 Construyendo mapa LinkedIn→Email desde Lemlist...');
  const map = {};

  try {
    // 1. Obtener todas las campañas
    const campaignsRes = await axios.get(
      'https://api.lemlist.com/api/campaigns',
      { auth: { username: '', password: LEMLIST_API_KEY } }
    );
    const campaigns = campaignsRes.data || [];
    console.log(`   Campañas encontradas: ${campaigns.length}`);

    // 2. Por cada campaña, paginar todos los leads
    for (const campaign of campaigns) {
      let offset = 0;
      const limit = 100;
      let totalFetched = 0;

      while (true) {
        try {
          const leadsRes = await axios.get(
            `https://api.lemlist.com/api/campaigns/${campaign._id}/leads`,
            {
              auth: { username: '', password: LEMLIST_API_KEY },
              params: { limit, offset }
            }
          );
          const leads = leadsRes.data || [];
          if (leads.length === 0) break;

          for (const lead of leads) {
            const email = lead.email;
            if (!email) continue;

            // Buscar LinkedIn URL en TODOS los campos del lead
            // (el nombre del campo depende de cómo se importó)
            const allValues = Object.values(lead).filter(v => typeof v === 'string');
            for (const val of allValues) {
              const normalized = normalizeLinkedinUrl(val);
              if (normalized) {
                map[normalized] = email;
                break;
              }
            }
          }

          totalFetched += leads.length;
          if (leads.length < limit) break;
          offset += limit;
        } catch (err) {
          console.error(`   ⚠️  Error en campaña ${campaign.name}: ${err.message}`);
          break;
        }
      }

      if (totalFetched > 0) {
        console.log(`   ${campaign.name}: ${totalFetched} leads cargados`);
      }

      // Rate limit: pequeña pausa entre campañas
      await new Promise(r => setTimeout(r, 200));
    }

    lemlistEmailMap = map;
    lemlistMapBuiltAt = new Date();
    console.log(`\n✅ Mapa construido: ${Object.keys(map).length} leads con LinkedIn URL\n`);

  } catch (err) {
    console.error('❌ Error construyendo mapa Lemlist:', err.message);
  }
}

// Resuelve el email de un lead dado su profileUrl de LinkedIn
function resolveEmailFromLinkedIn(profileUrl) {
  const normalized = normalizeLinkedinUrl(profileUrl);
  if (!normalized) return null;
  return lemlistEmailMap[normalized] || null;
}

// ─── FETCH PHANTOMBUSTER RESULTS ─────────────────────────────────────────────

async function fetchPhantombusterResults() {
  const url = `https://api.phantombuster.com/api/v2/agents/fetch-output?id=${PHANTOM_AGENT_ID}`;
  const res = await axios.get(url, {
    headers: { 'X-Phantombuster-Org': PHANTOMBUSTER_ORG }
  });

  const output = res.data.output || '';

  // Intentar obtener el CSV de resultados desde S3
  const csvUrlMatch = output.match(/https:\/\/phantombuster\.s3[^\s]+\.csv/);
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

Representas a Native, plataforma de Computer Vision + AI Agents para marcas FMCG3 FMCG/CPG.

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
  // Primero verificar que el lead existe
  const checkRes = await axios.get(
    `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}`,
    { auth: { username: '', password: LEMLIST_API_KEY } }
  );

  if (!checkRes.data) {
    console.log(`   Lead no encontrado en Lemlist: ${email}`);
    return null;
  }

  // Actualizar las variables personalizadas
  const updateRes = await axios.patch(
    `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}`,
    variables,
    { auth: { username: '', password: LEMLIST_API_KEY } }
  );

  return updateRes.data;
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
  console.log(`🗺️  Leads en mapa LinkedIn→Email: ${Object.keys(lemlistEmailMap).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;

    // Resolver email: primero del CSV (vacío en Phantombuster), luego del mapa LinkedIn
    let email = contact.email;
    if (!email && contact.profileUrl) {
      email = resolveEmailFromLinkedIn(contact.profileUrl);
      if (email) {
        contact.email = email;
        console.log(`\n🔗 Email resuelto para ${contact.firstName}: ${email}`);
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
    lemlistMapSize: Object.keys(lemlistEmailMap).length,
    lemlistMapBuiltAt: lemlistMapBuiltAt ? lemlistMapBuiltAt.toISOString() : null
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
      // Refrescar mapa de emails si tiene más de 6 horas
      const sixHours = 6 * 60 * 60 * 1000;
      if (!lemlistMapBuiltAt || (Date.now() - lemlistMapBuiltAt.getTime()) > sixHours) {
        await buildLemlistEmailMap();
      }

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

// Forzar reconstrucción del mapa LinkedIn→Email
app.post('/rebuild-map', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  await buildLemlistEmailMap();
  res.json({
    success: true,
    mapSize: Object.keys(lemlistEmailMap).length,
    builtAt: lemlistMapBuiltAt
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
    lemlistMapSize: Object.keys(lemlistEmailMap).length,
    lemlistMapBuiltAt: lemlistMapBuiltAt ? lemlistMapBuiltAt.toISOString() : null,
    contacts: processed
  });
});

// ─── START ────────────────────────────────────────────────────────────────────

app.listen(PORT, async () => {
  console.log(`\n🎯 Native Outbound Server corriendo en puerto ${PORT}`);
  console.log(`   Webhook URL:     POST /webhook?secret=${WEBHOOK_SECRET}`);
  console.log(`   Process URL:     POST /process?secret=${WEBHOOK_SECRET}`);
  console.log(`   Direct Process:  POST /process-direct?secret=${WEBHOOK_SECRET}`);
  console.log(`   Rebuild Map:     POST /rebuild-map?secret=${WEBHOOK_SECRET}`);
  console.log(`   Stats URL:       GET  /stats`);

  // Construir mapa LinkedIn→Email al iniciar
  await buildLemlistEmailMap();
});
