/**
 * Webhook Server: Phantombuster â Claude AI â Lemlist
 * Genera mensajes outbound personalizados a partir de actividad LinkedIn
 */

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// âââ CONFIG (se leen desde variables de entorno) âââââââââââââââââââââââââââââ
const ANTHROPIC_API_KEY  = process.env.ANTHROPIC_API_KEY;
const LEMLIST_API_KEY    = process.env.LEMLIST_API_KEY;
const PHANTOMBUSTER_ORG  = process.env.PHANTOMBUSTER_ORG  || '4237829874326193';
const PHANTOM_AGENT_ID   = process.env.PHANTOM_AGENT_ID   || '5621422771951702';
const WEBHOOK_SECRET     = process.env.WEBHOOK_SECRET     || 'native-outbound-2026';
const PORT               = process.env.PORT               || 3000;

// POST_FRESHNESS_DAYS: posts mÃ¡s antiguos que esto se tratan como "sin contexto reciente"
const POST_FRESHNESS_DAYS = parseInt(process.env.POST_FRESHNESS_DAYS || '60');

// Archivo local para trackear contactos ya procesados
const PROCESSED_FILE = path.join(__dirname, 'processed_contacts.json');

// âââ HELPERS âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

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

// Normaliza una URL de LinkedIn para comparaciÃ³n: extrae "linkedin.com/in/username"
function normalizeLinkedinUrl(url) {
  if (!url || typeof url !== 'string') return '';
  const match = url.toLowerCase().match(/linkedin\.com\/in\/([^/?#\s]+)/);
  if (match) return `linkedin.com/in/${match[1].replace(/\/$/, '')}`;
  return '';
}

// Calcula los dÃ­as de antigÃ¼edad de una fecha
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

// Devuelve true si al menos un post es mÃ¡s reciente que maxDaysOld
function hasRecentPosts(posts, maxDaysOld) {
  if (!posts || posts.length === 0) return false;
  return posts.some(p => daysAgo(p.postDate) <= maxDaysOld);
}

// âââ LEMLIST EMAIL MAP (LinkedIn URL â email) âââââââââââââââââââââââââââââââââ

let lemlistEmailMap = {}; // normalizedLinkedinUrl â email
let lemlistMapBuiltAt = null;

async function buildLemlistEmailMap() {
  console.log('\n\u{1F4E7} Construyendo mapa LinkedIn→Email desde Lemlist...');
  const map = {};

  function extractLinkedInFromLead(lead, map) {
    const email = lead.email;
    if (!email) return;
    const allValues = Object.values(lead).filter(v => typeof v === 'string');
    for (const val of allValues) {
      const normalized = normalizeLinkedinUrl(val);
      if (normalized) { map[normalized] = email; break; }
    }
  }

  try {
    // 1. Contactos GLOBALES
    console.log('   \u{1F50D} Buscando en contactos globales...');
    let gOff = 0; let gTotal = 0;
    while (true) {
      try {
        const gRes = await axios.get('https://api.lemlist.com/api/leads', {
          auth: { username: '', password: LEMLIST_API_KEY },
          params: { limit: 100, offset: gOff }
        });
        const gl = gRes.data || [];
        if (gl.length === 0) break;
        for (const lead of gl) extractLinkedInFromLead(lead, map);
        gTotal += gl.length;
        if (gl.length < 100) break;
        gOff += 100;
        await new Promise(r => setTimeout(r, 150));
      } catch (err) { console.error('   \u26A0\uFE0F Contactos globales:', err.message); break; }
    }
    console.log(`   \u2705 Contactos globales: ${gTotal} procesados`);

    // 2. Campanas (fallback)
    const campaignsRes = await axios.get(
      'https://api.lemlist.com/api/campaigns',
      { auth: { username: '', password: LEMLIST_API_KEY } }
    );
    const campaigns = campaignsRes.data || [];
    console.log(`   Campanas encontradas: ${campaigns.length}`);

    for (const campaign of campaigns) {
      let offset = 0; let totalFetched = 0;
      while (true) {
        try {
          const leadsRes = await axios.get(
            `https://api.lemlist.com/api/campaigns/${campaign._id}/leads`,
            { auth: { username: '', password: LEMLIST_API_KEY }, params: { limit: 100, offset } }
          );
          const leads = leadsRes.data || [];
          if (leads.length === 0) break;
          for (const lead of leads) extractLinkedInFromLead(lead, map);
          totalFetched += leads.length;
          if (leads.length < 100) break;
          offset += 100;
        } catch (err) { console.error(`   \u26A0\uFE0F ${campaign.name}: ${err.message}`); break; }
      }
      if (totalFetched > 0) console.log(`   \u2705 ${campaign.name}: ${totalFetched} leads`);
      await new Promise(r => setTimeout(r, 200));
    }

    lemlistEmailMap = map;
    lemlistMapBuiltAt = new Date();
    console.log(`\n\u2705 Mapa construido: ${Object.keys(map).length} leads con LinkedIn URL\n`);

  } catch (err) {
    console.error('\u274C Error construyendo mapa Lemlist:', err.message);
  }
}
// Resuelve el email de un lead dado su profileUrl de LinkedIn
function resolveEmailFromLinkedIn(profileUrl) {
  const normalized = normalizeLinkedinUrl(profileUrl);
  if (!normalized) return null;
  return lemlistEmailMap[normalized] || null;
}

// âââ FETCH PHANTOMBUSTER RESULTS âââââââââââââââââââââââââââââââââââââââââââââ

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

// âââ CLAUDE AI: GENERAR MENSAJES PERSONALIZADOS âââââââââââââââââââââââââââââââ

async function generatePersonalizedMessages(contact, postsAreRecent) {
  const { firstName, lastName, jobTitle, companyName, posts, profileUrl } = contact;

  let postsText;

  if (!postsAreRecent || !posts || posts.length === 0) {
    // Posts muy viejos o sin posts â contexto genÃ©rico
    postsText = `â ï¸  Sin actividad reciente disponible (posts >60 dÃ­as o sin posts)
â Genera mensajes basados en su cargo y empresa. Menciona el canal tradicional de forma genÃ©rica.
â NO inventes ni parafrasees posts especÃ­ficos que no tienes.`;
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

LO QUE HACE NATIVE (Ãºsalo selectivamente, nunca todo junto):
â¢ Visibilidad del 100% del punto de venta tradicional mediante Computer Vision
â¢ Detecta oportunidades de distribuciÃ³n, quiebre de stock y share of shelf en tiempo real
â¢ Convierte datos granulares (tienda por tienda, SKU por SKU) en decisiones de ejecuciÃ³n
â¢ Elimina puntos ciegos del canal: los equipos saben exactamente dÃ³nde y cuÃ¡ndo actuar
â¢ Clientes activos en MÃ©xico, Colombia, PerÃº, Chile, Ecuador (canal tradicional)

TU MISIÃN: escribir mensajes que parezcan escritos a mano por alguien que REALMENTE leyÃ³ sus posts.

PROCESO OBLIGATORIO antes de escribir:
1. Identifica el TEMA CENTRAL que mueve a esta persona (Â¿quÃ© lo/la apasiona? Â¿quÃ© problema menciona?)
2. Encuentra UNA frase, idea o dato especÃ­fico de sus posts que puedas mencionar literalmente
3. Detecta su tono (tÃ©cnico, inspiracional, operativo, estratÃ©gico) y espÃ©jalo
4. Conecta su preocupaciÃ³n real con el Ã¡ngulo mÃ¡s relevante de Native (sin mencionar Native aÃºn)

REGLAS DE ESCRITURA:
- Primera lÃ­nea: referencia directa y especÃ­fica a algo de sus posts (o, si no hay posts recientes, referencia a su cargo/industria de forma concreta)
- Email: mÃ¡x 120 palabras, sin bullets, fluido como conversaciÃ³n
- LinkedIn DM: mÃ¡x 75 palabras, mÃ¡s casual y directo
- Follow-ups: Ã¡ngulos distintos, no repetir el mismo gancho
- NUNCA empieces con "Vi tu post sobre..." â sÃ© mÃ¡s creativo
- NUNCA menciones "Native" en el primer contacto â solo genera curiosidad
- Idioma: detecta si escribe en espaÃ±ol o inglÃ©s y Ãºsalo

SEÃALES DE PERSONALIZACIÃN REAL (al menos UNA por mensaje):
â¢ Citar una frase textual o parafrasearla de forma reconocible
â¢ Referenciar un resultado o mÃ©trica que mencionÃ³
â¢ Mencionar un paÃ­s/mercado especÃ­fico que nombrÃ³
â¢ Aludir a un reto o aprendizaje que compartiÃ³`;

  const userPrompt = `PROSPECTO:
â¢ Nombre: ${firstName} ${lastName}
â¢ Cargo: ${jobTitle || 'No especificado'}
â¢ Empresa: ${companyName || 'No especificada'}
â¢ LinkedIn: ${profileUrl || 'N/A'}

âââââââââââââââââââââââââââââââââââââââ
ACTIVIDAD LINKEDIN RECIENTE (LEE CON ATENCIÃN):
âââââââââââââââââââââââââââââââââââââââ
${postsText}

âââââââââââââââââââââââââââââââââââââââ
ANÃLISIS PREVIO (piensa en voz alta antes de escribir):
Antes de generar los mensajes, incluye brevemente en tu respuesta JSON un campo "analysis" con:
- El tema central que identifiques
- La frase/dato especÃ­fico que usarÃ¡s como gancho
- El Ã¡ngulo de Native mÃ¡s relevante para este perfil

Luego genera los mensajes con exactamente estas claves:
âââââââââââââââââââââââââââââââââââââââ

{
  "analysis": {
    "centralTheme": "Â¿de quÃ© trata principalmente su actividad?",
    "hook": "la frase/dato especÃ­fico que usarÃ¡s",
    "nativeAngle": "quÃ© aspecto de Native conecta mejor con este perfil"
  },
  "customSubject": "asunto del email (mÃ¡x 55 chars, sin clickbait, que genere curiosidad real â puede referenciar algo de sus posts)",
  "customEmailBody": "cuerpo del email (mÃ¡x 120 palabras, primera lÃ­nea con referencia especÃ­fica a sus posts, segunda parte abre una pregunta o tensiÃ³n relevante para su rol, cierre con CTA suave)",
  "customLinkedinDm": "mensaje directo LinkedIn (mÃ¡x 75 palabras, tono mÃ¡s casual, como si ya se conocieran de haber leÃ­do sus posts, termina con pregunta abierta)",
  "customFollowup1": "follow-up 1 â dÃ­a 4 (mÃ¡x 80 palabras, Ã¡ngulo diferente: ahora sÃ­ puedes mencionar quÃ© hace Native de forma concisa, pero conectado a algo que Ã©l/ella mencionÃ³)",
  "customFollowup2": "follow-up 2 â dÃ­a 8 (mÃ¡x 55 palabras, muy breve, admite que no ha respondido con humor suave, deja la puerta abierta)"
}

Responde SOLO con el JSON vÃ¡lido, sin texto adicional fuera de Ã©l.`;

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

// âââ LEMLIST: ACTUALIZAR LEAD âââââââââââââââââââââââââââââââââââââââââââââââââ

async function updateLemlistLead(email, variables) {
  try {
    // PATCH /api/leads/:email/variables â correct Lemlist endpoint for custom variables
    const updateRes = await axios.patch(
      `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}/variables`,
      variables,
      { auth: { username: '', password: LEMLIST_API_KEY } }
    );
    return updateRes.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`   Lead no encontrado en Lemlist: ${email}`);
      if (err.response?.data) console.error(`   â Lemlist 404 detail:`, JSON.stringify(err.response.data));
      return null;
    }
    console.error(`   â Lemlist PATCH error ${err.response?.status}:`, err.response?.data || err.message);
    throw err;
  }
}

// âââ PROCESAMIENTO PRINCIPAL ââââââââââââââââââââââââââââââââââââââââââââââââââ

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

  console.log(`\nð Total contactos en resultados: ${Object.keys(contactMap).length}`);
  console.log(`â Ya procesados: ${Object.keys(processed).length}`);
  console.log(`ðºï¸  Leads en mapa LinkedInâEmail: ${Object.keys(lemlistEmailMap).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;

    // Resolver email: primero del CSV (vacÃ­o en Phantombuster), luego del mapa LinkedIn
    let email = contact.email;
    if (!email && contact.profileUrl) {
      email = resolveEmailFromLinkedIn(contact.profileUrl);
      if (email) {
        contact.email = email;
        console.log(`\nð Email resuelto para ${contact.firstName}: ${email}`);
      }
    }

    console.log(`\nð Procesando: ${contact.firstName} ${contact.lastName} | ${contact.profileUrl || email || 'sin ID'}`);

    // Verificar frescura de posts
    const postsAreRecent = hasRecentPosts(contact.posts, POST_FRESHNESS_DAYS);
    if (!postsAreRecent && contact.posts.length > 0) {
      console.log(`   â° Posts mÃ¡s antiguos de ${POST_FRESHNESS_DAYS} dÃ­as â usando mensaje genÃ©rico`);
    }

    try {
      // 1. Generar mensajes con Claude
      const messages = await generatePersonalizedMessages(contact, postsAreRecent);
      if (messages.analysis) {
        console.log(`   ð§  Tema: "${messages.analysis.centralTheme}"`);
        console.log(`   ðª Hook: "${messages.analysis.hook}"`);
        console.log(`   ð¯ Angulo Native: "${messages.analysis.nativeAngle}"`);
      }
      console.log(`   âï¸  Mensajes generados por Claude`);

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
            console.log(`   â Lemlist actualizado: ${email}`);
          } else {
            console.log(`   â ï¸  Lead no encontrado en Lemlist: ${email}`);
          }
        } catch (lemErr) {
          console.error(`   â Error actualizando Lemlist: ${lemErr.message}`);
        }
      } else {
        console.log(`   â ï¸  Sin email â no se actualizo Lemlist (profileUrl: ${contact.profileUrl})`);
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
      console.error(`   â Error procesando ${key}:`, err.message);
      if (err.response?.data) console.error(`   â API error detail:`, JSON.stringify(err.response.data));
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

// âââ RUTAS HTTP âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

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

// Webhook principal â Phantombuster llama aquÃ­ al terminar cada run
app.post('/webhook', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  console.log('\nð Webhook recibido de Phantombuster:', new Date().toISOString());
  res.json({ status: 'processing', message: 'Procesando resultados en background' });

  setImmediate(async () => {
    try {
      // Refrescar mapa de emails si tiene mÃ¡s de 6 horas
      const sixHours = 6 * 60 * 60 * 1000;
      if (!lemlistMapBuiltAt || (Date.now() - lemlistMapBuiltAt.getTime()) > sixHours) {
        await buildLemlistEmailMap();
      }

      const results = await fetchPhantombusterResults();
      if (!results || results.length === 0) {
        console.log('â ï¸  No se encontraron resultados CSV, intentando body del webhook...');
        if (req.body && Array.isArray(req.body.results)) {
          const stats = await processNewContacts(req.body.results);
          console.log(`\nâ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
        }
        return;
      }
      const stats = await processNewContacts(results);
      console.log(`\nâ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
    } catch (err) {
      console.error('â Error en procesamiento:', err.message);
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
    console.log('\nð§ Trigger manual de procesamiento...');
    const results = await fetchPhantombusterResults();
    if (!results) {
      return res.status(404).json({ error: 'No se encontraron resultados en Phantombuster' });
    }
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) {
    console.error('â Error:', err.message);
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

// Forzar reconstrucciÃ³n del mapa LinkedInâEmail
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

// Ver estadÃ­sticas de procesados
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

// Debug: obtener emails reales de Lemlist y probar PATCH variables
app.get('/debug-lemlist', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });

  try {
    // 1. Obtener campaÃ±as
    const campsRes = await axios.get('https://api.lemlist.com/api/campaigns',
      { auth: { username: '', password: LEMLIST_API_KEY } });
    const master = (campsRes.data || []).find(c => c.name === 'Master Campaign 2.0');
    if (!master) return res.json({ error: 'Master Campaign 2.0 not found', campaigns: (campsRes.data || []).map(c => c.name) });

    // 2. Obtener primeros 3 leads
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

// Debug: ver contactos GLOBALES de Lemlist (/api/leads)
app.get('/debug-contacts', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const resp = await axios.get('https://api.lemlist.com/api/leads', {
      auth: { username: '', password: LEMLIST_API_KEY },
      params: { limit: 20, offset: 0 }
    });
    const contacts = resp.data || [];
    const sample = contacts.slice(0, 5).map(c => ({
      email: c.email,
      firstName: c.firstName,
      lastName: c.lastName,
      allFields: Object.keys(c)
    }));
    res.json({ total: contacts.length, sample, rawFirst: contacts[0] || null });
  } catch (err) {
    res.status(500).json({ error: err.message, status: err.response?.status, detail: err.response?.data });
  }
});

// âââ START ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

app.listen(PORT, async () => {
  console.log(`\nð¯ Native Outbound Server corriendo en puerto ${PORT}`);
  console.log(`   Webhook URL:     POST /webhook?secret=${WEBHOOK_SECRET}`);
  console.log(`   Process URL:     POST /process?secret=${WEBHOOK_SECRET}`);
  console.log(`   Direct Process:  POST /process-direct?secret=${WEBHOOK_SECRET}`);
  console.log(`   Rebuild Map:     POST /rebuild-map?secret=${WEBHOOK_SECRET}`);
  console.log(`   Stats URL:       GET  /stats`);

  // Construir mapa LinkedInâEmail al iniciar
  await buildLemlistEmailMap();
});
