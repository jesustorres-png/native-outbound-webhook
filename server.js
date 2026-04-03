/**
 * Webhook Server: Phantombuster Ã¢ÂÂ Claude AI Ã¢ÂÂ Lemlist
 * Genera mensajes outbound personalizados a partir de actividad LinkedIn
 */

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ CONFIG (se leen desde variables de entorno) Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
const ANTHROPIC_API_KEY  = process.env.ANTHROPIC_API_KEY;
const LEMLIST_API_KEY    = process.env.LEMLIST_API_KEY;
const PHANTOMBUSTER_ORG  = process.env.PHANTOMBUSTER_ORG  || '4237829874326193';
const PHANTOM_AGENT_ID   = process.env.PHANTOM_AGENT_ID   || '5621422771951702';
const WEBHOOK_SECRET     = process.env.WEBHOOK_SECRET     || 'native-outbound-2026';
const PORT               = process.env.PORT               || 3000;

// POST_FRESHNESS_DAYS: posts mÃÂ¡s antiguos que esto se tratan como "sin contexto reciente"
const POST_FRESHNESS_DAYS = parseInt(process.env.POST_FRESHNESS_DAYS || '60');

// Archivo local para trackear contactos ya procesados
const PROCESSED_FILE = path.join(__dirname, 'processed_contacts.json');

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ HELPERS Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

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

// Normaliza una URL de LinkedIn para comparaciÃÂ³n: extrae "linkedin.com/in/username"
function normalizeLinkedinUrl(url) {
  if (!url || typeof url !== 'string') return '';
  const match = url.toLowerCase().match(/linkedin\.com\/in\/([^/?#\s]+)/);
  if (match) return `linkedin.com/in/${match[1].replace(/\/$/, '')}`;
  return '';
}

// Calcula los dÃÂ­as de antigÃÂ¼edad de una fecha
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

// Devuelve true si al menos un post es mÃÂ¡s reciente que maxDaysOld
function hasRecentPosts(posts, maxDaysOld) {
  if (!posts || posts.length === 0) return false;
  return posts.some(p => daysAgo(p.postDate) <= maxDaysOld);
}

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ LEMLIST EMAIL MAP (LinkedIn URL Ã¢ÂÂ email) Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

// Cache para evitar llamadas repetidas al API de contactos
const contactEmailCache = {};

// Busca un contacto en LemCRM por LinkedIn URL usando la API de contactos
// Endpoint: GET https://app.lemlist.com/api/contacts?apiKey=KEY&search=SLUG
async function findContactByLinkedIn(profileUrl) {
  const normalized = normalizeLinkedinUrl(profileUrl);
  if (!normalized) return null;

  // Cache hit (null tambien se cachea para evitar re-intentos)
  if (Object.prototype.hasOwnProperty.call(contactEmailCache, normalized)) {
    return contactEmailCache[normalized];
  }

  // Extraer el slug: "linkedin.com/in/john-doe" -> "john-doe"
  const slug = normalized.replace('linkedin.com/in/', '');

  try {
    const resp = await axios.get('https://app.lemlist.com/api/contacts', {
      params: { apiKey: LEMLIST_API_KEY, search: slug, limit: 10 },
      timeout: 10000
    });
    const contacts = Array.isArray(resp.data) ? resp.data : [];

    let found = null;
    for (const c of contacts) {
      const cLinkedin = c.linkedinUrl || c.linkedin || c.linkedInUrl || c.linkedinProfile || '';
      if (normalizeLinkedinUrl(cLinkedin) === normalized) {
        found = c;
        break;
      }
    }
    // Fallback: unico resultado en la busqueda
    if (!found && contacts.length === 1) found = contacts[0];

    const email = found?.email || null;
    contactEmailCache[normalized] = email;
    if (email) console.log('   contacto encontrado en LemCRM: ' + email);
    return email;
  } catch (err) {
    console.error('   findContactByLinkedIn error:', err.message);
    return null;
  }
}

// Resuelve el email de un lead dado su profileUrl de LinkedIn
async function resolveEmailFromLinkedIn(profileUrl) {
  return await findContactByLinkedIn(profileUrl);
}

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ FETCH PHANTOMBUSTER RESULTS Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

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

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ CLAUDE AI: GENERAR MENSAJES PERSONALIZADOS Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

async function generatePersonalizedMessages(contact, postsAreRecent) {
  const { firstName, lastName, jobTitle, companyName, posts, profileUrl } = contact;

  let postsText;

  if (!postsAreRecent || !posts || posts.length === 0) {
    // Posts muy viejos o sin posts Ã¢ÂÂ contexto genÃÂ©rico
    postsText = `Ã¢ÂÂ Ã¯Â¸Â  Sin actividad reciente disponible (posts >60 dÃÂ­as o sin posts)
Ã¢ÂÂ Genera mensajes basados en su cargo y empresa. Menciona el canal tradicional de forma genÃÂ©rica.
Ã¢ÂÂ NO inventes ni parafrasees posts especÃÂ­ficos que no tienes.`;
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

LO QUE HACE NATIVE (ÃÂºsalo selectivamente, nunca todo junto):
Ã¢ÂÂ¢ Visibilidad del 100% del punto de venta tradicional mediante Computer Vision
Ã¢ÂÂ¢ Detecta oportunidades de distribuciÃÂ³n, quiebre de stock y share of shelf en tiempo real
Ã¢ÂÂ¢ Convierte datos granulares (tienda por tienda, SKU por SKU) en decisiones de ejecuciÃÂ³n
Ã¢ÂÂ¢ Elimina puntos ciegos del canal: los equipos saben exactamente dÃÂ³nde y cuÃÂ¡ndo actuar
Ã¢ÂÂ¢ Clientes activos en MÃÂ©xico, Colombia, PerÃÂº, Chile, Ecuador (canal tradicional)

TU MISIÃÂN: escribir mensajes que parezcan escritos a mano por alguien que REALMENTE leyÃÂ³ sus posts.

PROCESO OBLIGATORIO antes de escribir:
1. Identifica el TEMA CENTRAL que mueve a esta persona (ÃÂ¿quÃÂ© lo/la apasiona? ÃÂ¿quÃÂ© problema menciona?)
2. Encuentra UNA frase, idea o dato especÃÂ­fico de sus posts que puedas mencionar literalmente
3. Detecta su tono (tÃÂ©cnico, inspiracional, operativo, estratÃÂ©gico) y espÃÂ©jalo
4. Conecta su preocupaciÃÂ³n real con el ÃÂ¡ngulo mÃÂ¡s relevante de Native (sin mencionar Native aÃÂºn)

REGLAS DE ESCRITURA:
- Primera lÃÂ­nea: referencia directa y especÃÂ­fica a algo de sus posts (o, si no hay posts recientes, referencia a su cargo/industria de forma concreta)
- Email: mÃÂ¡x 120 palabras, sin bullets, fluido como conversaciÃÂ³n
- LinkedIn DM: mÃÂ¡x 75 palabras, mÃÂ¡s casual y directo
- Follow-ups: ÃÂ¡ngulos distintos, no repetir el mismo gancho
- NUNCA empieces con "Vi tu post sobre..." Ã¢ÂÂ sÃÂ© mÃÂ¡s creativo
- NUNCA menciones "Native" en el primer contacto Ã¢ÂÂ solo genera curiosidad
- Idioma: detecta si escribe en espaÃÂ±ol o inglÃÂ©s y ÃÂºsalo

SEÃÂALES DE PERSONALIZACIÃÂN REAL (al menos UNA por mensaje):
Ã¢ÂÂ¢ Citar una frase textual o parafrasearla de forma reconocible
Ã¢ÂÂ¢ Referenciar un resultado o mÃÂ©trica que mencionÃÂ³
Ã¢ÂÂ¢ Mencionar un paÃÂ­s/mercado especÃÂ­fico que nombrÃÂ³
Ã¢ÂÂ¢ Aludir a un reto o aprendizaje que compartiÃÂ³`;

  const userPrompt = `PROSPECTO:
Ã¢ÂÂ¢ Nombre: ${firstName} ${lastName}
Ã¢ÂÂ¢ Cargo: ${jobTitle || 'No especificado'}
Ã¢ÂÂ¢ Empresa: ${companyName || 'No especificada'}
Ã¢ÂÂ¢ LinkedIn: ${profileUrl || 'N/A'}

Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
ACTIVIDAD LINKEDIN RECIENTE (LEE CON ATENCIÃÂN):
Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
${postsText}

Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
ANÃÂLISIS PREVIO (piensa en voz alta antes de escribir):
Antes de generar los mensajes, incluye brevemente en tu respuesta JSON un campo "analysis" con:
- El tema central que identifiques
- La frase/dato especÃÂ­fico que usarÃÂ¡s como gancho
- El ÃÂ¡ngulo de Native mÃÂ¡s relevante para este perfil

Luego genera los mensajes con exactamente estas claves:
Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

{
  "analysis": {
    "centralTheme": "ÃÂ¿de quÃÂ© trata principalmente su actividad?",
    "hook": "la frase/dato especÃÂ­fico que usarÃÂ¡s",
    "nativeAngle": "quÃÂ© aspecto de Native conecta mejor con este perfil"
  },
  "customSubject": "asunto del email (mÃÂ¡x 55 chars, sin clickbait, que genere curiosidad real Ã¢ÂÂ puede referenciar algo de sus posts)",
  "customEmailBody": "cuerpo del email (mÃÂ¡x 120 palabras, primera lÃÂ­nea con referencia especÃÂ­fica a sus posts, segunda parte abre una pregunta o tensiÃÂ³n relevante para su rol, cierre con CTA suave)",
  "customLinkedinDm": "mensaje directo LinkedIn (mÃÂ¡x 75 palabras, tono mÃÂ¡s casual, como si ya se conocieran de haber leÃÂ­do sus posts, termina con pregunta abierta)",
  "customFollowup1": "follow-up 1 Ã¢ÂÂ dÃÂ­a 4 (mÃÂ¡x 80 palabras, ÃÂ¡ngulo diferente: ahora sÃÂ­ puedes mencionar quÃÂ© hace Native de forma concisa, pero conectado a algo que ÃÂ©l/ella mencionÃÂ³)",
  "customFollowup2": "follow-up 2 Ã¢ÂÂ dÃÂ­a 8 (mÃÂ¡x 55 palabras, muy breve, admite que no ha respondido con humor suave, deja la puerta abierta)"
}

Responde SOLO con el JSON vÃÂ¡lido, sin texto adicional fuera de ÃÂ©l.`;

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

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ LEMLIST: ACTUALIZAR LEAD Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

async function updateLemlistLead(email, variables) {
  try {
    // PATCH /api/leads/:email/variables Ã¢ÂÂ correct Lemlist endpoint for custom variables
    const updateRes = await axios.patch(
      `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}/variables`,
      variables,
      { auth: { username: '', password: LEMLIST_API_KEY } }
    );
    return updateRes.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`   Lead no encontrado en Lemlist: ${email}`);
      if (err.response?.data) console.error(`   Ã¢ÂÂ Lemlist 404 detail:`, JSON.stringify(err.response.data));
      return null;
    }
    console.error(`   Ã¢ÂÂ Lemlist PATCH error ${err.response?.status}:`, err.response?.data || err.message);
    throw err;
  }
}

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ PROCESAMIENTO PRINCIPAL Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

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

  console.log(`\nÃ°ÂÂÂ Total contactos en resultados: ${Object.keys(contactMap).length}`);
  console.log(`Ã¢ÂÂ Ya procesados: ${Object.keys(processed).length}`);
  console.log(`🗺️  Contactos en cache LinkedIn→Email: ${Object.keys(contactEmailCache).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;

    // Resolver email: primero del CSV (vacÃÂ­o en Phantombuster), luego del mapa LinkedIn
    let email = contact.email;
    if (!email && contact.profileUrl) {
      email = await resolveEmailFromLinkedIn(contact.profileUrl);
      if (email) {
        contact.email = email;
        console.log(`\nÃ°ÂÂÂ Email resuelto para ${contact.firstName}: ${email}`);
      }
    }

    console.log(`\nÃ°ÂÂÂ Procesando: ${contact.firstName} ${contact.lastName} | ${contact.profileUrl || email || 'sin ID'}`);

    // Verificar frescura de posts
    const postsAreRecent = hasRecentPosts(contact.posts, POST_FRESHNESS_DAYS);
    if (!postsAreRecent && contact.posts.length > 0) {
      console.log(`   Ã¢ÂÂ° Posts mÃÂ¡s antiguos de ${POST_FRESHNESS_DAYS} dÃÂ­as Ã¢ÂÂ usando mensaje genÃÂ©rico`);
    }

    try {
      // 1. Generar mensajes con Claude
      const messages = await generatePersonalizedMessages(contact, postsAreRecent);
      if (messages.analysis) {
        console.log(`   Ã°ÂÂ§Â  Tema: "${messages.analysis.centralTheme}"`);
        console.log(`   Ã°ÂÂªÂ Hook: "${messages.analysis.hook}"`);
        console.log(`   Ã°ÂÂÂ¯ Angulo Native: "${messages.analysis.nativeAngle}"`);
      }
      console.log(`   Ã¢ÂÂÃ¯Â¸Â  Mensajes generados por Claude`);

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
            console.log(`   Ã¢ÂÂ Lemlist actualizado: ${email}`);
          } else {
            console.log(`   Ã¢ÂÂ Ã¯Â¸Â  Lead no encontrado en Lemlist: ${email}`);
          }
        } catch (lemErr) {
          console.error(`   Ã¢ÂÂ Error actualizando Lemlist: ${lemErr.message}`);
        }
      } else {
        console.log(`   Ã¢ÂÂ Ã¯Â¸Â  Sin email Ã¢ÂÂ no se actualizo Lemlist (profileUrl: ${contact.profileUrl})`);
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
      console.error(`   Ã¢ÂÂ Error procesando ${key}:`, err.message);
      if (err.response?.data) console.error(`   Ã¢ÂÂ API error detail:`, JSON.stringify(err.response.data));
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

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ RUTAS HTTP Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

// Health check
app.get('/', (req, res) => {
  const processed = loadProcessed();
  res.json({
    status: 'ok',
    service: 'Native Outbound',
    ts: new Date().toISOString(),
    totalProcessed: Object.keys(processed).length,
    contactCacheSize: Object.keys(contactEmailCache).length,
    lemlistMapBuiltAt: lemlistMapBuiltAt ? lemlistMapBuiltAt.toISOString() : null
  });
});

// Webhook principal Ã¢ÂÂ Phantombuster llama aquÃÂ­ al terminar cada run
app.post('/webhook', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  console.log('\nÃ°ÂÂÂ Webhook recibido de Phantombuster:', new Date().toISOString());
  res.json({ status: 'processing', message: 'Procesando resultados en background' });

  setImmediate(async () => {
    try {
      const results = await fetchPhantombusterResults();
      if (!results || results.length === 0) {
        console.log('Ã¢ÂÂ Ã¯Â¸Â  No se encontraron resultados CSV, intentando body del webhook...');
        if (req.body && Array.isArray(req.body.results)) {
          const stats = await processNewContacts(req.body.results);
          console.log(`\nÃ¢ÂÂ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
        }
        return;
      }
      const stats = await processNewContacts(results);
      console.log(`\nÃ¢ÂÂ Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores, ${stats.noEmailCount} sin email`);
    } catch (err) {
      console.error('Ã¢ÂÂ Error en procesamiento:', err.message);
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
    console.log('\nÃ°ÂÂÂ§ Trigger manual de procesamiento...');
    const results = await fetchPhantombusterResults();
    if (!results) {
      return res.status(404).json({ error: 'No se encontraron resultados en Phantombuster' });
    }
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) {
    console.error('Ã¢ÂÂ Error:', err.message);
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

// Limpiar cache de contactos (fuerza re-busqueda en el proximo procesamiento)
app.post('/rebuild-map', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const prev = Object.keys(contactEmailCache).length;
  Object.keys(contactEmailCache).forEach(k => delete contactEmailCache[k]);
  res.json({
    success: true,
    message: 'Cache limpiado (' + prev + ' entradas eliminadas). Los contactos se buscaran on-demand.',
    cacheSize: 0
  });
});



// Ver estadÃÂ­sticas de procesados
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
    
    contacts: processed
  });
});

// Debug: obtener emails reales de Lemlist y probar PATCH variables
app.get('/debug-lemlist', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });

  try {
    // 1. Obtener campaÃÂ±as
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

// Debug: buscar contacto en LemCRM por termino o LinkedIn URL
// GET /debug-contacts?secret=...&search=john-doe
app.get('/debug-contacts', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const searchTerm = req.query.search || req.query.linkedin || 'a';
  try {
    const resp = await axios.get('https://app.lemlist.com/api/contacts', {
      params: { apiKey: LEMLIST_API_KEY, search: searchTerm, limit: 5 },
      timeout: 10000
    });
    const contacts = Array.isArray(resp.data) ? resp.data : resp.data;
    const sample = Array.isArray(contacts) ? contacts.slice(0, 5).map(c => ({
      email: c.email, firstName: c.firstName, lastName: c.lastName,
      linkedinUrl: c.linkedinUrl || c.linkedin || c.linkedInUrl || null,
      allFields: Object.keys(c)
    })) : contacts;
    res.json({ searchTerm, total: Array.isArray(contacts) ? contacts.length : 1, sample, rawFirst: Array.isArray(contacts) ? (contacts[0] || null) : contacts });
  } catch (err) {
    res.status(500).json({ error: err.message, status: err.response?.status, detail: err.response?.data });
  }
});


// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂ START Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

app.listen(PORT, async () => {
  console.log(`\nÃ°ÂÂÂ¯ Native Outbound Server corriendo en puerto ${PORT}`);
  console.log(`   Webhook URL:     POST /webhook?secret=${WEBHOOK_SECRET}`);
  console.log(`   Process URL:     POST /process?secret=${WEBHOOK_SECRET}`);
  console.log(`   Direct Process:  POST /process-direct?secret=${WEBHOOK_SECRET}`);
  console.log(`   Rebuild Map:     POST /rebuild-map?secret=${WEBHOOK_SECRET}`);
  console.log(`   Stats URL:       GET  /stats`);

  console.log('   Contact lookup: ON-DEMAND via LemCRM API');
});
