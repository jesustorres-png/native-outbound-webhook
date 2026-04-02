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

// ─── FETCH PHANTOMBUSTER RESULTS ─────────────────────────────────────────────

async function fetchPhantombusterResults() {
  const url = `https://api.phantombuster.com/api/v2/agents/fetch-output?id=${PHANTOM_AGENT_ID}`;
  const res = await axios.get(url, {
    headers: { 'X-Phantombuster-Org': PHANTOMBUSTER_ORG }
  });

  const output = res.data.output || '';
  const csvUrlMatch = output.match(/https:\/\/phantombuster\.s3[^\s"]+\.csv/);
  if (!csvUrlMatch) {
    console.log('No CSV URL found in output');
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
    const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
    const obj = {};
    headers.forEach((h, i) => { obj[h] = values[i] || ''; });
    return obj;
  });
}

// ─── CLAUDE AI: GENERAR MENSAJES PERSONALIZADOS ───────────────────────────────

async function generatePersonalizedMessages(contact) {
  const { firstName, lastName, jobTitle, companyName, posts, profileUrl } = contact;

  // Construir contexto rico de cada post con métricas de engagement
  const postsText = posts.map((p, i) => {
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

  const systemPrompt = `Eres un SDR senior especializado en ventas B2B consultivas para el canal tradicional (retail tradicional / trade) en LATAM.

Representas a Native, plataforma de Computer Vision + AI Agents para marcas FMCG/CPG.

LO QUE HACE NATIVE (usalo selectivamente, nunca todo junto):
- Visibilidad del 100% del punto de venta tradicional mediante Computer Vision
- Detecta oportunidades de distribucion, quiebre de stock y share of shelf en tiempo real
- Convierte datos granulares (tienda por tienda, SKU por SKU) en decisiones de ejecucion
- Elimina puntos ciegos del canal: los equipos saben exactamente donde y cuando actuar
- Clientes activos en Mexico, Colombia, Peru, Chile, Ecuador (canal tradicional)

TU MISION: escribir mensajes que parezcan escritos a mano por alguien que REALMENTE leyo sus posts.

PROCESO OBLIGATORIO antes de escribir:
1. Identifica el TEMA CENTRAL que mueve a esta persona (que le apasiona, que problema menciona)
2. Encuentra UNA frase, idea o dato especifico de sus posts que puedas mencionar literalmente
3. Detecta su tono (tecnico, inspiracional, operativo, estrategico) y espejalo
4. Conecta su preocupacion real con el angulo mas relevante de Native (sin mencionar Native aun)

REGLAS DE ESCRITURA:
- Primera linea: referencia directa y especifica a algo de sus posts (no generico)
- Email: max 120 palabras, sin bullets, fluido como conversacion
- LinkedIn DM: max 75 palabras, mas casual y directo
- Follow-ups: angulos distintos, no repetir el mismo gancho
- NUNCA empieces con "Vi tu post sobre..." — se mas creativo
- NUNCA menciones "Native" en el primer contacto — solo genera curiosidad
- Idioma: detecta si escribe en espanol o ingles y usalo

SENALES DE PERSONALIZACION REAL (al menos UNA por mensaje):
- Citar una frase textual o parafrasearla de forma reconocible
- Referenciar un resultado o metrica que menciono
- Mencionar un pais/mercado especifico que nombro
- Aludir a un reto o aprendizaje que compartio`;

  const userPrompt = `PROSPECTO:
- Nombre: ${firstName} ${lastName}
- Cargo: ${jobTitle || 'No especificado'}
- Empresa: ${companyName || 'No especificada'}
- LinkedIn: ${profileUrl || 'N/A'}

ACTIVIDAD LINKEDIN RECIENTE (LEE CON ATENCION):
${postsText || 'Sin posts disponibles — genera mensajes basados en su cargo y empresa'}

INSTRUCCION: Antes de escribir los mensajes, haz un analisis breve en el campo "analysis". Luego genera los 5 mensajes.

Responde SOLO con este JSON valido:
{
  "analysis": {
    "centralTheme": "tema central de su actividad",
    "hook": "frase o dato especifico que usaras como gancho",
    "nativeAngle": "aspecto de Native que mejor conecta con este perfil"
  },
  "customSubject": "asunto del email (max 55 chars, genera curiosidad real basada en sus posts)",
  "customEmailBody": "cuerpo del email (max 120 palabras, primera linea con referencia especifica a sus posts, abre tension relevante para su rol, cierre con CTA suave)",
  "customLinkedinDm": "mensaje directo LinkedIn (max 75 palabras, tono casual como si hubiera leido sus posts, termina con pregunta abierta)",
  "customFollowup1": "follow-up dia 4 (max 80 palabras, angulo diferente: menciona que hace Native de forma concisa conectado a algo que el/ella menciono)",
  "customFollowup2": "follow-up dia 8 (max 55 palabras, muy breve, tono humano, deja la puerta abierta)"
}`;

  const response = await axios.post(
    'https://api.anthropic.com/v1/messages',
    {
      model: 'claude-sonnet-4-6',
      max_tokens: 1800,
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
  const res = await axios.get(
    `https://api.lemlist.com/api/leads/${encodeURIComponent(email)}`,
    { auth: { username: '', password: LEMLIST_API_KEY } }
  );

  if (!res.data) {
    console.log(`Lead no encontrado en Lemlist: ${email}`);
    return null;
  }

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

  const contactMap = {};
  for (const row of results) {
    const profileUrl = row.profileUrl || row.profile_url || row['Profile Url'];
    const email = row.email || row.Email;
    const key = email || profileUrl;
    if (!key) continue;

    if (!contactMap[key]) {
      const authorFull  = row.Author || row.author || '';
      const authorParts = authorFull.split(' ');
      contactMap[key] = {
        email,
        profileUrl: profileUrl || row['Author Url'] || row.authorUrl || '',
        firstName:   row.firstName   || row.first_name   || authorParts[0] || '',
        lastName:    row.lastName    || row.last_name    || authorParts.slice(1).join(' ') || '',
        jobTitle:    row.jobTitle    || row.job_title    || row['Job Title'] || '',
        companyName: row.companyName || row.company_name || row['Company'] || '',
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

  console.log(`\n Total contactos en resultados: ${Object.keys(contactMap).length}`);
  console.log(`Ya procesados: ${Object.keys(processed).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;

    console.log(`\nProcesando: ${contact.firstName} ${contact.lastName} (${contact.email || contact.profileUrl})`);

    try {
      const messages = await generatePersonalizedMessages(contact);

      if (messages.analysis) {
        console.log(`  Tema: "${messages.analysis.centralTheme}"`);
        console.log(`  Hook: "${messages.analysis.hook}"`);
        console.log(`  Angulo Native: "${messages.analysis.nativeAngle}"`);
      }
      console.log(`  Mensajes generados por Claude`);

      if (contact.email) {
        try {
          const lemlistResult = await updateLemlistLead(contact.email, {
            customSubject:        messages.customSubject,
            customEmailBody:      messages.customEmailBody,
            customLinkedinDm:     messages.customLinkedinDm,
            customFollowup1:      messages.customFollowup1,
            customFollowup2:      messages.customFollowup2,
            customPersonalHook:   messages.analysis?.hook        || '',
            customNativeAngle:    messages.analysis?.nativeAngle || '',
            linkedinActivityProcessed: new Date().toISOString()
          });

          if (lemlistResult) {
            console.log(`  Lemlist actualizado: ${contact.email}`);
          } else {
            console.log(`  Lead no encontrado en Lemlist: ${contact.email}`);
          }
        } catch (lemErr) {
          console.log(`  Lemlist error (no critico): ${lemErr.message}`);
        }
      }

      processed[key] = {
        processedAt: new Date().toISOString(),
        name: `${contact.firstName} ${contact.lastName}`,
        postsCount: contact.posts.length,
        hook: messages.analysis?.hook || ''
      };

      newCount++;
      await new Promise(r => setTimeout(r, 1200));

    } catch (err) {
      console.error(`  Error procesando ${key}:`, err.message);
      errorCount++;
    }
  }

  saveProcessed(processed);
  return { newCount, errorCount, totalContacts: Object.keys(contactMap).length };
}

// ─── RUTAS HTTP ───────────────────────────────────────────────────────────────

app.get('/', (req, res) => {
  const processed = loadProcessed();
  res.json({
    status: 'ok',
    service: 'Native Outbound',
    ts: new Date().toISOString(),
    totalProcessed: Object.keys(processed).length
  });
});

app.post('/webhook', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  console.log('\nWebhook recibido de Phantombuster:', new Date().toISOString());
  res.json({ status: 'processing', message: 'Procesando en background' });

  setImmediate(async () => {
    try {
      const results = await fetchPhantombusterResults();
      if (!results || results.length === 0) {
        if (req.body && Array.isArray(req.body.results)) {
          const stats = await processNewContacts(req.body.results);
          console.log(`Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores`);
        }
        return;
      }
      const stats = await processNewContacts(results);
      console.log(`Completado: ${stats.newCount} nuevos, ${stats.errorCount} errores`);
    } catch (err) {
      console.error('Error en procesamiento:', err.message);
    }
  });
});

app.post('/process', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const results = await fetchPhantombusterResults();
    if (!results) {
      return res.status(404).json({ error: 'No se encontraron resultados en Phantombuster' });
    }
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

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

app.get('/stats', (req, res) => {
  const processed = loadProcessed();
  res.json({
    totalProcessed: Object.keys(processed).length,
    processed
  });
});

app.listen(PORT, () => {
  console.log(`Native Outbound Server en puerto ${PORT}`);
  console.log(`Webhook: POST /webhook?secret=${WEBHOOK_SECRET}`);
  console.log(`Process: POST /process?secret=${WEBHOOK_SECRET}`);
  console.log(`Stats:   GET  /stats`);
});
