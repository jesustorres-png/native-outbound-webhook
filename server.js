/**
 * Webhook Server: Phantombuster -> Claude AI -> Lemlist
 * Genera mensajes outbound personalizados a partir de actividad LinkedIn
 */

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const ANTHROPIC_API_KEY  = process.env.ANTHROPIC_API_KEY;
const LEMLIST_API_KEY    = process.env.LEMLIST_API_KEY;
const PHANTOMBUSTER_ORG  = process.env.PHANTOMBUSTER_ORG  || '4237829874326193';
const PHANTOM_AGENT_ID   = process.env.PHANTOM_AGENT_ID   || '5621422771951702';
const WEBHOOK_SECRET     = process.env.WEBHOOK_SECRET     || 'native-outbound-2026';
const PORT               = process.env.PORT               || 3000;

const PROCESSED_FILE = path.join(__dirname, 'processed_contacts.json');

function loadProcessed() {
  try { return JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8')); }
  catch { return {}; }
}

function saveProcessed(data) {
  fs.writeFileSync(PROCESSED_FILE, JSON.stringify(data, null, 2));
}

async function fetchPhantombusterResults() {
  const url = `https://api.phantombuster.com/api/v2/agents/fetch-output?id=${PHANTOM_AGENT_ID}`;
  const res = await axios.get(url, { headers: { 'X-Phantombuster-Org': PHANTOMBUSTER_ORG } });
  const output = res.data.output || '';
  const csvUrlMatch = output.match(/https:\/\/phantombuster\.s3[^\s"]+\.csv/);
  if (!csvUrlMatch) return null;
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

async function generatePersonalizedMessages(contact) {
  const { firstName, lastName, jobTitle, companyName, posts } = contact;
  const postsText = posts.map((p, i) =>
    `Post ${i + 1} (${p.postDate || 'reciente'}):\n"${p.postContent?.substring(0, 500) || ''}"`
  ).join('\n\n');

  const systemPrompt = `Eres un experto en ventas B2B consultivas para el canal tradicional en LATAM.
Trabajas para Native, plataforma de Computer Vision + AI Agents para marcas CPG/FMCG.
Genera mensajes outbound HIPER-personalizados.
REGLAS: usa el idioma del prospecto, referencia su actividad LinkedIn directamente,
email < 120 palabras, DM < 80 palabras, tono profesional pero humano,
NO menciones "Native" directamente en el primer contacto.`;

  const userPrompt = `Prospecto: ${firstName} ${lastName}, ${jobTitle || ''} en ${companyName || ''}
Actividad LinkedIn: ${postsText || 'Sin posts recientes'}
Genera JSON con claves: customSubject, customEmailBody, customLinkedinDm, customFollowup1, customFollowup2`;

  const response = await axios.post('https://api.anthropic.com/v1/messages', {
    model: 'claude-sonnet-4-6',
    max_tokens: 1500,
    messages: [{ role: 'user', content: userPrompt }],
    system: systemPrompt
  }, {
    headers: {
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json'
    }
  });

  const text = response.data.content[0].text;
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error('Claude no devolvio JSON valido');
  return JSON.parse(jsonMatch[0]);
}

async function updateLemlistLead(email, variables) {
  try {
    await axios.get(`https://api.lemlist.com/api/leads/${encodeURIComponent(email)}`,
      { auth: { username: '', password: LEMLIST_API_KEY } });
    const res = await axios.patch(`https://api.lemlist.com/api/leads/${encodeURIComponent(email)}`,
      variables, { auth: { username: '', password: LEMLIST_API_KEY } });
    return res.data;
  } catch (err) {
    if (err.response?.status === 404) return null;
    throw err;
  }
}

async function processNewContacts(results) {
  const processed = loadProcessed();
  let newCount = 0, errorCount = 0;
  const contactMap = {};

  for (const row of results) {
    const email = row.email || row.Email;
    const profileUrl = row.profileUrl || row.profile_url || row['Profile Url'];
    const key = email || profileUrl;
    if (!key) continue;
    if (!contactMap[key]) {
      contactMap[key] = {
        email, profileUrl,
        firstName: row.firstName || row['Author']?.split(' ')[0] || '',
        lastName: row.lastName || '',
        jobTitle: row.jobTitle || '',
        companyName: row.companyName || '',
        posts: []
      };
    }
    const postContent = row.postContent || row['Post Content'] || '';
    if (postContent) {
      contactMap[key].posts.push({
        postContent,
        postDate: row.postDate || row['Post Date'] || '',
        postUrl: row.postUrl || row['Post Url'] || ''
      });
    }
  }

  console.log(`Total contactos: ${Object.keys(contactMap).length}, ya procesados: ${Object.keys(processed).length}`);

  for (const [key, contact] of Object.entries(contactMap)) {
    if (processed[key]) continue;
    console.log(`Procesando: ${contact.firstName} ${contact.lastName}`);
    try {
      const messages = await generatePersonalizedMessages(contact);
      if (contact.email) {
        const r = await updateLemlistLead(contact.email, {
          customSubject: messages.customSubject,
          customEmailBody: messages.customEmailBody,
          customLinkedinDm: messages.customLinkedinDm,
          customFollowup1: messages.customFollowup1,
          customFollowup2: messages.customFollowup2,
          linkedinActivityProcessed: new Date().toISOString()
        });
        console.log(r ? `Lemlist actualizado: ${contact.email}` : `Lead no encontrado: ${contact.email}`);
      }
      processed[key] = { processedAt: new Date().toISOString(), name: `${contact.firstName} ${contact.lastName}` };
      newCount++;
      await new Promise(r => setTimeout(r, 1000));
    } catch (err) {
      console.error(`Error: ${key}: ${err.message}`);
      errorCount++;
    }
  }
  saveProcessed(processed);
  return { newCount, errorCount, total: Object.keys(contactMap).length };
}

app.get('/', (req, res) => res.json({ status: 'ok', service: 'Native Outbound', ts: new Date().toISOString() }));

app.post('/webhook', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  console.log('Webhook recibido:', new Date().toISOString());
  res.json({ status: 'processing' });
  setImmediate(async () => {
    try {
      const results = await fetchPhantombusterResults();
      if (results) { const s = await processNewContacts(results); console.log('Done:', s); }
    } catch (err) { console.error('Error webhook:', err.message); }
  });
});

app.post('/process', async (req, res) => {
  const secret = req.headers['x-webhook-secret'] || req.query.secret;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const results = await fetchPhantombusterResults();
    if (!results) return res.status(404).json({ error: 'No results found' });
    const stats = await processNewContacts(results);
    res.json({ success: true, ...stats });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/stats', (req, res) => {
  const p = loadProcessed();
  res.json({ totalProcessed: Object.keys(p).length, processed: p });
});

app.listen(PORT, () => console.log(`Native Outbound Server on port ${PORT}. Webhook: POST /webhook?secret=${WEBHOOK_SECRET}`));
