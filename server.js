const express = require("express");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const cors = require("cors");

const app = express();
app.use(express.json({ limit: '30mb' }));
app.use(cors());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const APP_ID = process.env.APP_ID;
const APP_SECRET = process.env.APP_SECRET;
const PORT = process.env.PORT || 3000;

// ═══════════════════════════════════════
// 1. LOGS CONDICIONAIS (desligue em produção para ganhar velocidade)
// ═══════════════════════════════════════
const DEBUG = process.env.DEBUG === 'true';
function log(...args) { if (DEBUG) console.log(...args); }
function errorLog(...args) { console.error(...args); } // erros sempre aparecem

let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  log("✅ Supabase conectado!");
}

// ── Multi-tenant: identifica o usuário logado (dono) ──
const SUPABASE_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlmcnhneWhreWdxaGpyd3Brc3J5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg1Mjk4MDcsImV4cCI6MjA5NDEwNTgwN30.6vjbaJdWk-u55xegMrHnv64pvlo0DByfPdtDSj2C7z4';
const _tokenOwner = {};
async function resolveOwner(req) {
  const a = req.headers.authorization || '';
  const tok = a.startsWith('Bearer ') ? a.slice(7) : null;
  if (!tok || !SUPABASE_URL) return null;
  const c = _tokenOwner[tok];
  if (c && Date.now() - c.ts < 300000) return c.email;
  try {
    const r = await axios.get(`${SUPABASE_URL}/auth/v1/user`, { headers: { Authorization: 'Bearer ' + tok, apikey: SUPABASE_ANON } });
    const email = (r.data?.email || '').toLowerCase() || null;
    if (email) _tokenOwner[tok] = { email, ts: Date.now() };
    return email;
  } catch (e) { return null; }
}
app.use(async (req, res, next) => { try { req.owner = await resolveOwner(req); } catch (_) { req.owner = null; } next(); });

app.get("/", (req, res) => res.send("✅ MeuCRM Backend funcionando!"));

// ── Verificação do Webhook ──
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    log("✅ Webhook verificado!");
    res.status(200).send(challenge);
  } else {
    res.sendStatus(403);
  }
});

// ── Códigos de erro da Meta ──
const META_ERROR_CODES = {
  131026: 'Mensagem não entregue: o número não tem WhatsApp ou não pode receber mensagens deste tipo.',
  131047: 'Fora da janela de 24h: só é possível enviar modelo (template) aprovado para este contato.',
  131049: 'A Meta limitou a entrega (limite de marketing/saúde da conta) e optou por não entregar.',
  131051: 'Tipo de mensagem não suportado pelo destinatário.',
  131053: 'Falha ao enviar a mídia (arquivo inválido ou inacessível).',
  131000: 'Erro interno da Meta ao processar a mensagem.',
  131042: 'Problema de pagamento da conta: o envio falhou por causa do método de pagamento do WhatsApp Business. Verifique o faturamento/cartão no Gerenciador de Negócios da Meta (WhatsApp > Configurações de pagamento).',
  131031: 'Conta do WhatsApp Business BLOQUEADA pela Meta. Geralmente por pagamento pendente/recusado ou violação de política. Resolva em business.facebook.com > Qualidade da conta / Central de Segurança (e regularize o pagamento). Pode ser necessário solicitar revisão.',
  130472: 'A Meta optou por não entregar (experimento/qualidade do número).',
  470:    'Fora da janela de 24h: use um modelo aprovado para reabrir a conversa.',
  132000: 'Modelo: número de variáveis não confere com o aprovado.',
  132001: 'Modelo não existe ou não está aprovado para este idioma.',
  132005: 'Modelo: o texto enviado foi reprovado pela Meta.',
  132007: 'Modelo: conteúdo viola as políticas do WhatsApp.',
  133010: 'Número não registrado na conta do WhatsApp.',
  100:    'Parâmetro inválido na requisição à Meta.',
};
function metaErrorText(er) {
  if (!er) return 'Falha no envio reportada pela Meta sem detalhes adicionais (status "failed").';
  const code = er.code;
  let txt;
  if (code && META_ERROR_CODES[code]) {
    txt = META_ERROR_CODES[code];
  } else {
    const parts = [];
    if (er.title) parts.push(er.title);
    if (er.error_data?.details) parts.push(er.error_data.details);
    else if (er.message) parts.push(er.message);
    txt = parts.filter(Boolean).join(' — ') || 'Falha no envio reportada pela Meta.';
  }
  if (code) txt += ` (código ${code})`;
  return txt;
}

// ── Buffer de status ──
const _pendingStatuses = {};
function _cachePendingStatus(wamid, upd) {
  if (!wamid) return;
  _pendingStatuses[wamid] = { ...upd, ts: Date.now() };
  const cutoff = Date.now() - 600000;
  for (const k in _pendingStatuses) if (_pendingStatuses[k].ts < cutoff) delete _pendingStatuses[k];
}
async function applyPendingStatus(wamid) {
  if (!wamid || !supabase) return;
  const p = _pendingStatuses[wamid];
  if (!p) return;
  const u = { status: p.status };
  if (p.error_info) u.error_info = p.error_info;
  await supabase.from('messages').update(u).eq('wamid', wamid);
}

// ── Webhook (receber mensagens) ──
app.post("/webhook", async (req, res) => {
  try {
    const body = req.body;
    log("📩 Webhook recebido:", JSON.stringify(body).substring(0, 200));

    if (body.object !== "whatsapp_business_account") {
      log("⚠️ Objeto ignorado:", body.object);
      return res.sendStatus(200);
    }

    const changes = body.entry?.[0]?.changes;
    if (!changes?.length) return res.sendStatus(200);

    for (const change of changes) {
      const value = change.value;

      if (value?.statuses?.length && supabase) {
        for (const st of value.statuses) {
          const { id: wamid, status } = st;
          if (wamid && ['sent','delivered','read','failed'].includes(status)) {
            const upd = { status };
            if (status === 'failed') {
              upd.error_info = metaErrorText(st.errors?.[0]);
              errorLog('❌ Entrega falhou:', wamid, upd.error_info);
            }
            _cachePendingStatus(wamid, upd);
            await supabase.from("messages").update(upd).eq("wamid", wamid);
          }
        }
      }

      if (!value?.messages?.length) continue;

      const message = value.messages[0];
      const contact = value.contacts?.[0];
      const from = message.from;
      const name = contact?.profile?.name || "Desconhecido";
      const timestamp = new Date(parseInt(message.timestamp) * 1000).toISOString();
      const phoneNumberId = value.metadata?.phone_number_id;

      if (message.type === 'reaction') {
        const emoji = message.reaction?.emoji || null;
        const targetWamid = message.reaction?.message_id;
        if (supabase && targetWamid) {
          await supabase.from('messages').update({ reaction: emoji, reaction_by: 'contact' }).eq('wamid', targetWamid);
          log(`😀 Reação ${emoji||'(removida)'} em ${targetWamid}`);
        }
        continue;
      }

      log(`📨 Mensagem de ${name} (${from}) via ${phoneNumberId}`);

      let accountId = null;
      let ownerEmail = null;
      if (supabase && phoneNumberId) {
        const { data: account, error: accErr } = await supabase
          .from("accounts").select("id, owner").eq("phone_number_id", phoneNumberId).maybeSingle();
        if (accErr) errorLog("❌ Erro ao buscar conta:", accErr.message);
        if (account) {
          accountId = account.id;
          ownerEmail = account.owner || null;
          log("✅ Conta encontrada:", accountId, "dono:", ownerEmail);
        } else {
          log("⚠️ Nenhuma conta com phone_number_id:", phoneNumberId);
        }
      }

      let content = "";
      let mediaId = null;
      let mediaMimeType = null;
      let mediaCaption = null;
      const type = message.type;
      if (type === "text") {
        content = message.text?.body || "";
      } else if (type === "image") {
        mediaId = message.image?.id || null;
        mediaMimeType = message.image?.mime_type || "image/jpeg";
        mediaCaption = message.image?.caption || null;
        content = mediaCaption ? `[Imagem: ${mediaCaption}]` : "[Imagem recebida]";
      } else if (type === "audio") {
        mediaId = message.audio?.id || null;
        mediaMimeType = message.audio?.mime_type || "audio/ogg";
        content = "[Áudio recebido]";
      } else if (type === "document") {
        mediaId = message.document?.id || null;
        mediaMimeType = message.document?.mime_type || "application/octet-stream";
        mediaCaption = message.document?.filename || null;
        content = mediaCaption ? `[Documento: ${mediaCaption}]` : "[Documento recebido]";
      } else if (type === "video") {
        mediaId = message.video?.id || null;
        mediaMimeType = message.video?.mime_type || "video/mp4";
        mediaCaption = message.video?.caption || null;
        content = mediaCaption ? `[Vídeo: ${mediaCaption}]` : "[Vídeo recebido]";
      } else if (type === "sticker") {
        mediaId = message.sticker?.id || null;
        mediaMimeType = message.sticker?.mime_type || "image/webp";
        content = "[Figurinha]";
      } else if (type === "button") {
        content = message.button?.text || "[Botão]";
      } else if (type === "interactive") {
        const it = message.interactive;
        content = it?.button_reply?.title || it?.list_reply?.title || it?.nfm_reply?.name || "[Resposta interativa]";
      } else {
        content = `[Mensagem do tipo: ${type}]`;
      }

      if (supabase) {
        const { data: existing } = await supabase
          .from("contacts").select("name, unread_count, first_unread_at").eq("phone", from).eq("owner", ownerEmail || ' ').maybeSingle();

        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = {
          phone: from, last_message_at: timestamp,
          last_message_preview: preview,
          last_message_direction: 'inbound',
        };
        if (!existing) contactData.name = name;
        if (accountId) contactData.account_id = accountId;
        if (ownerEmail) contactData.owner = ownerEmail;

        const { error: contactErr } = await supabase
          .from("contacts")
          .upsert(contactData, { onConflict: "owner,phone" });

        if (contactErr) errorLog("❌ Erro ao salvar contato:", contactErr.message);
        else log("✅ Contato salvo:", from);

        const currentUnread = existing?.unread_count || 0;
        const unreadUpdate = { unread_count: currentUnread + 1 };
        if (currentUnread === 0) unreadUpdate.first_unread_at = timestamp;
        await supabase.from("contacts").update(unreadUpdate).eq("phone", from).eq("owner", ownerEmail || ' ');

        const messageData = {
          phone: from,
          content,
          type,
          direction: "inbound",
          timestamp,
          media_id: mediaId,
          media_mime_type: mediaMimeType,
          wamid: message.id || null,
        };
        if (accountId) messageData.account_id = accountId;
        if (ownerEmail) messageData.owner = ownerEmail;

        const { error: msgErr } = await supabase.from("messages").insert(messageData);

        if (msgErr) errorLog("❌ Erro ao salvar mensagem:", msgErr.message);
        else log("✅ Mensagem salva:", content.substring(0, 50));

        if (['text','button','interactive'].includes(type) && content) {
          try { await handleBotReply(from, content, ownerEmail); } catch(be) { errorLog('Bot reply error:', be.message); }
        }
        const n8nUrl = _settings['n8n_webhook_url'];
        if (n8nUrl) {
          try {
            await axios.post(n8nUrl, {
              event: 'message_received',
              phone: from,
              name,
              content,
              type,
              timestamp,
              account_id: accountId || null,
              media_id: mediaId || null,
              media_mime_type: mediaMimeType || null
            }, { timeout: 8000 });
          } catch(ne) { errorLog('N8N forward error:', ne.message); }
        }
      }
    }

    res.sendStatus(200);
  } catch (err) {
    errorLog("❌ Erro no webhook:", err.message);
    res.sendStatus(500);
  }
});

// ── Embedded Signup ──
app.post("/auth/whatsapp", async (req, res) => {
  const { code, redirect_uri } = req.body;
  if (!code) return res.status(400).json({ error: "Código não informado" });
  if (!APP_ID || !APP_SECRET) return res.status(500).json({ error: "APP_ID e APP_SECRET não configurados" });

  try {
    const tokenParams = { client_id: APP_ID, client_secret: APP_SECRET, code };
    if (redirect_uri) tokenParams.redirect_uri = redirect_uri;

    const tokenRes = await axios.get("https://graph.facebook.com/v23.0/oauth/access_token", {
      params: tokenParams,
    });
    const userToken = tokenRes.data.access_token;
    log("✅ Token obtido via Embedded Signup");

    const appToken = `${APP_ID}|${APP_SECRET}`;
    const debugRes = await axios.get("https://graph.facebook.com/v23.0/debug_token", {
      params: { input_token: userToken, access_token: appToken },
    });

    const granularScopes = debugRes.data.data?.granular_scopes || [];
    const wabaScope = granularScopes.find(s => s.scope === "whatsapp_business_management");
    const wabaIds = wabaScope?.target_ids || [];
    log("✅ WABA IDs encontrados:", wabaIds);

    const savedAccounts = [];

    for (const wabaId of wabaIds) {
      let wabaName = wabaId;
      try {
        const wabaRes = await axios.get(`https://graph.facebook.com/v23.0/${wabaId}`, {
          params: { access_token: userToken, fields: "id,name" },
        });
        wabaName = wabaRes.data.name || wabaId;
      } catch (e) {
        log("⚠️ Não foi possível buscar nome do WABA:", e.response?.data?.error?.message);
      }

      const phonesRes = await axios.get(`https://graph.facebook.com/v23.0/${wabaId}/phone_numbers`, {
        params: { access_token: userToken, fields: "id,display_phone_number,verified_name" },
      });
      const phones = phonesRes.data.data || [];
      log(`📞 ${phones.length} número(s) no WABA ${wabaId}`);

      for (const phone of phones) {
        try {
          await axios.post(
            `https://graph.facebook.com/v23.0/${phone.id}/register`,
            { messaging_product: "whatsapp", pin: process.env.WHATSAPP_PIN || "123456" },
            { headers: { Authorization: `Bearer ${userToken}`, "Content-Type": "application/json" } }
          );
          log("✅ Número registrado:", phone.display_phone_number);
        } catch (e) {
          log("⚠️ Registro do número (pode já estar ativo):", e.response?.data?.error?.message);
        }

        try {
          await axios.post(
            `https://graph.facebook.com/v23.0/${wabaId}/subscribed_apps`,
            {},
            { params: { access_token: userToken } }
          );
          log("✅ WABA inscrito no webhook:", wabaId);
        } catch (e) {
          log("⚠️ Aviso webhook subscribe:", e.response?.data?.error?.message);
        }

        const accountData = {
          name: phone.verified_name || wabaName,
          phone_number_id: phone.id,
          phone_display: phone.display_phone_number,
          token: userToken,
          waba_id: wabaId,
          owner: req.owner || null,
        };

        if (supabase) {
          const { data, error } = await supabase
            .from("accounts")
            .upsert(accountData, { onConflict: "phone_number_id" })
            .select()
            .single();
          if (!error) {
            savedAccounts.push(data);
            log("✅ Conta salva:", accountData.name);
          } else {
            errorLog("❌ Erro ao salvar conta:", error.message);
          }
        } else {
          savedAccounts.push(accountData);
        }
      }
    }

    if (savedAccounts.length === 0) {
      return res.status(400).json({
        error: "Nenhum número de WhatsApp encontrado nesta conta do Facebook. Verifique se há uma conta WhatsApp Business vinculada.",
      });
    }

    res.json({ success: true, accounts: savedAccounts });
  } catch (err) {
    errorLog("❌ Erro auth:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao conectar com o Facebook" });
  }
});

// ── Listar contas ──
app.get("/accounts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("accounts").select("id, name, phone_number_id, phone_display, type, evolution_instance, created_at")
    .eq("owner", req.owner || ' ')
    .order("created_at", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/accounts", async (req, res) => {
  const { name, phone_number_id, token } = req.body;
  if (!name || !phone_number_id || !token)
    return res.status(400).json({ error: "Informe name, phone_number_id e token" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { data, error } = await supabase
    .from("accounts").insert({ name, phone_number_id, token, owner: req.owner || null }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

app.delete("/accounts/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("accounts").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Enviar mensagem ──
app.post("/send", async (req, res) => {
  const { to, message, account_id, quoted_id, quoted_content, quoted_direction } = req.body;
  if (!to || !message) return res.status(400).json({ error: "Informe 'to' e 'message'" });

  let phoneNumberId, token, evolutionInstance = null, accountType = 'cloudapi';

  if (supabase && account_id) {
    const { data: account, error: accErr } = await supabase
      .from("accounts").select("phone_number_id, token, type, evolution_instance").eq("id", account_id).single();
    if (accErr) errorLog("❌ Erro ao buscar conta para envio:", accErr.message);
    if (account) {
      phoneNumberId = account.phone_number_id;
      token = account.token;
      accountType = account.type || 'cloudapi';
      evolutionInstance = account.evolution_instance || null;
    }
  }

  if (!evolutionInstance && (!phoneNumberId || !token)) {
    phoneNumberId = process.env.PHONE_NUMBER_ID;
    token = process.env.WHATSAPP_TOKEN;
    if (phoneNumberId && token) {
      log("⚠️ Usando credenciais das variáveis de ambiente");
    }
  }

  if (accountType === 'evolution' && evolutionInstance) {
    try {
      const evoRes = await sendViaEvolution(evolutionInstance, to, message);
      const wamid = evoRes?.key?.id || null;
      if (supabase) {
        const safeAccountId = account_id || null;
        const preview = message.length > 80 ? message.substring(0, 80) + '…' : message;
        await supabase.from('contacts').upsert({ phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId, last_message_preview: preview, last_message_direction: 'outbound' }, { onConflict: 'phone' });
        await supabase.from('messages').insert({ phone: to, content: message, type: 'text', direction: 'outbound', timestamp: new Date().toISOString(), account_id: safeAccountId, wamid, quoted_id: quoted_id || null, quoted_content: quoted_content || null, quoted_direction: quoted_direction || null });
      }
      return res.json({ success: true, via: 'evolution' });
    } catch(e) {
      errorLog('Evolution send error:', e.response?.data || e.message);
      return res.status(500).json({ error: 'Falha ao enviar via Evolution: ' + (e.response?.data?.message || e.message) });
    }
  }

  if (!phoneNumberId || !token)
    return res.status(400).json({ error: "Nenhuma conta configurada. Adicione uma conta WhatsApp primeiro." });

  try {
    const response = await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product: "whatsapp", to, type: "text", text: { body: message } },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    if (supabase) {
      const safeAccountId = account_id || null;
      const preview = message.length > 80 ? message.substring(0, 80) + '…' : message;
      await supabase.from("contacts").upsert(
        {
          phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId,
          last_message_preview: preview,
          last_message_direction: 'outbound',
          owner: req.owner || null,
        },
        { onConflict: "owner,phone" }
      );
      const wamid = response.data?.messages?.[0]?.id || null;
      const { error: msgErr } = await supabase.from("messages").insert({
        phone: to, content: message, type: "text", direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: 'sent', wamid, owner: req.owner || null,
        quoted_id: quoted_id || null,
        quoted_content: quoted_content || null,
        quoted_direction: quoted_direction || null,
      });
      if (msgErr) errorLog("❌ Erro ao salvar mensagem enviada:", msgErr.message);
      else {
        await applyPendingStatus(wamid);
        log("✅ Mensagem enviada salva:", message.substring(0, 50));
      }
    }
    res.json({ success: true, data: response.data });
  } catch (err) {
    errorLog("❌ Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem", detail: err.response?.data });
  }
});

// ── Reagir a uma mensagem ──
app.post("/react", async (req, res) => {
  const { to, wamid, emoji, account_id } = req.body;
  if (!to || !wamid) return res.status(400).json({ error: "Informe 'to' e 'wamid'" });
  const acct = await botGetAcct(account_id);
  if (!acct.phone_number_id || !acct.token) return res.status(400).json({ error: "Nenhuma conta configurada." });
  try {
    await axios.post(
      `https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
      { messaging_product: "whatsapp", to, type: "reaction", reaction: { message_id: wamid, emoji: emoji || "" } },
      { headers: { Authorization: `Bearer ${acct.token}`, "Content-Type": "application/json" } }
    );
    if (supabase) await supabase.from("messages").update({ reaction: emoji || null, reaction_by: 'me' }).eq("wamid", wamid);
    res.json({ success: true });
  } catch (err) {
    errorLog("❌ Erro ao reagir:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao reagir", detail: err.response?.data });
  }
});

// ── Enviar mídia ──
app.post("/send-media", async (req, res) => {
  const { to, account_id, fileBase64, fileName, mimeType } = req.body;
  if (!to || !fileBase64 || !fileName || !mimeType)
    return res.status(400).json({ error: "Informe to, fileBase64, fileName e mimeType" });

  let phoneNumberId, token;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("phone_number_id, token").eq("id", account_id).single();
    if (account) { phoneNumberId = account.phone_number_id; token = account.token; }
  }
  if (!phoneNumberId || !token) {
    phoneNumberId = process.env.PHONE_NUMBER_ID;
    token = process.env.WHATSAPP_TOKEN;
  }
  if (!phoneNumberId || !token)
    return res.status(400).json({ error: "Nenhuma conta configurada." });

  try {
    const FormData = require("form-data");
    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", mimeType);
    form.append("file", Buffer.from(fileBase64, "base64"), {
      filename: fileName,
      contentType: mimeType,
    });

    const uploadRes = await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/media`,
      form,
      { headers: { ...form.getHeaders(), Authorization: `Bearer ${token}` } }
    );
    const mediaId = uploadRes.data.id;
    log("✅ Mídia enviada para Meta, id:", mediaId);

    let msgType = "document";
    if (mimeType.startsWith("image/")) msgType = "image";
    else if (mimeType.startsWith("video/")) msgType = "video";
    else if (mimeType.startsWith("audio/")) msgType = "audio";

    const mediaObj = { id: mediaId };
    if (msgType === "document") mediaObj.filename = fileName;

    const mediaResp = await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product: "whatsapp", to, type: msgType, [msgType]: mediaObj },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    if (supabase) {
      const safeAccountId = account_id || null;
      const mediaWamid = mediaResp.data?.messages?.[0]?.id || null;
      const label = msgType === "image" ? "Imagem" : msgType === "video" ? "Vídeo" : msgType === "audio" ? "Áudio" : "Documento";
      const content = `[${label}: ${fileName}]`;
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId,
          last_message_preview: content, last_message_direction: 'outbound', owner: req.owner || null },
        { onConflict: "owner,phone" }
      );
      await supabase.from("messages").insert({
        phone: to, content,
        type: msgType, direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: 'sent', wamid: mediaWamid, owner: req.owner || null,
        media_id: mediaId, media_mime_type: mimeType,
      });
      await applyPendingStatus(mediaWamid);
    }
    res.json({ success: true });
  } catch (err) {
    errorLog("❌ Erro ao enviar mídia:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mídia", detail: err.response?.data });
  }
});

// ── Proxy de mídia HÍBRIDO (cache para imagens/áudios, streaming para vídeos) ──
const mediaUrlCache = new Map();
const _mediaBufCache = {};

app.get("/media-proxy/:mediaId", async (req, res) => {
  const { account_id, download, filename } = req.query;
  const { mediaId } = req.params;

  let token = process.env.WHATSAPP_TOKEN;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("token").eq("id", account_id).maybeSingle();
    if (account?.token) token = account.token;
  }
  if (!token) return res.status(400).json({ error: "Token não encontrado" });

  const cacheKey = `${mediaId}_${token.substring(0, 20)}`;

  try {
    let url = mediaUrlCache.get(cacheKey)?.url;
    if (!url) {
      const metaRes = await axios.get(`https://graph.facebook.com/v23.0/${mediaId}`, {
        headers: { Authorization: `Bearer ${token}` }, timeout: 20000
      });
      url = metaRes.data.url;
      if (!url) throw new Error("URL de mídia não encontrada");
      mediaUrlCache.set(cacheKey, { url, ts: Date.now() });
    }

    // Verifica se é imagem ou áudio (pequeno) para usar cache em memória
    const isSmall = /\.(jpg|jpeg|png|gif|webp|mp3|ogg|wav)$/i.test(filename || '');
    const isVideo = /\.(mp4|mov|avi|mkv|webm)$/i.test(filename || '');

    // Se for pequeno E não for vídeo → cacheia em memória (rápido)
    if (isSmall && !isVideo) {
      let entry = _mediaBufCache[cacheKey];
      if (!entry || Date.now() - entry.ts > 600000) {
        const resp = await axios.get(url, {
          headers: { Authorization: `Bearer ${token}`, "User-Agent": "WhatsApp/2.0" },
          responseType: "arraybuffer", timeout: 30000,
          validateStatus: s => s >= 200 && s < 400,
        });
        entry = {
          buf: Buffer.from(resp.data),
          ctype: resp.headers["content-type"] || req.query.mime || "application/octet-stream",
          ts: Date.now()
        };
        _mediaBufCache[cacheKey] = entry;
        // Mantém no máximo 5 arquivos em cache
        const keys = Object.keys(_mediaBufCache);
        if (keys.length > 5) {
          keys.sort((a,b) => _mediaBufCache[a].ts - _mediaBufCache[b].ts);
          delete _mediaBufCache[keys[0]];
        }
      }

      const buf = entry.buf;
      const total = buf.length;
      const ctype = req.query.mime || entry.ctype || "application/octet-stream";
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Accept-Ranges", "bytes");
      res.setHeader("Content-Type", ctype);

      if (download === "1") {
        const safeFilename = filename ? decodeURIComponent(filename) : `midia_${mediaId.substring(0,8)}`;
        res.setHeader("Content-Disposition", `attachment; filename="${safeFilename}"`);
        res.setHeader("Content-Length", total);
        return res.status(200).end(buf);
      }
      res.setHeader("Cache-Control", "public, max-age=600");

      const range = req.headers.range;
      if (range) {
        const m = /bytes=(\d*)-(\d*)/.exec(range);
        let start = m && m[1] !== "" ? parseInt(m[1], 10) : 0;
        let end   = m && m[2] !== "" ? parseInt(m[2], 10) : total - 1;
        if (isNaN(start)) start = 0;
        if (isNaN(end) || end >= total) end = total - 1;
        if (start > end || start >= total) {
          res.setHeader("Content-Range", `bytes */${total}`);
          return res.status(416).end();
        }
        res.status(206);
        res.setHeader("Content-Range", `bytes ${start}-${end}/${total}`);
        res.setHeader("Content-Length", end - start + 1);
        return res.end(buf.subarray(start, end + 1));
      }

      res.status(200);
      res.setHeader("Content-Length", total);
      return res.end(buf);
    }

    // Para vídeos ou arquivos grandes → streaming (sem cache em memória)
    const headers = { Authorization: `Bearer ${token}`, "User-Agent": "WhatsApp/2.0" };
    const range = req.headers.range;
    if (range) headers.Range = range;

    const response = await axios({
      method: 'get',
      url,
      headers,
      responseType: 'stream',
      timeout: 60000,
      validateStatus: s => s >= 200 && s < 400,
    });

    const contentType = response.headers['content-type'] || req.query.mime || 'application/octet-stream';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=600');

    if (download === '1') {
      const safeFilename = filename ? decodeURIComponent(filename) : `midia_${mediaId.substring(0,8)}`;
      res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
    }

    if (response.status === 206) {
      res.status(206);
      res.setHeader('Content-Range', response.headers['content-range']);
      res.setHeader('Content-Length', response.headers['content-length']);
    } else {
      res.status(200);
      res.setHeader('Content-Length', response.headers['content-length'] || '');
    }

    response.data.pipe(res);
    response.data.on('error', (err) => {
      errorLog('Stream error:', err.message);
      if (!res.headersSent) res.status(500).json({ error: 'Erro no streaming' });
    });

  } catch (err) {
    errorLog('❌ Media error:', err.response?.status || err.message);
    if (!res.headersSent) res.status(500).json({ error: "Falha ao carregar mídia" });
  }
});

// ── Lista todas as tags ──
app.get("/tags", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase.from("contacts").select("tags").eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  const set = new Set();
  (data || []).forEach(c => (c.tags || []).forEach(t => { if (t) set.add(t); }));
  res.json(Array.from(set).sort((a, b) => a.localeCompare(b)));
});

// ── Busca por nome, telefone OU conteúdo das mensagens ──
app.get("/search", async (req, res) => {
  if (!supabase) return res.json([]);
  const raw = (req.query.q || "").trim();
  if (!raw) return res.json([]);
  const { account_id } = req.query;
  const term = raw.replace(/[,()]/g, " ").trim();
  const like = `%${term}%`;
  try {
    let mq = supabase.from("messages").select("phone").ilike("content", like).eq("owner", req.owner || ' ').limit(500);
    if (account_id) mq = mq.eq("account_id", account_id);
    const { data: msgRows } = await mq;
    const phones = [...new Set((msgRows || []).map(m => m.phone).filter(Boolean))];

    let orCond = `name.ilike.${like},phone.ilike.${like}`;
    if (phones.length) orCond += `,phone.in.(${phones.join(",")})`;
    let cq = supabase.from("contacts")
      .select("phone, name, account_id, stage_id, tags, unread_count, first_unread_at, last_message_at, last_message_preview, last_message_direction")
      .eq("owner", req.owner || ' ')
      .or(orCond)
      .not("last_message_preview", "is", null)
      .order("last_message_at", { ascending: false })
      .limit(200);
    if (account_id) cq = cq.eq("account_id", account_id);
    const { data, error } = await cq;
    if (error) return res.status(500).json({ error: error.message });
    res.json(data || []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Tarefas ──
app.get("/tasks", async (req, res) => {
  if (!supabase) return res.json([]);
  const { phone, pending } = req.query;
  let q = supabase.from("tasks").select("*").eq("owner", req.owner || ' ').order("due_at", { ascending: true, nullsFirst: false });
  if (phone) q = q.eq("phone", phone);
  if (pending === "1") q = q.eq("done", false);
  const { data, error } = await q;
  if (error) return res.status(500).json({ error: error.message });
  const tasks = data || [];
  const phones = [...new Set(tasks.map(t => t.phone).filter(Boolean))];
  if (phones.length) {
    const { data: cts } = await supabase.from("contacts").select("phone,name").in("phone", phones).eq("owner", req.owner || ' ');
    const nameMap = {};
    for (const c of cts || []) nameMap[c.phone] = c.name;
    for (const t of tasks) t.contact_name = t.phone ? (nameMap[t.phone] || t.phone) : null;
  }
  res.json(tasks);
});

app.post("/tasks", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phone, account_id, title, due_at, notes } = req.body;
  if (!title) return res.status(400).json({ error: "Título obrigatório" });
  const { data, error } = await supabase.from("tasks")
    .insert({ phone: phone || null, account_id: account_id || null, title, due_at: due_at || null, notes: notes || null, owner: req.owner || null })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

app.put("/tasks/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const upd = {};
  if (typeof req.body.done === "boolean") upd.done = req.body.done;
  if (req.body.title != null) upd.title = req.body.title;
  if (req.body.due_at !== undefined) upd.due_at = req.body.due_at || null;
  if (req.body.notes !== undefined) upd.notes = req.body.notes || null;
  const { error } = await supabase.from("tasks").update(upd).eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.delete("/tasks/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("tasks").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Tags por contato ──
app.put("/contacts/:phone/tags", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { tags } = req.body;
  if (!Array.isArray(tags)) return res.status(400).json({ error: "tags deve ser array" });
  const { error } = await supabase
    .from("contacts").update({ tags }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.put("/contacts/:phone/name", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const name = (req.body.name || "").trim();
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const { error } = await supabase.from("contacts").update({ name }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.get("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.json({ notes: "" });
  const { data, error } = await supabase
    .from("contacts").select("notes").eq("phone", req.params.phone).eq("owner", req.owner || ' ').maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ notes: data?.notes || "" });
});

app.put("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { notes } = req.body;
  const { error } = await supabase
    .from("contacts")
    .update({ notes: notes ?? "" })
    .eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Criar contato manualmente ──
app.post("/contacts", async (req, res) => {
  const { name, phone, account_id } = req.body;
  if (!name || !phone) return res.status(400).json({ error: "Nome e celular são obrigatórios" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const cleanPhone = phone.replace(/\D/g, '');
  if (cleanPhone.length < 8) return res.status(400).json({ error: "Número de celular inválido" });
  const { data, error } = await supabase.from("contacts")
    .upsert({ phone: cleanPhone, name, account_id: account_id || null, owner: req.owner || null, last_message_at: new Date().toISOString() }, { onConflict: "owner,phone" })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

// ── Importar lista de contatos ──
app.post("/contacts/import", async (req, res) => {
  const { contacts, account_id, stage_id } = req.body;
  if (!contacts || !Array.isArray(contacts)) return res.status(400).json({ error: "Lista inválida" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const toInsert = contacts
    .map(c => {
      const obj = { phone: String(c.phone || '').replace(/\D/g, ''), name: c.name || 'Desconhecido', account_id: account_id || null, owner: req.owner || null, last_message_at: new Date().toISOString() };
      if (stage_id) obj.stage_id = stage_id;
      return obj;
    })
    .filter(c => c.phone.length >= 8);
  if (!toInsert.length) return res.status(400).json({ error: "Nenhum contato válido encontrado" });
  const { error } = await supabase.from("contacts").upsert(toInsert, { onConflict: "owner,phone" });
  if (error) return res.status(500).json({ error: error.message });
  log(`✅ ${toInsert.length} contatos importados`);
  res.json({ success: true, count: toInsert.length });
});

// ── Importar lead via n8n ──
app.post("/import/lead", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const items = Array.isArray(req.body) ? req.body
              : (Array.isArray(req.body.leads) ? req.body.leads : [req.body]);
  const n8nOwner = (String(req.query.owner||'').trim()) || (!Array.isArray(req.body) && req.body.owner) || 'elianecezaroliveira@gmail.com';
  const stageCache = {};
  let imported = 0;
  const errors = [];

  for (const it of (items || [])) {
    const name  = (String(it.name || it.title || it["Lead Titulo"] || "").replace(/^=+\s*/, "").trim()) || "Lead";
    const phone = String(it.phone || it.celular || it["Celular"] || "").replace(/\D/g, "");
    const extId = String(it.id || it["ID"] || it.stage_external_id || "").replace(/^=+\s*/, "").trim();
    const account_id = it.account_id || null;
    if (phone.length < 8) { errors.push({ phone, error: "telefone inválido" }); continue; }

    let stage_id = null;
    if (extId) {
      if (stageCache[extId] === undefined) {
        const { data: st } = await supabase.from("pipeline_stages").select("id").eq("external_id", extId).eq("owner", n8nOwner).maybeSingle();
        stageCache[extId] = st ? st.id : null;
      }
      stage_id = stageCache[extId];
    }

    const row = { phone, name, owner: n8nOwner };
    if (account_id) row.account_id = account_id;
    if (stage_id) row.stage_id = stage_id;
    const { error: e } = await supabase.from("contacts").upsert(row, { onConflict: "owner,phone" });
    if (e) errors.push({ phone, error: e.message }); else imported++;
  }

  log(`📥 n8n importou ${imported} lead(s)` + (errors.length ? `, ${errors.length} erro(s)` : ""));
  res.json({ success: true, imported, errors });
});

// ── Alterar a ETAPA de um lead já existente (via n8n) ──
app.post("/update/lead", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const items = Array.isArray(req.body) ? req.body
              : (Array.isArray(req.body.leads) ? req.body.leads : [req.body]);
  const clean = v => String(v || "").replace(/^=+\s*/, "").trim();
  const n8nOwner = (String(req.query.owner||'').trim()) || (!Array.isArray(req.body) && req.body.owner) || 'elianecezaroliveira@gmail.com';
  const stageCache = {};
  let updated = 0;
  const errors = [];

  for (const it of (items || [])) {
    const phone = clean(it.phone || it.celular || it["Celular"]).replace(/\D/g, "");
    if (phone.length < 8) { errors.push({ phone, error: "telefone inválido" }); continue; }

    let stage_id = clean(it.stage_id) || null;
    const extId     = clean(it.id || it["ID"] || it.stage_external_id);
    const stageName = clean(it.stage || it.etapa || it["Etapa"] || it.stage_name);
    const { data: prev } = await supabase.from("contacts").select("stage_id").eq("phone", phone).eq("owner", n8nOwner).maybeSingle();

    if (!stage_id) {
      const key = "ext:" + extId + "|name:" + stageName.toLowerCase();
      if (stageCache[key] === undefined) {
        let q = supabase.from("pipeline_stages").select("id").eq("owner", n8nOwner);
        if (extId) q = q.eq("external_id", extId);
        else if (stageName) q = q.ilike("name", stageName);
        else { stageCache[key] = null; }
        if (stageCache[key] === undefined) {
          const { data: st } = await q.maybeSingle();
          stageCache[key] = st ? st.id : null;
        }
      }
      stage_id = stageCache[key];
    }
    if (!stage_id) { errors.push({ phone, error: "etapa não encontrada" }); continue; }

    const { data, error } = await supabase.from("contacts").update({ stage_id }).eq("phone", phone).eq("owner", n8nOwner).select("phone");
    if (error) { errors.push({ phone, error: error.message }); continue; }
    if (!data || !data.length) { errors.push({ phone, error: "lead não encontrado no CRM" }); continue; }
    updated++;
    if (prev?.stage_id !== stage_id) { try { await fireStageBots(phone, stage_id, n8nOwner); } catch(e) { errorLog('fireStageBots (n8n):', e.message); } }
  }

  log(`🔁 n8n atualizou etapa de ${updated} lead(s)` + (errors.length ? `, ${errors.length} erro(s)` : ""));
  res.json({ success: true, updated, errors });
});

// ── Listar leads de uma etapa ──
app.get("/leads", async (req, res) => {
  if (!supabase) return res.json([]);
  const clean = v => String(v || "").replace(/^=+\s*/, "").trim();
  const extId     = clean(req.query.id || req.query.external_id);
  const stageName = clean(req.query.stage || req.query.etapa);
  let stage_id    = clean(req.query.stage_id) || null;
  const n8nOwner  = clean(req.query.owner) || 'elianecezaroliveira@gmail.com';

  if (!stage_id && (extId || stageName)) {
    let q = supabase.from("pipeline_stages").select("id").eq("owner", n8nOwner);
    if (extId) q = q.eq("external_id", extId);
    else q = q.ilike("name", stageName);
    const { data: st } = await q.maybeSingle();
    stage_id = st ? st.id : null;
  }
  if (!stage_id) return res.json([]);

  const { data, error } = await supabase.from("contacts")
    .select("phone, name, stage_id, account_id, tags")
    .eq("stage_id", stage_id).eq("owner", n8nOwner)
    .order("last_message_at", { ascending: false, nullsFirst: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// ── Deletar mensagem individual ──
app.delete("/messages/id/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("messages").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Mensagens de um contato (com limite) ──
app.get("/messages/:phone", async (req, res) => {
  if (!supabase) return res.json([]);
  const limit = parseInt(req.query.limit) || 50; // só carrega as últimas 50
  const { data, error } = await supabase
    .from("messages").select("*").eq("phone", req.params.phone).eq("owner", req.owner || ' ')
    .order("timestamp", { ascending: true })
    .limit(limit);
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ── Listar templates ──
app.get("/templates", async (req, res) => {
  const { account_id } = req.query;
  if (!supabase || !account_id) return res.status(400).json({ error: "account_id obrigatório" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("token, waba_id").eq("id", account_id).single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  if (!account.waba_id) return res.status(400).json({ error: "WABA ID não encontrado para esta conta" });
  try {
    const response = await axios.get(`https://graph.facebook.com/v23.0/${account.waba_id}/message_templates`, {
      params: { access_token: account.token, fields: "id,name,status,category,language,components", limit: 100 },
    });
    res.json(response.data.data || []);
  } catch (err) {
    errorLog("❌ Erro ao listar templates:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao listar templates" });
  }
});

// ── Criar template ──
app.post("/templates", async (req, res) => {
  const { account_id, name, category, language, components } = req.body;
  if (!account_id || !name || !category || !language || !components)
    return res.status(400).json({ error: "Campos obrigatórios: account_id, name, category, language, components" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("token, waba_id").eq("id", account_id).single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  try {
    const response = await axios.post(
      `https://graph.facebook.com/v23.0/${account.waba_id}/message_templates`,
      { name, category, language, components },
      { headers: { Authorization: `Bearer ${account.token}`, "Content-Type": "application/json" } }
    );
    log("✅ Template criado:", name);
    res.json({ success: true, data: response.data });
  } catch (err) {
    errorLog("❌ Erro ao criar template:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao criar template" });
  }
});

// ── Deletar template ──
app.delete("/templates/:template_id", async (req, res) => {
  const { account_id } = req.query;
  if (!supabase || !account_id) return res.status(400).json({ error: "account_id obrigatório" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("token, waba_id").eq("id", account_id).single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  if (!account.waba_id) return res.status(400).json({ error: "WABA ID não encontrado para esta conta" });
  const name = decodeURIComponent(req.params.template_id);
  const hsm_id = req.query.hsm_id;
  try {
    const params = { name, access_token: account.token };
    if (hsm_id) params.hsm_id = hsm_id;
    await axios.delete(`https://graph.facebook.com/v23.0/${account.waba_id}/message_templates`, { params });
    log("🗑️ Template excluído:", name);
    res.json({ success: true });
  } catch (err) {
    const metaErr = err.response?.data?.error;
    errorLog("❌ Erro ao deletar template:", metaErr || err.message);
    const msg = metaErr
      ? `${metaErr.message || 'erro'}${metaErr.code ? ' (código ' + metaErr.code + (metaErr.error_subcode ? '/' + metaErr.error_subcode : '') + ')' : ''}`
      : (err.message || "Erro ao deletar template");
    res.status(500).json({ error: msg, detail: metaErr || null });
  }
});

// ── Enviar template ──
app.post("/send-template", async (req, res) => {
  const { to, account_id, template_name, language_code, components, body_text } = req.body;
  if (!to || !account_id || !template_name)
    return res.status(400).json({ error: "Campos obrigatórios: to, account_id, template_name" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("phone_number_id, token").eq("id", account_id).single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  try {
    const templateMsg = {
      messaging_product: "whatsapp", to, type: "template",
      template: { name: template_name, language: { code: language_code || "pt_BR" } },
    };
    if (components && components.length > 0) templateMsg.template.components = components;
    const response = await axios.post(
      `https://graph.facebook.com/v23.0/${account.phone_number_id}/messages`,
      templateMsg,
      { headers: { Authorization: `Bearer ${account.token}`, "Content-Type": "application/json" } }
    );
    const safeAccountId = account_id || null;
    const shownText = (body_text && String(body_text).trim()) ? String(body_text).trim() : `[Template: ${template_name}]`;
    const preview = shownText.length > 80 ? shownText.substring(0, 80) + '…' : shownText;
    await supabase.from("contacts").upsert(
      { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId,
        last_message_preview: preview, last_message_direction: 'outbound', owner: req.owner || null },
      { onConflict: "owner,phone" }
    );
    const tplWamid = response.data?.messages?.[0]?.id || null;
    await supabase.from("messages").insert({
      phone: to, content: shownText, type: "template",
      direction: "outbound", timestamp: new Date().toISOString(), account_id: safeAccountId,
      status: 'sent', wamid: tplWamid, owner: req.owner || null,
    });
    await applyPendingStatus(tplWamid);
    log("✅ Template enviado:", template_name, "→", to, "wamid:", tplWamid);
    res.json({ success: true, data: response.data });
  } catch (err) {
    const e = err.response?.data?.error || {};
    const msg = e.error_user_msg || e.message || err.message || "Erro ao enviar template";
    const detail = e.error_user_title || e.error_data?.details || "";
    errorLog("❌ Erro ao enviar template:", err.response?.data || err.message);
    res.status(500).json({ error: msg, detail, code: e.code || null });
  }
});

// ── Pipeline / Kanban ──
app.get("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("pipeline_stages").select("*").eq("owner", req.owner || ' ').order("position", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

app.post("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const { data, error } = await supabase
    .from("pipeline_stages").insert({ name, position: position || 0, owner: req.owner || null }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.put("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  const updates = {};
  if (name !== undefined) updates.name = name;
  if (position !== undefined) updates.position = position;
  const { error } = await supabase
    .from("pipeline_stages").update(updates).eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.delete("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  await supabase.from("contacts").update({ stage_id: null }).eq("stage_id", req.params.id).eq("owner", req.owner || ' ');
  const { error } = await supabase.from("pipeline_stages").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.get("/contacts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { account_id, with_messages } = req.query;
  let query = supabase
    .from("contacts").select("phone, name, account_id, stage_id, tags, unread_count, first_unread_at, last_message_at, last_message_preview, last_message_direction")
    .eq("owner", req.owner || ' ')
    .order("last_message_at", { ascending: false });
  if (account_id) query = query.eq("account_id", account_id);
  if (with_messages) query = query.not("last_message_preview", "is", null);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

app.put("/contacts/:phone/stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { stage_id } = req.body;
  const { data: old } = await supabase.from("contacts").select("stage_id").eq("phone", req.params.phone).eq("owner", req.owner || ' ').maybeSingle();
  const { error } = await supabase
    .from("contacts").update({ stage_id: stage_id || null }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  if (stage_id && old?.stage_id !== stage_id) await fireStageBots(req.params.phone, stage_id, req.owner);
  res.json({ success: true });
});

app.put("/contacts/bulk-stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones, stage_id } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  const { data: prevRows } = await supabase.from("contacts").select("phone, stage_id").in("phone", phones).eq("owner", req.owner || ' ');
  const prevMap = {}; for (const r of prevRows || []) prevMap[r.phone] = r.stage_id;
  const { error } = await supabase.from("contacts")
    .update({ stage_id: stage_id || null })
    .in("phone", phones).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  if (stage_id) { for (const ph of phones) { if (prevMap[ph] !== stage_id) { try { await fireStageBots(ph, stage_id, req.owner); } catch(e) { errorLog('fireStageBots (bulk):', e.message); } } } }
  res.json({ success: true });
});

app.delete("/contacts/bulk-delete", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  await supabase.from("messages").delete().in("phone", phones).eq("owner", req.owner || ' ');
  const { error } = await supabase.from("contacts").delete().in("phone", phones).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.put("/contacts/bulk-tags", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones, tags } = req.body;
  if (!Array.isArray(phones) || !Array.isArray(tags)) return res.status(400).json({ error: "phones e tags obrigatórios" });
  for (const phone of phones) {
    const { data: contact } = await supabase.from("contacts").select("tags").eq("phone", phone).eq("owner", req.owner || ' ').maybeSingle();
    const merged = Array.from(new Set([...(contact?.tags || []), ...tags]));
    await supabase.from("contacts").update({ tags: merged }).eq("phone", phone).eq("owner", req.owner || ' ');
  }
  res.json({ success: true });
});

app.put("/contacts/:phone/read", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase
    .from("contacts").update({ unread_count: 0, first_unread_at: null }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ═══════════════════════════════════════
// SISTEMA DE BOTS (mantido)
// ═══════════════════════════════════════

function applyVars(str, name, phone, notes) {
  if (!str) return str;
  return String(str)
    .replace(/[\{\(\[]{1,2}\s*nome\s*[\}\)\]]{1,2}/gi, name || '')
    .replace(/[\{\(\[]{1,2}\s*telefone\s*[\}\)\]]{1,2}/gi, phone || '')
    .replace(/[\{\(\[]{1,2}\s*(?:notas?|anota[cç][aã]o|anota[cç][oõ]es|observa[cç][aã]o|observa[cç][oõ]es)\s*[\}\)\]]{1,2}/gi, notes || '');
}

async function sendBotMsg(phone, accountId, text, owner) {
  let phoneNumberId, token;
  if (supabase && accountId) {
    const { data: acct } = await supabase.from('accounts').select('phone_number_id,token').eq('id', accountId).maybeSingle();
    if (acct) { phoneNumberId = acct.phone_number_id; token = acct.token; }
  }
  if (!phoneNumberId) { phoneNumberId = process.env.PHONE_NUMBER_ID; token = process.env.WHATSAPP_TOKEN; }
  if (!phoneNumberId || !token) return null;
  try {
    const r = await axios.post(`https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product:'whatsapp', to:phone, type:'text', text:{body:text} },
      { headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' } });
    const wamid = r.data?.messages?.[0]?.id || null;
    if (supabase) {
      const ts = new Date().toISOString();
      await supabase.from('messages').insert({ phone, content:text, type:'text', direction:'outbound', timestamp:ts, account_id:accountId||null, status:'sent', wamid, owner:owner||null });
      await applyPendingStatus(wamid);
      const prev = text.length>80 ? text.substring(0,80)+'…' : text;
      await supabase.from('contacts').update({ last_message_at:ts, last_message_preview:prev, last_message_direction:'outbound', unread_count:0, first_unread_at:null }).eq('phone',phone).eq('owner',owner||' ');
    }
    return wamid;
  } catch(e) { errorLog('❌ Bot sendMsg:', e.response?.data||e.message); return null; }
}

async function botGetAcct(accountId) {
  if (supabase && accountId) {
    const { data } = await supabase.from('accounts').select('phone_number_id,token,waba_id').eq('id', accountId).maybeSingle();
    if (data && data.phone_number_id) return data;
  }
  return { phone_number_id: process.env.PHONE_NUMBER_ID, token: process.env.WHATSAPP_TOKEN, waba_id: process.env.WABA_ID };
}

const _tmplBodyCache = {};
async function getTemplateBodyText(token, wabaId, name, lang) {
  if (!token || !wabaId || !name) return null;
  const key = wabaId + '|' + name + '|' + (lang || '');
  if (_tmplBodyCache[key] !== undefined) return _tmplBodyCache[key];
  try {
    const r = await axios.get(`https://graph.facebook.com/v23.0/${wabaId}/message_templates`, {
      params: { access_token: token, name, fields: 'name,language,components', limit: 10 }
    });
    const list = r.data?.data || [];
    const tmpl = list.find(t => t.name === name && (!lang || t.language === lang)) || list.find(t => t.name === name) || list[0];
    const body = tmpl?.components?.find(c => c.type === 'BODY');
    const txt = body?.text || null;
    _tmplBodyCache[key] = txt;
    return txt;
  } catch(e) { _tmplBodyCache[key] = null; return null; }
}

function renderTemplateBody(bodyText, vars) {
  let txt = bodyText || '';
  (vars || []).forEach((val, i) => { txt = txt.split('{{' + (i + 1) + '}}').join(val); });
  return txt;
}

async function sendBotTemplate(phone, accountId, cfg, name, notes, owner) {
  const acct = await botGetAcct(accountId);
  if (!acct.phone_number_id || !acct.token) return null;
  let bodyText = null;
  try { bodyText = await getTemplateBodyText(acct.token, acct.waba_id, cfg.template_name, cfg.language || 'pt_BR'); } catch(_) {}
  const provided = (cfg.vars || []).map(v => applyVars(String(v || ''), name || phone, phone, notes));
  const needed = bodyText ? new Set(bodyText.match(/\{\{\d+\}\}/g) || []).size : provided.length;
  const vars = [];
  for (let i = 0; i < needed; i++) {
    const p = provided[i];
    vars.push(p && p.trim() ? p : (i === 0 ? (name || phone) : ' '));
  }
  const tmpl = { name: cfg.template_name, language: { code: cfg.language || 'pt_BR' } };
  if (vars.length) tmpl.components = [{ type: 'body', parameters: vars.map(t => ({ type: 'text', text: t })) }];
  try {
    const r = await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
      { messaging_product: 'whatsapp', to: phone, type: 'template', template: tmpl },
      { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } });
    if (supabase) {
      const ts = new Date().toISOString();
      let shown = bodyText ? renderTemplateBody(bodyText, vars) : `[Modelo: ${cfg.template_name}]`;
      const prev = shown.length > 80 ? shown.substring(0, 80) + '…' : shown;
      const tWamid = r.data?.messages?.[0]?.id || null;
      await supabase.from('messages').insert({ phone, content: shown, type: 'template', direction: 'outbound', timestamp: ts, account_id: accountId || null, status: 'sent', wamid: tWamid, owner: owner || null });
      await applyPendingStatus(tWamid);
      await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
    }
    return true;
  } catch(e) { errorLog('❌ Bot template:', e.response?.data || e.message); return null; }
}

function businessHoursState(nowMs, cfg) {
  const days = (cfg.days && cfg.days.length) ? cfg.days.map(Number) : [1,2,3,4,5];
  const [sh, sm] = String(cfg.start || '08:00').split(':').map(Number);
  const [eh, em] = String(cfg.end   || '18:00').split(':').map(Number);
  const startMin = sh*60 + sm, endMin = eh*60 + em;
  const brt = new Date(nowMs - 3*3600000);
  const dow = brt.getUTCDay();
  const minNow = brt.getUTCHours()*60 + brt.getUTCMinutes();
  const isOpen = days.includes(dow) && minNow >= startMin && minNow < endMin;
  if (isOpen) return { open: true };
  for (let off = 0; off <= 7; off++) {
    const d = new Date(Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate() + off));
    if (!days.includes(d.getUTCDay())) continue;
    if (off === 0 && minNow >= startMin) continue;
    const openBrtMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), sh, sm);
    return { open: false, nextOpenMs: openBrtMs + 3*3600000 };
  }
  return { open: false, nextOpenMs: nowMs + 3600000 };
}

async function getNextNodeId(fromNodeId, edgeLabel) {
  if (!supabase) return null;
  const { data:edges } = await supabase.from('bot_edges').select('to_node_id,label').eq('from_node_id', fromNodeId);
  if (!edges?.length) return null;
  if (edgeLabel) {
    const m = edges.find(e => e.label && e.label.toLowerCase() === edgeLabel.toLowerCase());
    if (m) return m.to_node_id;
  }
  const def = edges.find(e => !e.label || e.label==='' || e.label==='default');
  return def?.to_node_id || edges[0]?.to_node_id || null;
}

async function stopRun(runId, status='completed') {
  if (supabase) await supabase.from('bot_runs').update({ status, updated_at:new Date().toISOString() }).eq('id', runId);
}

async function processNode(run, depth=0) {
  if (!supabase || depth > 30) return;
  const { id:runId, contact_phone:phone, account_id:acctId, current_node_id:nodeId, owner:botOwner } = run;
  const OW = botOwner || ' ';
  const { data:node } = await supabase.from('bot_nodes').select('*').eq('id', nodeId).maybeSingle();
  if (!node) { await stopRun(runId,'stopped'); return; }
  const cfg = node.config || {};

  if (node.type === 'start') {
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'message') {
    const { data:ct } = await supabase.from('contacts').select('name,notes').eq('phone',phone).eq('owner',OW).maybeSingle();
    const name = ct?.name || phone;
    const notes = ct?.notes || '';
    let sendOk;
    if (cfg.mode === 'template' && cfg.template_name) {
      sendOk = await sendBotTemplate(phone, acctId, cfg, name, notes, botOwner);
    } else {
      const text = applyVars(cfg.text || '', name, phone, notes);
      sendOk = text ? await sendBotMsg(phone, acctId, text, botOwner) : true;
    }
    const { data:medges } = await supabase.from('bot_edges').select('to_node_id,label').eq('from_node_id', nodeId);
    const okNxt   = medges?.find(e=>!e.label||e.label===''||e.label==='default')?.to_node_id || null;
    const failNxt = medges?.find(e=>(e.label||'').toLowerCase()==='__failed__')?.to_node_id || null;
    const hasButtons = cfg.mode === 'template' && Array.isArray(cfg.buttons) && cfg.buttons.length > 0;
    if (!sendOk && failNxt) {
      await supabase.from('bot_runs').update({ current_node_id:failNxt, updated_at:new Date().toISOString() }).eq('id',runId);
      await processNode({...run,current_node_id:failNxt}, depth+1);
    } else if (!sendOk) {
      await stopRun(runId,'failed');
    } else if (hasButtons) {
      let pauseUntil = null;
      if (cfg.timeout_hours && cfg.timeout_hours > 0) pauseUntil = new Date(Date.now() + cfg.timeout_hours*3600000).toISOString();
      await supabase.from('bot_runs').update({ status:'waiting_reply', pause_until:pauseUntil, updated_at:new Date().toISOString() }).eq('id',runId);
    } else if (okNxt) {
      await supabase.from('bot_runs').update({ current_node_id:okNxt, updated_at:new Date().toISOString() }).eq('id',runId);
      await processNode({...run,current_node_id:okNxt}, depth+1);
    } else {
      await stopRun(runId,'completed');
    }

  } else if (node.type === 'tags') {
    const { data:ct } = await supabase.from('contacts').select('tags').eq('phone',phone).eq('owner',OW).maybeSingle();
    let tags = Array.isArray(ct?.tags) ? ct.tags.slice() : [];
    (cfg.add||[]).forEach(t => { if (t && !tags.includes(t)) tags.push(t); });
    if (cfg.remove?.length) tags = tags.filter(t => !cfg.remove.includes(t));
    await supabase.from('contacts').update({ tags }).eq('phone',phone).eq('owner',OW);
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'task') {
    const { data:ct } = await supabase.from('contacts').select('name').eq('phone',phone).eq('owner',OW).maybeSingle();
    const title = applyVars(cfg.title || 'Tarefa', ct?.name || phone, phone);
    const due = cfg.due_hours ? new Date(Date.now() + Number(cfg.due_hours)*3600000).toISOString() : null;
    await supabase.from('tasks').insert({ phone, account_id:acctId||null, title, due_at:due, owner:botOwner||null });
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'complete_task') {
    let q = supabase.from('tasks').update({ done:true }).eq('phone', phone).eq('done', false).eq('owner', OW);
    if (cfg.title_filter) q = q.ilike('title', '%' + cfg.title_filter + '%');
    await q;
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'mark_read') {
    await supabase.from('contacts').update({ unread_count:0, first_unread_at:null }).eq('phone',phone).eq('owner',OW);
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'round_robin') {
    const branches = cfg.branches || [];
    let chosen = acctId, branchIdx = 0;
    if (branches.length) {
      const key = 'rr_' + nodeId;
      const { data:s } = await supabase.from('settings').select('value').eq('key', key).maybeSingle();
      let idx = parseInt(s?.value || '0', 10); if (isNaN(idx)) idx = 0;
      branchIdx = idx % branches.length;
      await supabase.from('settings').upsert({ key, value: String(idx + 1), updated_at: new Date().toISOString() });
      const accId = branches[branchIdx]?.account_id;
      if (accId) {
        chosen = accId;
        await supabase.from('contacts').update({ account_id: accId }).eq('phone', phone).eq('owner', OW);
        await supabase.from('bot_runs').update({ account_id: accId }).eq('id', runId);
      }
    }
    const nxt = await getNextNodeId(nodeId, String(branchIdx));
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt,account_id:chosen}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'wait_reply') {
    let pauseUntil = null;
    if (cfg.timeout_hours && cfg.timeout_hours > 0) pauseUntil = new Date(Date.now() + cfg.timeout_hours*3600000).toISOString();
    await supabase.from('bot_runs').update({ status:'waiting_reply', pause_until:pauseUntil, updated_at:new Date().toISOString() }).eq('id',runId);

  } else if (node.type === 'pause') {
    const ms = ((cfg.days||0)*24+(cfg.hours||0))*3600000 + (cfg.minutes||0)*60000 + (cfg.seconds||0)*1000;
    const pauseUntil = new Date(Date.now()+Math.max(ms,1000)).toISOString();
    await supabase.from('bot_runs').update({ status:'paused', pause_until:pauseUntil, updated_at:new Date().toISOString() }).eq('id',runId);

  } else if (node.type === 'business_hours') {
    const st = businessHoursState(Date.now(), cfg);
    if (st.open) {
      const nxt = await getNextNodeId(nodeId, '__open__');
      if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
      else await stopRun(runId,'completed');
    } else if (cfg.wait !== false) {
      await supabase.from('bot_runs').update({ status:'paused', pause_until:new Date(st.nextOpenMs).toISOString(), updated_at:new Date().toISOString() }).eq('id',runId);
    } else {
      const nxt = await getNextNodeId(nodeId, '__closed__');
      if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
      else await stopRun(runId,'completed');
    }

  } else if (node.type === 'move_stage') {
    if (cfg.stage_id) await supabase.from('contacts').update({ stage_id:cfg.stage_id }).eq('phone',phone).eq('owner',OW);
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');

  } else if (node.type === 'end') {
    await stopRun(runId,'completed');
  } else {
    const nxt = await getNextNodeId(nodeId, null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId); await processNode({...run,current_node_id:nxt}, depth+1); }
    else await stopRun(runId,'completed');
  }
}

async function handleBotReply(phone, text, owner) {
  if (!supabase) return false;
  let rq = supabase.from('bot_runs').select('*').eq('contact_phone',phone).eq('status','waiting_reply').order('created_at',{ascending:false}).limit(1);
  if (owner) rq = rq.eq('owner', owner);
  const { data:run } = await rq.maybeSingle();
  if (!run) return false;
  const { data:edges } = await supabase.from('bot_edges').select('*').eq('from_node_id', run.current_node_id);
  if (!edges?.length) { await stopRun(run.id,'completed'); return true; }
  const tl = text.toLowerCase().trim();
  let matched = null;
  for (const e of edges) {
    if (!e.label || e.label.startsWith('__')) continue;
    if (tl === e.label.toLowerCase() || tl.includes(e.label.toLowerCase())) { matched = e; break; }
  }
  if (!matched) matched = edges.find(e=>e.label==='__other__') || edges.find(e=>!e.label||e.label===''||e.label==='default') || edges[0];
  if (matched?.to_node_id) {
    const upd = { current_node_id:matched.to_node_id, status:'running', pause_until:null, updated_at:new Date().toISOString() };
    await supabase.from('bot_runs').update(upd).eq('id',run.id);
    await processNode({...run,...upd});
  } else { await stopRun(run.id,'completed'); }
  return true;
}

async function fireStageBots(phone, stageId, owner) {
  if (!supabase || !stageId || !phone) return;
  try {
    let bq = supabase.from('bots').select('*').eq('trigger_type','stage_enter').eq('trigger_stage_id',stageId).eq('active',true);
    if (owner) bq = bq.eq('owner', owner);
    const { data: bots } = await bq;
    if (!bots || !bots.length) return;
    let cq = supabase.from('contacts').select('account_id').eq('phone',phone);
    if (owner) cq = cq.eq('owner', owner);
    const { data: ct } = await cq.maybeSingle();
    const leadAcct = ct?.account_id || null;
    for (const bot of bots) {
      log(`🤖 Gatilho de etapa: bot "${bot.name}" para ${phone}`);
      await startBot(bot.id, phone, bot.account_id || leadAcct, owner || bot.owner);
    }
  } catch(e) { errorLog('fireStageBots error:', e.message); }
}

async function startBot(botId, phone, accountId, owner) {
  if (!supabase) return null;
  let ownerEmail = owner;
  if (!ownerEmail) { const { data:b } = await supabase.from('bots').select('owner').eq('id',botId).maybeSingle(); ownerEmail = b?.owner || null; }
  await supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() }).eq('contact_phone',phone).eq('bot_id',botId).in('status',['running','waiting_reply','paused']);
  const { data:startNodes } = await supabase.from('bot_nodes').select('id').eq('bot_id',botId).eq('type','start').limit(1);
  const startNode = startNodes && startNodes[0];
  if (!startNode) { errorLog('❌ Bot sem nó start:', botId); return null; }
  const { data:run, error } = await supabase.from('bot_runs').insert({
    bot_id:botId, contact_phone:phone, account_id:accountId||null,
    current_node_id:startNode.id, status:'running', owner:ownerEmail||null,
    created_at:new Date().toISOString(), updated_at:new Date().toISOString()
  }).select().single();
  if (error) { errorLog('❌ Bot run insert:', error.message); return null; }
  await processNode(run);
  return run;
}

setInterval(async () => {
  if (!supabase) return;
  const now = new Date().toISOString();
  const { data:paused } = await supabase.from('bot_runs').select('*').in('status',['paused','waiting_reply']).lte('pause_until',now).not('pause_until','is',null);
  for (const run of paused||[]) {
    const { data:curNode } = await supabase.from('bot_nodes').select('type').eq('id', run.current_node_id).maybeSingle();
    if (curNode?.type === 'business_hours') {
      await supabase.from('bot_runs').update({ status:'running', pause_until:null, updated_at:now }).eq('id',run.id);
      await processNode({...run, status:'running'});
      continue;
    }
    const nxt = await getNextNodeId(run.current_node_id,'__timeout__') || await getNextNodeId(run.current_node_id,'__other__') || await getNextNodeId(run.current_node_id,null);
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, status:'running', pause_until:null, updated_at:now }).eq('id',run.id); await processNode({...run,current_node_id:nxt,status:'running'}); }
    else { await stopRun(run.id,'completed'); }
  }
}, 5000);

// ── CRUD de Bots ──
app.get('/bots', async (req,res) => {
  if (!supabase) return res.json([]);
  const { data,error } = await supabase.from('bots').select('*').eq('owner', req.owner || ' ').order('created_at',{ascending:false});
  if (error) return res.status(500).json({error:error.message});
  res.json(data||[]);
});
app.get('/bots/:id', async (req,res) => {
  if (!supabase) return res.json({});
  const { data,error } = await supabase.from('bots').select('*').eq('id',req.params.id).eq('owner', req.owner || ' ').single();
  if (error) return res.status(404).json({error:error.message});
  res.json(data||{});
});
app.post('/bots', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const { name,trigger_type,trigger_stage_id,account_id } = req.body;
  const { data,error } = await supabase.from('bots').insert({ name:name||'Novo Bot', trigger_type:trigger_type||'manual', trigger_stage_id:trigger_stage_id||null, account_id:account_id||null, active:true, owner:req.owner||null }).select().single();
  if (error) return res.status(500).json({error:error.message});
  res.json(data);
});
app.put('/bots/:id', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const { name,trigger_type,trigger_stage_id,active } = req.body;
  const upd = {};
  if (name!==undefined) upd.name=name;
  if (trigger_type!==undefined) upd.trigger_type=trigger_type;
  if (trigger_stage_id!==undefined) upd.trigger_stage_id=trigger_stage_id||null;
  if (active!==undefined) upd.active=active;
  const { data,error } = await supabase.from('bots').update(upd).eq('id',req.params.id).eq('owner', req.owner || ' ').select().single();
  if (error) return res.status(500).json({error:error.message});
  res.json(data);
});
app.delete('/bots/:id', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const id = req.params.id;
  const { data: own } = await supabase.from('bots').select('id').eq('id',id).eq('owner', req.owner || ' ').maybeSingle();
  if (!own) return res.status(404).json({error:'Bot não encontrado'});
  await supabase.from('bot_runs').delete().eq('bot_id',id);
  await supabase.from('bot_edges').delete().eq('bot_id',id);
  await supabase.from('bot_nodes').delete().eq('bot_id',id);
  const { error } = await supabase.from('bots').delete().eq('id',id).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({error:error.message});
  res.json({success:true});
});
app.get('/bots/:id/flow', async (req,res) => {
  if (!supabase) return res.json({nodes:[],edges:[]});
  const id = req.params.id;
  const { data: own } = await supabase.from('bots').select('id').eq('id',id).eq('owner', req.owner || ' ').maybeSingle();
  if (!own) return res.json({nodes:[],edges:[]});
  const [nr,er] = await Promise.all([
    supabase.from('bot_nodes').select('*').eq('bot_id',id),
    supabase.from('bot_edges').select('*').eq('bot_id',id)
  ]);
  res.json({nodes:nr.data||[],edges:er.data||[]});
});
app.put('/bots/:id/flow', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const { nodes,edges } = req.body;
  const botId = req.params.id;
  const { data: own } = await supabase.from('bots').select('id').eq('id',botId).eq('owner', req.owner || ' ').maybeSingle();
  if (!own) return res.status(404).json({error:'Bot não encontrado'});
  try {
    await supabase.from('bot_edges').delete().eq('bot_id',botId);
    await supabase.from('bot_nodes').delete().eq('bot_id',botId);
    if (nodes?.length) { const { error:ne } = await supabase.from('bot_nodes').insert(nodes.map(n=>({ id:n.id, bot_id:botId, type:n.type, label:n.label||'', config:n.config||{}, pos_x:Math.round(n.pos_x||0), pos_y:Math.round(n.pos_y||0), owner:req.owner||null }))); if (ne) throw ne; }
    if (edges?.length) { const { error:ee } = await supabase.from('bot_edges').insert(edges.map(e=>({ id:e.id, bot_id:botId, from_node_id:e.from_node_id, to_node_id:e.to_node_id, label:e.label||'', owner:req.owner||null }))); if (ee) throw ee; }
    res.json({success:true});
  } catch(err) { res.status(500).json({error:err.message}); }
});
app.post('/bots/:id/duplicate', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const srcId = req.params.id;
  const { data: bot, error: be } = await supabase.from('bots').select('*').eq('id', srcId).eq('owner', req.owner || ' ').single();
  if (be || !bot) return res.status(404).json({error:'Bot não encontrado'});
  const { data: newBot, error: ne } = await supabase.from('bots').insert({
    name: (bot.name || 'Bot') + ' (cópia)', trigger_type: 'manual', trigger_stage_id: null,
    account_id: bot.account_id || null, active: false, owner: req.owner || null
  }).select().single();
  if (ne) return res.status(500).json({error:ne.message});
  const [{data:nodes},{data:edges}] = await Promise.all([
    supabase.from('bot_nodes').select('*').eq('bot_id', srcId),
    supabase.from('bot_edges').select('*').eq('bot_id', srcId)
  ]);
  let c = 0;
  const genId = () => 'n' + Date.now().toString(36) + (c++).toString(36) + Math.random().toString(36).substring(2,5);
  const idMap = {};
  (nodes || []).forEach(n => { idMap[n.id] = genId(); });
  if (nodes?.length) {
    const { error } = await supabase.from('bot_nodes').insert(nodes.map(n => ({
      id: idMap[n.id], bot_id: newBot.id, type: n.type, label: n.label || '', config: n.config || {}, pos_x: n.pos_x || 0, pos_y: n.pos_y || 0, owner: req.owner || null
    })));
    if (error) return res.status(500).json({error:error.message});
  }
  if (edges?.length) {
    const rows = edges.map(e => ({ id: genId(), bot_id: newBot.id, from_node_id: idMap[e.from_node_id], to_node_id: idMap[e.to_node_id], label: e.label || '', owner: req.owner || null }))
                      .filter(e => e.from_node_id && e.to_node_id);
    if (rows.length) { const { error } = await supabase.from('bot_edges').insert(rows); if (error) return res.status(500).json({error:error.message}); }
  }
  res.json({ success:true, id:newBot.id });
});
app.post('/bots/:id/start', async (req,res) => {
  const { phone,account_id } = req.body;
  if (!phone) return res.status(400).json({error:'phone obrigatório'});
  const { data: own } = await supabase.from('bots').select('id').eq('id',req.params.id).eq('owner', req.owner || ' ').maybeSingle();
  if (!own) return res.status(404).json({error:'Bot não encontrado'});
  const run = await startBot(req.params.id, phone, account_id, req.owner);
  if (!run) return res.status(500).json({error:'Erro ao iniciar bot (verifique se o fluxo tem nó Início)'});
  res.json({success:true, run_id:run.id});
});
app.post('/bot-runs/:id/stop', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  await supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() }).eq('id',req.params.id).eq('owner', req.owner || ' ');
  res.json({success:true});
});
app.get('/bot-runs/contact/:phone', async (req,res) => {
  if (!supabase) return res.json([]);
  const { data } = await supabase.from('bot_runs').select('*, bots(name)').eq('contact_phone',req.params.phone).eq('owner', req.owner || ' ').in('status',['running','waiting_reply','paused']).order('created_at',{ascending:false});
  res.json(data||[]);
});

// ═══════════════════════════════════════
// SETTINGS + N8N
// ═══════════════════════════════════════

let _settings = {};
async function loadSettings() {
  if (!supabase) return;
  try {
    const { data } = await supabase.from('settings').select('key, value');
    for (const row of data || []) _settings[row.key] = row.value;
    log('✅ Settings carregados:', Object.keys(_settings).join(', ') || '(nenhum)');
  } catch(e) { errorLog('Settings load error:', e.message); }
}
loadSettings();

app.get('/settings/:key', async (req, res) => {
  if (!supabase) return res.json({ value: null });
  const { data } = await supabase.from('settings').select('value').eq('key', req.params.key).maybeSingle();
  res.json({ value: data?.value || null });
});

app.put('/settings/:key', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { value } = req.body;
  const { error } = await supabase.from('settings').upsert({ key: req.params.key, value, updated_at: new Date().toISOString() });
  if (error) return res.status(500).json({ error: error.message });
  _settings[req.params.key] = value;
  res.json({ success: true });
});

app.post('/n8n/test', async (req, res) => {
  const n8nUrl = _settings['n8n_webhook_url'];
  if (!n8nUrl) return res.status(400).json({ error: 'URL do N8N não configurada' });
  try {
    await axios.post(n8nUrl, {
      event: 'test',
      phone: '5500000000000',
      name: 'Teste MeuCRM',
      content: 'Esta é uma mensagem de teste enviada pelo MeuCRM ✅',
      type: 'text',
      timestamp: new Date().toISOString(),
      account_id: null,
      media_id: null,
      media_mime_type: null
    }, { timeout: 10000 });
    res.json({ success: true });
  } catch(e) {
    res.status(500).json({ error: 'Não conseguiu conectar: ' + e.message });
  }
});

// ═══════════════════════════════════════
// EVOLUTION API (mantida)
// ═══════════════════════════════════════

const EVOLUTION_URL = (process.env.EVOLUTION_API_URL || 'https://evolution-api-production-ac49c.up.railway.app').replace(/\/$/, '');
const EVOLUTION_KEY = process.env.EVOLUTION_API_KEY || 'meucrm2024';
const BACKEND_URL   = process.env.BACKEND_PUBLIC_URL || 'https://meucrm-backend-production-d4f4.up.railway.app';

const evoHdr = () => ({ apikey: EVOLUTION_KEY, 'Content-Type': 'application/json' });
const qrCache = {};

async function sendViaEvolution(instanceName, to, text) {
  const r = await axios.post(`${EVOLUTION_URL}/message/sendText/${instanceName}`, {
    number: to,
    text,
    options: { delay: 1000 }
  }, { headers: evoHdr(), timeout: 15000 });
  return r.data;
}

app.post('/evolution/connect', async (req, res) => {
  const instanceName = `meucrm_${Date.now()}`;
  try {
    try {
      const { data: list } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
      for (const inst of list || []) {
        const name = inst.instance?.instanceName || inst.instanceName || inst.name;
        const status = inst.instance?.connectionStatus || inst.connectionStatus;
        if (name && name.startsWith('meucrm_') && status !== 'open') {
          await axios.delete(`${EVOLUTION_URL}/instance/delete/${name}`, { headers: evoHdr(), timeout: 8000 }).catch(() => {});
          log('🗑️ Instância antiga removida:', name);
        }
      }
    } catch(cleanErr) { log('Cleanup warn:', cleanErr.message); }

    const webhookUrl = `${BACKEND_URL}/evolution-webhook`;
    const { data } = await axios.post(`${EVOLUTION_URL}/instance/create`, {
      instanceName,
      qrcode: true,
      integration: 'WHATSAPP-BAILEYS',
      webhook: {
        url: webhookUrl,
        byEvents: false,
        base64: false,
        events: ['QRCODE_UPDATED', 'MESSAGES_UPSERT', 'CONNECTION_UPDATE']
      }
    }, { headers: evoHdr(), timeout: 15000 });

    let qr = data?.qrcode?.base64 || data?.base64 || null;

    if (!qr) {
      log(`⏳ QR não veio na criação, tentando rápido via /instance/connect...`);
      for (let i = 0; i < 3; i++) {
        await new Promise(r => setTimeout(r, 2000));
        try {
          const { data: qrData } = await axios.get(
            `${EVOLUTION_URL}/instance/connect/${instanceName}`,
            { headers: evoHdr(), timeout: 8000 }
          );
          qr = qrData?.base64 || qrData?.qrcode?.base64 || null;
          if (qr) { log(`✅ QR obtido na tentativa ${i+1}`); break; }
        } catch(qrErr) { log('QR attempt error:', qrErr.message); }
      }
    }

    log(`Instância criada: ${instanceName}, QR: ${qr ? 'SIM' : 'NAO'}`);
    res.json({ success: true, instance: instanceName, qr });
  } catch(e) {
    errorLog('Evolution create error:', e.response?.data || e.message);
    res.status(500).json({ error: e.response?.data?.message || e.message });
  }
});

app.get('/evolution/qr/:instance', async (req, res) => {
  if (qrCache[req.params.instance]) {
    return res.json({ qr: qrCache[req.params.instance], code: null, pairingCode: null, raw: { cached: true } });
  }
  try {
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/connect/${req.params.instance}`, {
      headers: evoHdr(), timeout: 10000
    });
    const qr = data?.base64 || data?.qrcode?.base64 || null;
    const code = data?.code || data?.qrcode?.code || null;
    res.json({ qr, code, pairingCode: data?.pairingCode || null, raw: data });
  } catch(e) {
    errorLog('Evolution QR error:', e.response?.data || e.message);
    try {
      const { data: d2 } = await axios.get(`${EVOLUTION_URL}/instance/qrcode/${req.params.instance}`, { headers: evoHdr(), timeout: 8000, params: { image: true } });
      const qr = d2?.base64 || d2?.qrcode?.base64 || null;
      const code = d2?.code || d2?.qrcode?.code || null;
      return res.json({ qr, code, raw: d2 });
    } catch(e2) {}
    res.status(500).json({ error: e.message, qr: null, raw: e.response?.data });
  }
});

app.get('/evolution/debug', async (req, res) => {
  try {
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
    res.json({ instances: data, url: EVOLUTION_URL });
  } catch(e) {
    res.status(500).json({ error: e.message, url: EVOLUTION_URL, detail: e.response?.data });
  }
});

app.get('/evolution/status/:instance', async (req, res) => {
  try {
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/connectionState/${req.params.instance}`, { headers: evoHdr(), timeout: 10000 });
    const state = data?.instance?.state || data?.state || 'close';
    let phone = null;
    if (state === 'open') {
      try {
        const { data: list } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
        const inst = (list || []).find(i => (i.instance?.instanceName || i.instanceName || i.name) === req.params.instance);
        const ownerJid = inst?.instance?.ownerJid || inst?.ownerJid || '';
        if (ownerJid) phone = ownerJid.replace('@s.whatsapp.net', '').replace(/\D/g, '') || null;
      } catch(e2) { log('Fetch instances err:', e2.message); }
    }
    res.json({ state, phone });
  } catch(e) {
    res.status(500).json({ error: e.message, state: 'close' });
  }
});

app.post('/evolution/save-account', async (req, res) => {
  const { instance, phone } = req.body;
  if (!instance) return res.status(400).json({ error: 'instance obrigatório' });
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const name = phone ? `WhatsApp ${phone}` : `WhatsApp QR (${instance})`;
  const { data, error } = await supabase.from('accounts')
    .upsert({ name, type: 'evolution', evolution_instance: instance, phone_display: phone || null, phone_number_id: instance, token: '' }, { onConflict: 'phone_number_id' })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  log('✅ Conta Evolution salva:', name);
  res.json({ success: true, data });
});

app.delete('/evolution/disconnect/:instance', async (req, res) => {
  try {
    await axios.delete(`${EVOLUTION_URL}/instance/delete/${req.params.instance}`, { headers: evoHdr(), timeout: 10000 });
    if (supabase) await supabase.from('accounts').delete().eq('evolution_instance', req.params.instance);
    res.json({ success: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/evolution-webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const { event, instance: instanceName, data } = req.body;
    log('📩 Evolution webhook:', event, instanceName);

    if (event === 'messages.upsert') {
      if (!data) return;
      const fromMe    = !!data.key?.fromMe;
      const remoteJid = data.key?.remoteJid || '';
      if (remoteJid.includes('@g.us')) return;

      const phone     = remoteJid.replace('@s.whatsapp.net', '');
      const name      = data.pushName || phone;
      const timestamp = new Date((data.messageTimestamp || Date.now() / 1000) * 1000).toISOString();
      const wamid     = data.key?.id || null;
      const direction = fromMe ? 'outbound' : 'inbound';

      let content = fromMe ? '[Mensagem enviada]' : '[Mensagem recebida]', type = 'text';
      const msg = data.message || {};
      if      (msg.conversation)          { content = msg.conversation; type = 'text'; }
      else if (msg.extendedTextMessage)   { content = msg.extendedTextMessage.text || ''; type = 'text'; }
      else if (msg.imageMessage)          { content = msg.imageMessage.caption || '[Imagem]'; type = 'image'; }
      else if (msg.audioMessage || msg.pttMessage) { content = '[Áudio]'; type = 'audio'; }
      else if (msg.videoMessage)          { content = msg.videoMessage.caption || '[Vídeo]'; type = 'video'; }
      else if (msg.documentMessage)       { content = msg.documentMessage.fileName || '[Documento]'; type = 'document'; }

      let accountId = null;
      if (supabase && instanceName) {
        const { data: acc } = await supabase.from('accounts').select('id').eq('evolution_instance', instanceName).maybeSingle();
        if (acc) accountId = acc.id;
      }

      if (supabase) {
        if (wamid) {
          const { data: exists } = await supabase.from('messages').select('id').eq('wamid', wamid).maybeSingle();
          if (exists) return;
        }

        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = { phone, name, last_message_at: timestamp, last_message_preview: preview, last_message_direction: direction };
        if (accountId) contactData.account_id = accountId;
        await supabase.from('contacts').upsert(contactData, { onConflict: 'phone' });

        if (!fromMe) {
          const { data: cRow } = await supabase.from('contacts').select('unread_count, first_unread_at').eq('phone', phone).maybeSingle();
          const currentUnread = cRow?.unread_count || 0;
          const unreadUpdate = { unread_count: currentUnread + 1 };
          if (currentUnread === 0) unreadUpdate.first_unread_at = timestamp;
          await supabase.from('contacts').update(unreadUpdate).eq('phone', phone);
        }

        const msgData = { phone, content, type, direction, timestamp, wamid };
        if (accountId) msgData.account_id = accountId;
        await supabase.from('messages').insert(msgData);

        if (!fromMe && type === 'text' && content) {
          try { await handleBotReply(phone, content); } catch(be) { errorLog('Bot reply error:', be.message); }
        }
        if (!fromMe) {
          const n8nUrl = _settings['n8n_webhook_url'];
          if (n8nUrl) {
            try { await axios.post(n8nUrl, { event: 'message_received', phone, name, content, type, timestamp, account_id: accountId || null }, { timeout: 8000 }); } catch(ne) {}
          }
        }
      }
    } else if (event === 'qrcode.updated') {
      const b64 = data?.qrcode?.base64 || data?.base64 || null;
      if (b64) {
        qrCache[instanceName] = b64.startsWith('data:') ? b64 : 'data:image/png;base64,' + b64;
        log(`📲 QR cacheado para ${instanceName}`);
      }
    } else if (event === 'connection.update') {
      log(`🔌 Evolution ${instanceName}: ${data?.state}`);
      if (data?.state === 'open' || data?.state === 'close') delete qrCache[instanceName];
    }
  } catch(err) {
    errorLog('Evolution webhook error:', err.message);
  }
});

app.listen(PORT, () => log(`🚀 MeuCRM na porta ${PORT}`));
