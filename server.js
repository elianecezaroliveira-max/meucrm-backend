const express = require("express");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const cors = require("cors");

const app = express();
app.use(express.json({ limit: '30mb' })); // suporta fotos/vídeos em base64 (limite WhatsApp ~16MB → ~22MB em base64)
app.use(cors());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const APP_ID = process.env.APP_ID;
const APP_SECRET = process.env.APP_SECRET;
const PORT = process.env.PORT || 3000;

let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log("✅ Supabase conectado!");
}

// ── Multi-tenant: identifica o usuário logado (dono) a partir do token do Supabase ──
const SUPABASE_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlmcnhneWhreWdxaGpyd3Brc3J5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg1Mjk4MDcsImV4cCI6MjA5NDEwNTgwN30.6vjbaJdWk-u55xegMrHnv64pvlo0DByfPdtDSj2C7z4';
const _tokenOwner = {}; // cache token -> { email, ts }
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

app.get("/", (req, res) => res.send("✅ VETRA Backend funcionando!"));

// ── Verificação do Webhook ──
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    console.log("✅ Webhook verificado!");
    res.status(200).send(challenge);
  } else {
    res.sendStatus(403);
  }
});

// Traduz o erro de entrega da Meta para um texto sempre preenchido (Meta + sistema)
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
    // Temos tradução em português: usa só ela (não anexa o texto em inglês da Meta)
    txt = META_ERROR_CODES[code];
  } else {
    // Sem tradução: usa o que a Meta mandou (em inglês mesmo) para não ficar sem motivo
    const parts = [];
    if (er.title) parts.push(er.title);
    if (er.error_data?.details) parts.push(er.error_data.details);
    else if (er.message) parts.push(er.message);
    txt = parts.filter(Boolean).join(' — ') || 'Falha no envio reportada pela Meta.';
  }
  if (code) txt += ` (código ${code})`;
  return txt;
}

// Buffer de status que chegam ANTES da mensagem ser salva (corrige ✓ que não vira ✓✓)
const _pendingStatuses = {}; // wamid -> { status, error_info, ts }
function _cachePendingStatus(wamid, upd) {
  if (!wamid) return;
  _pendingStatuses[wamid] = { ...upd, ts: Date.now() };
  // limpa entradas com mais de 10 min
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

// ── Normalização de telefone BR (nono dígito) ──
// O WhatsApp pode devolver o número do cliente SEM o nono dígito (55 DDD 8ddddddd)
// mesmo quando o envio foi feito COM ele (55 DDD 9dddddddd) — o que criava uma
// conversa nova quando o cliente respondia a um template.
function phoneVariants(phone) {
  const p = String(phone || '').replace(/\D/g, '');
  const set = new Set([p]);
  if (/^55\d{2}9\d{8}$/.test(p)) set.add(p.slice(0, 4) + p.slice(5));      // com 9 → sem 9
  if (/^55\d{2}[6-9]\d{7}$/.test(p)) set.add(p.slice(0, 4) + '9' + p.slice(4)); // sem 9 → com 9
  return [...set];
}
// Se já existe contato numa variante equivalente, usa o telefone JÁ CADASTRADO
async function resolveExistingPhone(phone, owner) {
  if (!supabase) return phone;
  const variants = phoneVariants(phone);
  if (variants.length === 1) return phone;
  const { data } = await supabase.from('contacts').select('phone, last_message_at')
    .in('phone', variants).eq('owner', owner || ' ')
    .order('last_message_at', { ascending: false }).limit(1).maybeSingle();
  return data?.phone || phone;
}

// ── Receber mensagens ──
app.post("/webhook", async (req, res) => {
  // Responde 200 IMEDIATAMENTE — se a resposta demorar, a Meta reenvia o webhook
  // e a mensagem chega duplicada no CRM.
  res.sendStatus(200);
  try {
    const body = req.body;

    // Log para debug - mostra o que chegou
    console.log("📩 Webhook recebido:", JSON.stringify(body).substring(0, 300));

    if (body.object !== "whatsapp_business_account") {
      console.log("⚠️ Objeto ignorado:", body.object);
      return;
    }

    // Percorre TODOS os entries (a Meta pode agrupar vários)
    const changes = (body.entry || []).flatMap(e => e.changes || []);
    if (!changes.length) return;

    for (const change of changes) {
      const value = change.value;

      // Handle status updates (read receipts)
      if (value?.statuses?.length && supabase) {
        for (const st of value.statuses) {
          const { id: wamid, status } = st;
          if (wamid && ['sent','delivered','read','failed'].includes(status)) {
            const upd = { status };
            if (status === 'failed') {
              upd.error_info = metaErrorText(st.errors?.[0]);
              console.error('❌ Entrega falhou:', wamid, upd.error_info);
            }
            _cachePendingStatus(wamid, upd); // guarda caso a msg ainda não esteja salva
            await supabase.from("messages").update(upd).eq("wamid", wamid);
          }
        }
      }

      if (!value?.messages?.length) continue;

      // Processa TODAS as mensagens do lote (a Meta pode agrupar várias num só webhook)
      for (const message of value.messages) {
      const contact = value.contacts?.[0];
      let from = message.from;
      const name = contact?.profile?.name || "Desconhecido";
      const timestamp = new Date(parseInt(message.timestamp) * 1000).toISOString();
      const phoneNumberId = value.metadata?.phone_number_id;

      // Reação a uma mensagem (lead reagiu a uma mensagem minha)
      if (message.type === 'reaction') {
        const emoji = message.reaction?.emoji || null; // vazio = reação removida
        const targetWamid = message.reaction?.message_id;
        if (supabase && targetWamid) {
          await supabase.from('messages').update({ reaction: emoji, reaction_by: 'contact' }).eq('wamid', targetWamid);
          console.log(`😀 Reação ${emoji||'(removida)'} em ${targetWamid}`);
        }
        continue;
      }

      // Dedup: a Meta reenvia webhooks — ignora mensagem que já está salva
      if (supabase && message.id) {
        const { data: dupe } = await supabase.from("messages").select("id").eq("wamid", message.id).maybeSingle();
        if (dupe) { console.log("↩️ Webhook duplicado ignorado:", message.id); continue; }
      }

      console.log(`📨 Mensagem de ${name} (${from}) via número ${phoneNumberId}`);

      // Busca account_id + dono (owner) — roteia a mensagem para o usuário certo
      let accountId = null;
      let ownerEmail = null;
      if (supabase && phoneNumberId) {
        const { data: account, error: accErr } = await supabase
          .from("accounts").select("id, owner").eq("phone_number_id", phoneNumberId).maybeSingle();
        if (accErr) console.error("❌ Erro ao buscar conta:", accErr.message);
        if (account) {
          accountId = account.id;
          ownerEmail = account.owner || null;
          console.log("✅ Conta encontrada:", accountId, "dono:", ownerEmail);
        } else {
          console.log("⚠️ Nenhuma conta com phone_number_id:", phoneNumberId, "- salvando sem account_id");
        }
      }

      // Unifica a conversa se o contato já existe com/sem o nono dígito
      const resolvedFrom = await resolveExistingPhone(from, ownerEmail);
      if (resolvedFrom !== from) {
        console.log(`🔗 Número ${from} unificado com contato existente ${resolvedFrom}`);
        from = resolvedFrom;
      }

      // Extrai conteúdo da mensagem
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
        // Botão de resposta rápida de um template aprovado
        content = message.button?.text || "[Botão]";
      } else if (type === "interactive") {
        // Botões/listas interativas
        const it = message.interactive;
        content = it?.button_reply?.title || it?.list_reply?.title || it?.nfm_reply?.name || "[Resposta interativa]";
      } else {
        content = `[Mensagem do tipo: ${type}]`;
      }

      if (supabase) {
        // Já existe? (para NÃO sobrescrever o nome que o usuário editou manualmente)
        const { data: existing } = await supabase
          .from("contacts").select("name, unread_count, first_unread_at").eq("phone", from).eq("owner", ownerEmail || ' ').maybeSingle();

        // Salva contato com prévia da última mensagem
        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = {
          phone: from, last_message_at: timestamp,
          last_message_preview: preview,
          last_message_direction: 'inbound',
        };
        if (!existing) contactData.name = name; // só define o nome do WhatsApp na CRIAÇÃO; depois respeita o editado
        if (accountId) contactData.account_id = accountId;
        if (ownerEmail) contactData.owner = ownerEmail; // dono = dono da conta de WhatsApp

        const { error: contactErr } = await supabase
          .from("contacts")
          .upsert(contactData, { onConflict: "owner,phone" });

        if (contactErr) {
          console.error("❌ Erro ao salvar contato:", contactErr.message, contactErr.details);
        } else {
          console.log("✅ Contato salvo:", from);
        }

        // Incrementa contador de não lidas e marca hora da 1ª mensagem não lida
        const currentUnread = existing?.unread_count || 0;
        const unreadUpdate = { unread_count: currentUnread + 1 };
        if (currentUnread === 0) unreadUpdate.first_unread_at = timestamp; // só na 1ª mensagem não lida
        await supabase.from("contacts").update(unreadUpdate).eq("phone", from).eq("owner", ownerEmail || ' ');

        // Salva mensagem
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
        if (accountId) messageData.account_id = accountId; // só inclui se não for null
        if (ownerEmail) messageData.owner = ownerEmail;

        const { error: msgErr } = await supabase.from("messages").insert(messageData);

        if (msgErr) {
          console.error("❌ Erro ao salvar mensagem:", msgErr.message, msgErr.details);
        } else {
          console.log("✅ Mensagem salva:", content.substring(0, 50));
        }

        // Notificação push nos aparelhos do dono (não bloqueia o processamento)
        sendPushToOwner(ownerEmail, {
          title: existing?.name || name || from,
          body: preview,
          phone: from,
          tag: 'chat-' + from,
        }).catch(() => {});
        // Processa reply de bot ativo (texto OU clique em botão/lista)
        if (['text','button','interactive'].includes(type) && content) {
          try { await handleBotReply(from, content, ownerEmail); } catch(be) { console.error('Bot reply error:', be.message); }
        }
        // Encaminha para N8N se configurado
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
          } catch(ne) { console.error('N8N forward error:', ne.message); }
        }
      }
      } // fim do for (message of value.messages)
    }
  } catch (err) {
    console.error("❌ Erro no webhook:", err.message);
  }
});

// ── Embedded Signup: recebe código do Facebook e salva conta automaticamente ──
app.post("/auth/whatsapp", async (req, res) => {
  const { code, redirect_uri } = req.body;
  if (!code) return res.status(400).json({ error: "Código não informado" });
  if (!APP_ID || !APP_SECRET) return res.status(500).json({ error: "APP_ID e APP_SECRET não configurados" });

  try {
    // 1. Troca código pelo access token
    const tokenParams = { client_id: APP_ID, client_secret: APP_SECRET, code };
    if (redirect_uri) tokenParams.redirect_uri = redirect_uri;

    const tokenRes = await axios.get("https://graph.facebook.com/v23.0/oauth/access_token", {
      params: tokenParams,
    });
    const userToken = tokenRes.data.access_token;
    console.log("✅ Token obtido via Embedded Signup");

    // 2. Usa debug_token para obter WABA IDs das permissões granulares
    // (não requer business_management — funciona com whatsapp_business_management)
    const appToken = `${APP_ID}|${APP_SECRET}`;
    const debugRes = await axios.get("https://graph.facebook.com/v23.0/debug_token", {
      params: { input_token: userToken, access_token: appToken },
    });

    const granularScopes = debugRes.data.data?.granular_scopes || [];
    const wabaScope = granularScopes.find(s => s.scope === "whatsapp_business_management");
    const wabaIds = wabaScope?.target_ids || [];
    console.log("✅ WABA IDs encontrados via debug_token:", wabaIds);

    const savedAccounts = [];

    for (const wabaId of wabaIds) {
      // 3. Busca nome do WABA
      let wabaName = wabaId;
      try {
        const wabaRes = await axios.get(`https://graph.facebook.com/v23.0/${wabaId}`, {
          params: { access_token: userToken, fields: "id,name" },
        });
        wabaName = wabaRes.data.name || wabaId;
      } catch (e) {
        console.log("⚠️ Não foi possível buscar nome do WABA:", e.response?.data?.error?.message);
      }

      // 4. Busca números de telefone do WABA
      const phonesRes = await axios.get(`https://graph.facebook.com/v23.0/${wabaId}/phone_numbers`, {
        params: { access_token: userToken, fields: "id,display_phone_number,verified_name" },
      });
      const phones = phonesRes.data.data || [];
      console.log(`📞 ${phones.length} número(s) encontrado(s) no WABA ${wabaId}`);

      for (const phone of phones) {
        // 5. Registra o número na Cloud API (ativa o número de "Pendente" para "Ativo")
        try {
          await axios.post(
            `https://graph.facebook.com/v23.0/${phone.id}/register`,
            { messaging_product: "whatsapp", pin: process.env.WHATSAPP_PIN || "123456" },
            { headers: { Authorization: `Bearer ${userToken}`, "Content-Type": "application/json" } }
          );
          console.log("✅ Número registrado na Cloud API:", phone.display_phone_number);
        } catch (e) {
          console.log("⚠️ Registro do número (pode já estar ativo):", e.response?.data?.error?.message);
        }

        // 6. Inscreve WABA no webhook do app
        try {
          await axios.post(
            `https://graph.facebook.com/v23.0/${wabaId}/subscribed_apps`,
            {},
            { params: { access_token: userToken } }
          );
          console.log("✅ WABA inscrito no webhook:", wabaId);
        } catch (e) {
          console.log("⚠️ Aviso webhook subscribe:", e.response?.data?.error?.message);
        }

        // 6. Salva conta no Supabase
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
            console.log("✅ Conta salva:", accountData.name);
          } else {
            console.error("❌ Erro ao salvar conta:", error.message);
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
    console.error("❌ Erro auth:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao conectar com o Facebook" });
  }
});

// ── Listar contas ──
app.get("/accounts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("accounts").select("id, name, phone_number_id, phone_display, type, evolution_instance, created_at")
    .eq("owner", req.owner || ' ')
    .order("created_at", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ── Adicionar conta manualmente ──
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

// ── Remover conta ──
app.delete("/accounts/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("accounts").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Enviar mensagem ──
app.post("/send", async (req, res) => {
  let { to, message, account_id, quoted_id, quoted_content, quoted_direction } = req.body;
  if (!to || !message) return res.status(400).json({ error: "Informe 'to' e 'message'" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito

  let phoneNumberId, token, evolutionInstance = null, accountType = 'cloudapi';

  // 1. Tenta buscar conta do banco de dados pelo account_id
  if (supabase && account_id) {
    const { data: account, error: accErr } = await supabase
      .from("accounts").select("phone_number_id, token, type, evolution_instance").eq("id", account_id).single();
    if (accErr) console.error("❌ Erro ao buscar conta para envio:", accErr.message);
    if (account) {
      phoneNumberId = account.phone_number_id;
      token = account.token;
      accountType = account.type || 'cloudapi';
      evolutionInstance = account.evolution_instance || null;
    }
  }

  // 2. Fallback: usa variáveis de ambiente (PHONE_NUMBER_ID + WHATSAPP_TOKEN)
  if (!evolutionInstance && (!phoneNumberId || !token)) {
    phoneNumberId = process.env.PHONE_NUMBER_ID;
    token = process.env.WHATSAPP_TOKEN;
    if (phoneNumberId && token) {
      console.log("⚠️ Conta não encontrada no banco — usando credenciais das variáveis de ambiente");
    }
  }

  // 3. Envio via Evolution API (QR Code)
  if (accountType === 'evolution' && evolutionInstance) {
    try {
      const evoRes = await sendViaEvolution(evolutionInstance, to, message);
      const wamid = evoRes?.key?.id || null; // mesmo id que volta no webhook → permite dedup
      if (supabase) {
        const safeAccountId = account_id || null;
        const preview = message.length > 80 ? message.substring(0, 80) + '…' : message;
        // Inclui owner — sem ele a mensagem não aparece no CRM (o GET /messages filtra por owner)
        await supabase.from('contacts').upsert({ phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId, last_message_preview: preview, last_message_direction: 'outbound', owner: req.owner || null }, { onConflict: 'owner,phone' });
        await supabase.from('messages').insert({ phone: to, content: message, type: 'text', direction: 'outbound', timestamp: new Date().toISOString(), account_id: safeAccountId, wamid, owner: req.owner || null, quoted_id: quoted_id || null, quoted_content: quoted_content || null, quoted_direction: quoted_direction || null });
      }
      return res.json({ success: true, via: 'evolution' });
    } catch(e) {
      console.error('Evolution send error:', e.response?.data || e.message);
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
      if (msgErr) {
        console.error("❌ Erro ao salvar mensagem enviada:", msgErr.message, msgErr.details);
      } else {
        await applyPendingStatus(wamid);
        console.log("✅ Mensagem enviada salva no banco:", message.substring(0, 50));
      }
    }
    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("❌ Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem", detail: err.response?.data });
  }
});

// ── Reagir a uma mensagem com emoji (passe emoji vazio para remover) ──
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
    console.error("❌ Erro ao reagir:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao reagir", detail: err.response?.data });
  }
});

// ── Conversão de áudio para OGG/Opus (formato de voz do WhatsApp) ──
// Gravações do navegador (MP4 fragmentado do iPhone, WebM do Android) são
// rejeitadas pela Meta com o erro 131053 — a conversão resolve os dois casos.
let _ffmpeg = null;
try {
  _ffmpeg = require('fluent-ffmpeg');
  _ffmpeg.setFfmpegPath(require('@ffmpeg-installer/ffmpeg').path);
  console.log('✅ ffmpeg disponível (conversão de áudio ativa)');
} catch (e) { console.log('⚠️ ffmpeg não instalado — áudios gravados serão enviados sem conversão'); }

function convertAudioToOpus(buf) {
  return new Promise((resolve, reject) => {
    if (!_ffmpeg) return reject(new Error('ffmpeg indisponível'));
    const os = require('os'), fs = require('fs'), path = require('path');
    const inFile = path.join(os.tmpdir(), 'rec_' + Date.now() + '_' + Math.random().toString(36).slice(2));
    const outFile = inFile + '.ogg';
    const cleanup = () => { try { fs.unlinkSync(inFile); } catch (_) {} try { fs.unlinkSync(outFile); } catch (_) {} };
    fs.writeFileSync(inFile, buf);
    _ffmpeg(inFile)
      .noVideo()
      .audioCodec('libopus').audioBitrate('32k').audioChannels(1).audioFrequency(48000)
      .format('ogg')
      .on('end', () => {
        try { const out = fs.readFileSync(outFile); cleanup(); resolve(out); }
        catch (e) { cleanup(); reject(e); }
      })
      .on('error', err => { cleanup(); reject(err); })
      .save(outFile);
  });
}

// ── Enviar mídia (imagem, PDF, vídeo, etc.) ──
app.post("/send-media", async (req, res) => {
  let { to, account_id, fileBase64, fileName, mimeType } = req.body;
  if (!to || !fileBase64 || !fileName || !mimeType)
    return res.status(400).json({ error: "Informe to, fileBase64, fileName e mimeType" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito

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
    // 0. Áudio gravado no navegador → converte para OGG/Opus (voz do WhatsApp)
    let fileBuf = Buffer.from(fileBase64, "base64");
    let sendMime = mimeType, sendName = fileName;
    const baseMime = String(mimeType).split(";")[0].trim();
    if (baseMime.startsWith("audio/") && !["audio/ogg", "audio/mpeg", "audio/aac", "audio/amr"].includes(baseMime)) {
      try {
        fileBuf = await convertAudioToOpus(fileBuf);
        sendMime = "audio/ogg";
        sendName = fileName.replace(/\.[^.]+$/, "") + ".ogg";
        console.log(`🎙️ Áudio convertido para OGG/Opus (${fileBuf.length} bytes)`);
      } catch (convErr) {
        console.error("⚠️ Conversão de áudio falhou, enviando original:", convErr.message);
      }
    }

    // 1. Faz upload da mídia para a Meta
    const FormData = require("form-data");
    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", sendMime);
    form.append("file", fileBuf, {
      filename: sendName,
      contentType: sendMime,
    });

    const uploadRes = await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/media`,
      form,
      { headers: { ...form.getHeaders(), Authorization: `Bearer ${token}` } }
    );
    const mediaId = uploadRes.data.id;
    console.log("✅ Mídia enviada para Meta, id:", mediaId);

    // 2. Determina o tipo de mensagem WhatsApp
    let msgType = "document";
    if (mimeType.startsWith("image/")) msgType = "image";
    else if (mimeType.startsWith("video/")) msgType = "video";
    else if (mimeType.startsWith("audio/")) msgType = "audio";

    const mediaObj = { id: mediaId };
    if (msgType === "document") mediaObj.filename = fileName;

    // 3. Envia a mensagem de mídia
    const mediaResp = await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product: "whatsapp", to, type: msgType, [msgType]: mediaObj },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    // 4. Salva no Supabase
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
        media_id: mediaId, media_mime_type: sendMime, // permite exibir a mídia no CRM
      });
      await applyPendingStatus(mediaWamid);
    }
    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erro ao enviar mídia:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mídia", detail: err.response?.data });
  }
});

// ── Proxy de mídia recebida (imagem, áudio, vídeo, documento) ──
// Faz STREAMING direto da Meta repassando o Range — método correto para vídeo
// (sem baixar o arquivo inteiro na memória, evita travar a reprodução).
const mediaUrlCache = new Map(); // mediaId_token -> { url, ts }

async function getMediaUrl(mediaId, token, cacheKey, force) {
  const hit = mediaUrlCache.get(cacheKey);
  if (!force && hit && (Date.now() - hit.ts) < 3 * 60 * 1000) return hit.url;
  const metaRes = await axios.get(`https://graph.facebook.com/v23.0/${mediaId}`, {
    headers: { Authorization: `Bearer ${token}` }, timeout: 20000
  });
  const url = metaRes.data.url;
  if (!url) throw new Error("URL de mídia não encontrada");
  mediaUrlCache.set(cacheKey, { url, ts: Date.now() });
  return url;
}

app.get("/media-proxy/:mediaId", async (req, res) => {
  const { account_id, download, filename } = req.query;
  const { mediaId } = req.params;

  // Busca token da conta
  let token = process.env.WHATSAPP_TOKEN;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("token").eq("id", account_id).maybeSingle();
    if (account?.token) token = account.token;
  }
  if (!token) return res.status(400).json({ error: "Token não encontrado" });

  const cacheKey = `${mediaId}_${token.substring(0, 20)}`;

  try {
    // STREAMING real: repassa o Range do navegador direto para o CDN da Meta e
    // encaminha os bytes conforme chegam. O vídeo/áudio começa a tocar imediatamente,
    // sem baixar o arquivo inteiro na memória (que causava demora e travadas).
    const fetchStream = async (force) => {
      const url = await getMediaUrl(mediaId, token, cacheKey, force);
      const headers = { Authorization: `Bearer ${token}`, "User-Agent": "WhatsApp/2.0" };
      if (req.headers.range && download !== "1") headers.Range = req.headers.range;
      return axios.get(url, {
        headers, responseType: "stream", timeout: 30000,
        validateStatus: s => s === 200 || s === 206,
      });
    };
    let up;
    try { up = await fetchStream(false); } catch (e) { up = await fetchStream(true); } // URL pode ter expirado

    res.status(up.status); // 200 ou 206 (parcial)
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Accept-Ranges", up.headers["accept-ranges"] || "bytes");
    res.setHeader("Content-Type", req.query.mime || up.headers["content-type"] || "application/octet-stream");
    if (up.headers["content-length"]) res.setHeader("Content-Length", up.headers["content-length"]);
    if (up.headers["content-range"])  res.setHeader("Content-Range", up.headers["content-range"]);

    if (download === "1") {
      const safeFilename = (filename ? decodeURIComponent(filename) : `midia_${mediaId.substring(0, 8)}`).replace(/["\r\n]/g, "");
      res.setHeader("Content-Disposition", `attachment; filename="${safeFilename}"`);
    } else {
      res.setHeader("Cache-Control", "public, max-age=600");
    }

    up.data.pipe(res);
    up.data.on("error", (e) => {
      console.error("❌ Stream de mídia interrompido:", e.message);
      try { res.destroy(); } catch (_) {}
    });
    // Se o navegador cancelar (fechou o vídeo, pulou trecho), corta o download da Meta
    res.on("close", () => { try { up.data.destroy(); } catch (_) {} });
  } catch (err) {
    console.error("❌ Erro ao baixar mídia:", err.response?.status || err.message);
    if (!res.headersSent) res.status(500).json({ error: "Falha ao baixar mídia" });
  }
});

// ── Lista todas as tags existentes (para sugestões e filtro) ──
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
  const term = raw.replace(/[,()]/g, " ").trim(); // evita quebrar a sintaxe do filtro
  const like = `%${term}%`;
  try {
    // 1. Telefones que têm alguma mensagem contendo o termo
    let mq = supabase.from("messages").select("phone").ilike("content", like).eq("owner", req.owner || ' ').limit(500);
    if (account_id) mq = mq.eq("account_id", account_id);
    const { data: msgRows } = await mq;
    const phones = [...new Set((msgRows || []).map(m => m.phone).filter(Boolean))];

    // 2. Contatos por nome/telefone OU entre os telefones encontrados
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

// ── Tarefas / lembretes por lead ──
app.get("/tasks", async (req, res) => {
  if (!supabase) return res.json([]);
  const { phone, pending } = req.query;
  let q = supabase.from("tasks").select("*").eq("owner", req.owner || ' ').order("due_at", { ascending: true, nullsFirst: false });
  if (phone) q = q.eq("phone", phone);
  if (pending === "1") q = q.eq("done", false);
  const { data, error } = await q;
  if (error) return res.status(500).json({ error: error.message });
  const tasks = data || [];
  // anexa o nome do lead (para a aba global de tarefas)
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

// ── Atualiza o nome do contato/lead ──
app.put("/contacts/:phone/name", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const name = (req.body.name || "").trim();
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const { error } = await supabase.from("contacts").update({ name }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Anotações por contato ──
app.get("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.json({ notes: "", email: "" });
  const { data, error } = await supabase
    .from("contacts").select("notes, email").eq("phone", req.params.phone).eq("owner", req.owner || ' ').maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ notes: data?.notes || "", email: data?.email || "" });
});

app.put("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { notes, email } = req.body;
  const upd = { notes: notes ?? "" };
  if (email !== undefined) upd.email = String(email || "").trim() || null;
  const { error } = await supabase
    .from("contacts")
    .update(upd)
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
      if (stage_id) obj.stage_id = stage_id; // só grava etapa quando escolhida (não apaga a de quem já existe)
      return obj;
    })
    .filter(c => c.phone.length >= 8);
  if (!toInsert.length) return res.status(400).json({ error: "Nenhum contato válido encontrado" });
  const { error } = await supabase.from("contacts").upsert(toInsert, { onConflict: "owner,phone" });
  if (error) return res.status(500).json({ error: error.message });
  console.log(`✅ ${toInsert.length} contatos importados`);
  res.json({ success: true, count: toInsert.length });
});

// ── Importar lead via n8n / planilha (mapeia ID da etapa → stage_id) ──
// Aceita 1 lead OU um array de leads. Campos flexíveis:
//   name | title | "Lead Titulo"   →  nome
//   phone | celular | "Celular"     →  telefone
//   id | "ID" | stage_external_id   →  ID da etapa (external_id de pipeline_stages)
//   account_id (opcional)           →  vincula a uma conta WhatsApp
app.post("/import/lead", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const items = Array.isArray(req.body) ? req.body
              : (Array.isArray(req.body.leads) ? req.body.leads : [req.body]);
  const n8nOwner = (String(req.query.owner||'').trim()) || (!Array.isArray(req.body) && req.body.owner) || 'elianecezaroliveira@gmail.com';
  const stageCache = {};
  let imported = 0;
  const errors = [];

  for (const it of (items || [])) {
    // remove um "=" no início (marcador de expressão do n8n que às vezes vaza como texto)
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
    // Não define last_message_* → o lead aparece só no Pipeline até iniciar conversa
    const { error: e } = await supabase.from("contacts").upsert(row, { onConflict: "owner,phone" });
    if (e) errors.push({ phone, error: e.message }); else imported++;
  }

  console.log(`📥 n8n importou ${imported} lead(s)` + (errors.length ? `, ${errors.length} erro(s)` : ""));
  res.json({ success: true, imported, errors });
});

// ── Alterar a ETAPA de um lead já existente (via n8n) ──
// Aceita 1 lead OU array. Identifica o lead pelo telefone e a etapa por:
//   stage | etapa | "Etapa" (nome, ex.: "S3")  |  id | "ID" (external_id)  |  stage_id (UUID)
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
    // Dispara bots com gatilho "entrou na etapa" — só quando a etapa realmente mudou
    if (prev?.stage_id !== stage_id) { try { await fireStageBots(phone, stage_id, n8nOwner); } catch(e) { console.error('fireStageBots (n8n):', e.message); } }
  }

  console.log(`🔁 n8n atualizou etapa de ${updated} lead(s)` + (errors.length ? `, ${errors.length} erro(s)` : ""));
  res.json({ success: true, updated, errors });
});

// ── Listar leads de uma etapa (para o n8n buscar e depois mover) ──
// GET /leads?id=98177799   ou   ?stage=SIAPE3   ou   ?stage_id=<uuid>
// Retorna um array de { phone, name, stage_id } — o n8n itera direto.
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

// ── Mensagens de um contato ──
app.get("/messages/:phone", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("messages").select("*").eq("phone", req.params.phone).eq("owner", req.owner || ' ')
    .order("timestamp", { ascending: true });
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
    console.error("❌ Erro ao listar templates:", err.response?.data || err.message);
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
    console.log("✅ Template criado:", name);
    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("❌ Erro ao criar template:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao criar template" });
  }
});

// ── Deletar template ──
// A Meta exige excluir por NOME na borda do WABA (não pelo ID do nó).
// O parâmetro :template_id agora recebe o NOME do template.
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
    if (hsm_id) params.hsm_id = hsm_id; // exclui o template específico (recomendado pela Meta)
    await axios.delete(`https://graph.facebook.com/v23.0/${account.waba_id}/message_templates`, { params });
    console.log("🗑️ Template excluído:", name);
    res.json({ success: true });
  } catch (err) {
    const metaErr = err.response?.data?.error;
    console.error("❌ Erro ao deletar template:", metaErr || err.message);
    // Devolve a mensagem detalhada da Meta (código/subcódigo) para diagnóstico
    const msg = metaErr
      ? `${metaErr.message || 'erro'}${metaErr.code ? ' (código ' + metaErr.code + (metaErr.error_subcode ? '/' + metaErr.error_subcode : '') + ')' : ''}`
      : (err.message || "Erro ao deletar template");
    res.status(500).json({ error: msg, detail: metaErr || null });
  }
});

// ── Enviar template ──
app.post("/send-template", async (req, res) => {
  let { to, account_id, template_name, language_code, components, body_text } = req.body;
  if (!to || !account_id || !template_name)
    return res.status(400).json({ error: "Campos obrigatórios: to, account_id, template_name" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito
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
    console.log("✅ Template enviado:", template_name, "→", to, "wamid:", tplWamid);
    res.json({ success: true, data: response.data });
  } catch (err) {
    const e = err.response?.data?.error || {};
    const msg = e.error_user_msg || e.message || err.message || "Erro ao enviar template";
    const detail = e.error_user_title || e.error_data?.details || "";
    console.error("❌ Erro ao enviar template:", err.response?.data || err.message);
    res.status(500).json({ error: msg, detail, code: e.code || null });
  }
});

// ── Pipeline / Kanban ──

// Listar estágios
app.get("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("pipeline_stages").select("*").eq("owner", req.owner || ' ').order("position", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// Criar estágio
app.post("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const { data, error } = await supabase
    .from("pipeline_stages").insert({ name, position: position || 0, owner: req.owner || null }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Renomear / reordenar estágio
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

// Excluir estágio (move leads para sem-status)
app.delete("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  await supabase.from("contacts").update({ stage_id: null }).eq("stage_id", req.params.id).eq("owner", req.owner || ' ');
  const { error } = await supabase.from("pipeline_stages").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Listar contatos (com stage_id, unread_count e prévia)
app.get("/contacts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { account_id, with_messages } = req.query;
  let query = supabase
    .from("contacts").select("phone, name, account_id, stage_id, tags, unread_count, first_unread_at, last_message_at, last_message_preview, last_message_direction, favorite")
    .eq("owner", req.owner || ' ')
    .order("last_message_at", { ascending: false });
  if (account_id) query = query.eq("account_id", account_id); // filtra pela conta quando informada
  // Lista de CONVERSAS: só contatos que já tiveram mensagem real (preview só é preenchido por mensagem,
  // nunca por importação — diferente de last_message_direction, que tem padrão 'inbound' no banco)
  if (with_messages) query = query.not("last_message_preview", "is", null);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// Mover lead para estágio
app.put("/contacts/:phone/stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { stage_id } = req.body;
  const { data: old } = await supabase.from("contacts").select("stage_id").eq("phone", req.params.phone).eq("owner", req.owner || ' ').maybeSingle();
  const { error } = await supabase
    .from("contacts").update({ stage_id: stage_id || null }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  // Dispara bots com gatilho de etapa — só quando a etapa realmente mudou
  if (stage_id && old?.stage_id !== stage_id) await fireStageBots(req.params.phone, stage_id, req.owner);
  res.json({ success: true });
});

// ── Bulk actions ──
app.put("/contacts/bulk-stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones, stage_id } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  // Guarda etapas anteriores para disparar bot só em quem mudou de verdade
  const { data: prevRows } = await supabase.from("contacts").select("phone, stage_id").in("phone", phones).eq("owner", req.owner || ' ');
  const prevMap = {}; for (const r of prevRows || []) prevMap[r.phone] = r.stage_id;
  const { error } = await supabase.from("contacts")
    .update({ stage_id: stage_id || null })
    .in("phone", phones).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  // Dispara bots com gatilho "entrou na etapa" para cada lead que realmente mudou
  if (stage_id) { for (const ph of phones) { if (prevMap[ph] !== stage_id) { try { await fireStageBots(ph, stage_id, req.owner); } catch(e) { console.error('fireStageBots (bulk):', e.message); } } } }
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
  // For each phone, merge new tags with existing
  for (const phone of phones) {
    const { data: contact } = await supabase.from("contacts").select("tags").eq("phone", phone).eq("owner", req.owner || ' ').maybeSingle();
    const merged = Array.from(new Set([...(contact?.tags || []), ...tags]));
    await supabase.from("contacts").update({ tags: merged }).eq("phone", phone).eq("owner", req.owner || ' ');
  }
  res.json({ success: true });
});

// ── Marcar conversa como lida ──
app.put("/contacts/:phone/read", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase
    .from("contacts").update({ unread_count: 0, first_unread_at: null }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Favoritar / desfavoritar conversa (swipe no celular)
app.put("/contacts/:phone/favorite", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const favorite = !!req.body?.favorite;
  const { error } = await supabase
    .from("contacts").update({ favorite }).eq("phone", req.params.phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, favorite });
});

// ═══════════════════════════════════════
// SISTEMA DE BOTS — motor de execução
// ═══════════════════════════════════════

// Substitui variáveis aceitando vários formatos: {nome} (nome) [nome] {{nome}}, maiúsc/minúsc
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
      await applyPendingStatus(wamid); // aplica status que chegou antes do insert
      const prev = text.length>80 ? text.substring(0,80)+'…' : text;
      await supabase.from('contacts').update({ last_message_at:ts, last_message_preview:prev, last_message_direction:'outbound', unread_count:0, first_unread_at:null }).eq('phone',phone).eq('owner',owner||' ');
    }
    return wamid;
  } catch(e) { console.error('❌ Bot sendMsg:', e.response?.data||e.message); return null; }
}

async function botGetAcct(accountId) {
  if (supabase && accountId) {
    const { data } = await supabase.from('accounts').select('phone_number_id,token,waba_id').eq('id', accountId).maybeSingle();
    if (data && data.phone_number_id) return data;
  }
  return { phone_number_id: process.env.PHONE_NUMBER_ID, token: process.env.WHATSAPP_TOKEN, waba_id: process.env.WABA_ID };
}

// Busca o corpo (BODY) de um modelo aprovado na Meta — com cache em memória
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

// Substitui {{1}}, {{2}}… pelos valores das variáveis (posicional)
function renderTemplateBody(bodyText, vars) {
  let txt = bodyText || '';
  (vars || []).forEach((val, i) => { txt = txt.split('{{' + (i + 1) + '}}').join(val); });
  return txt;
}

// Envia um MODELO aprovado pelo bot (com variáveis no corpo)
async function sendBotTemplate(phone, accountId, cfg, name, notes, owner) {
  const acct = await botGetAcct(accountId);
  if (!acct.phone_number_id || !acct.token) return null;
  // Busca o corpo do modelo para saber QUANTAS variáveis ele exige (evita erro 132000)
  let bodyText = null;
  try { bodyText = await getTemplateBodyText(acct.token, acct.waba_id, cfg.template_name, cfg.language || 'pt_BR'); } catch(_) {}
  const provided = (cfg.vars || []).map(v => applyVars(String(v || ''), name || phone, phone, notes));
  const needed = bodyText ? new Set(bodyText.match(/\{\{\d+\}\}/g) || []).size : provided.length;
  const vars = [];
  for (let i = 0; i < needed; i++) {
    const p = provided[i];
    vars.push(p && p.trim() ? p : (i === 0 ? (name || phone) : ' ')); // preenche o que faltar (1ª = nome)
  }
  const tmpl = { name: cfg.template_name, language: { code: cfg.language || 'pt_BR' } };
  if (vars.length) tmpl.components = [{ type: 'body', parameters: vars.map(t => ({ type: 'text', text: t })) }];
  try {
    const r = await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
      { messaging_product: 'whatsapp', to: phone, type: 'template', template: tmpl },
      { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } });
    if (supabase) {
      const ts = new Date().toISOString();
      // Monta o texto real do modelo (troca {{n}} pelas variáveis)
      let shown = bodyText ? renderTemplateBody(bodyText, vars) : `[Modelo: ${cfg.template_name}]`;
      const prev = shown.length > 80 ? shown.substring(0, 80) + '…' : shown;
      const tWamid = r.data?.messages?.[0]?.id || null;
      await supabase.from('messages').insert({ phone, content: shown, type: 'template', direction: 'outbound', timestamp: ts, account_id: accountId || null, status: 'sent', wamid: tWamid, owner: owner || null });
      await applyPendingStatus(tWamid);
      await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
    }
    return true;
  } catch(e) { console.error('❌ Bot template:', e.response?.data || e.message); return null; }
}

// Verifica horário comercial (UTC-3) e calcula a próxima abertura
function businessHoursState(nowMs, cfg) {
  const days = (cfg.days && cfg.days.length) ? cfg.days.map(Number) : [1,2,3,4,5]; // 0=Dom..6=Sáb
  const [sh, sm] = String(cfg.start || '08:00').split(':').map(Number);
  const [eh, em] = String(cfg.end   || '18:00').split(':').map(Number);
  const startMin = sh*60 + sm, endMin = eh*60 + em;
  const brt = new Date(nowMs - 3*3600000); // relógio de Brasília nos campos UTC
  const dow = brt.getUTCDay();
  const minNow = brt.getUTCHours()*60 + brt.getUTCMinutes();
  const isOpen = days.includes(dow) && minNow >= startMin && minNow < endMin;
  if (isOpen) return { open: true };
  for (let off = 0; off <= 7; off++) {
    const d = new Date(Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate() + off));
    if (!days.includes(d.getUTCDay())) continue;
    if (off === 0 && minNow >= startMin) continue; // hoje já passou da abertura
    const openBrtMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), sh, sm);
    return { open: false, nextOpenMs: openBrtMs + 3*3600000 }; // volta para UTC real
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
  if (!supabase || depth > 30) return; // prevent infinite loops
  const { id:runId, contact_phone:phone, account_id:acctId, current_node_id:nodeId, owner:botOwner } = run;
  const OW = botOwner || ' '; // sentinela p/ escopo por dono
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
      sendOk = text ? await sendBotMsg(phone, acctId, text, botOwner) : true; // sem texto = nada a enviar (não é falha)
    }
    // resolve as arestas deste nó (sucesso = sem rótulo / falha = __failed__)
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
      // Modelo com botões: aguarda o lead clicar num botão (ramifica conforme o botão)
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
      // Fora do expediente: aguarda até reabrir (permanece neste nó; o timer re-avalia)
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
  if (owner) rq = rq.eq('owner', owner); // só a run do dono certo (telefone pode repetir entre donos)
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

// Dispara todos os bots com gatilho "entrou na etapa" para um lead (do dono certo)
async function fireStageBots(phone, stageId, owner) {
  if (!supabase || !stageId || !phone) return;
  try {
    let bq = supabase.from('bots').select('*').eq('trigger_type','stage_enter').eq('trigger_stage_id',stageId).eq('active',true);
    if (owner) bq = bq.eq('owner', owner); // só os bots do dono do lead
    const { data: bots } = await bq;
    if (!bots || !bots.length) return;
    let cq = supabase.from('contacts').select('account_id').eq('phone',phone);
    if (owner) cq = cq.eq('owner', owner);
    const { data: ct } = await cq.maybeSingle();
    const leadAcct = ct?.account_id || null;
    for (const bot of bots) {
      console.log(`🤖 Gatilho de etapa: bot "${bot.name}" para ${phone}`);
      await startBot(bot.id, phone, bot.account_id || leadAcct, owner || bot.owner);
    }
  } catch(e) { console.error('fireStageBots error:', e.message); }
}

async function startBot(botId, phone, accountId, owner) {
  if (!supabase) return null;
  let ownerEmail = owner;
  if (!ownerEmail) { const { data:b } = await supabase.from('bots').select('owner').eq('id',botId).maybeSingle(); ownerEmail = b?.owner || null; }
  await supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() }).eq('contact_phone',phone).eq('bot_id',botId).in('status',['running','waiting_reply','paused']);
  const { data:startNodes } = await supabase.from('bot_nodes').select('id').eq('bot_id',botId).eq('type','start').limit(1);
  const startNode = startNodes && startNodes[0];
  if (!startNode) { console.error('❌ Bot sem nó start:', botId); return null; }
  const { data:run, error } = await supabase.from('bot_runs').insert({
    bot_id:botId, contact_phone:phone, account_id:accountId||null,
    current_node_id:startNode.id, status:'running', owner:ownerEmail||null,
    created_at:new Date().toISOString(), updated_at:new Date().toISOString()
  }).select().single();
  if (error) { console.error('❌ Bot run insert:', error.message); return null; }
  await processNode(run);
  return run;
}

// Timer: resume paused/timed-out runs every 5s
setInterval(async () => {
  if (!supabase) return;
  const now = new Date().toISOString();
  const { data:paused } = await supabase.from('bot_runs').select('*').in('status',['paused','waiting_reply']).lte('pause_until',now).not('pause_until','is',null);
  for (const run of paused||[]) {
    // Se o nó atual é "Horário comercial", re-avalia o próprio nó (não avança)
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
  // confirma que o bot é do dono antes de apagar os filhos
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
  // só devolve o fluxo se o bot for do dono
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
// Duplicar um bot (copia config, nós e arestas com novos ids)
app.post('/bots/:id/duplicate', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const srcId = req.params.id;
  const { data: bot, error: be } = await supabase.from('bots').select('*').eq('id', srcId).eq('owner', req.owner || ' ').single();
  if (be || !bot) return res.status(404).json({error:'Bot não encontrado'});
  // novo bot: começa MANUAL e INATIVO para não disparar sem querer
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
  // confirma que o bot é do dono
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
// SETTINGS + INTEGRAÇÃO N8N
// ═══════════════════════════════════════

// Cache de settings (evita consulta ao DB em cada mensagem)
let _settings = {};
async function loadSettings() {
  if (!supabase) return;
  try {
    const { data } = await supabase.from('settings').select('key, value');
    for (const row of data || []) _settings[row.key] = row.value;
    console.log('✅ Settings carregados:', Object.keys(_settings).join(', ') || '(nenhum)');
  } catch(e) { console.error('Settings load error:', e.message); }
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

// ═══════════════════════════════════════
// NOTIFICAÇÕES PUSH (Web Push / PWA)
// ═══════════════════════════════════════
let webpush = null;
try { webpush = require('web-push'); } catch (e) { console.log('⚠️ web-push não instalado — notificações push desativadas'); }

let _vapid = null;
async function initPush() {
  if (!webpush || !supabase) return;
  try {
    const { data } = await supabase.from('settings').select('value').eq('key', 'vapid_keys').maybeSingle();
    if (data?.value) {
      _vapid = JSON.parse(data.value);
    } else {
      // Gera o par de chaves UMA vez e persiste (trocar as chaves invalida as inscrições)
      const crypto = require('crypto');
      const { publicKey, privateKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
      _vapid = {
        publicKey: publicKey.export({ type: 'spki', format: 'der' }).subarray(-65).toString('base64url'),
        privateKey: privateKey.export({ format: 'jwk' }).d,
      };
      await supabase.from('settings').upsert({ key: 'vapid_keys', value: JSON.stringify(_vapid), updated_at: new Date().toISOString() });
      console.log('🔑 Chaves VAPID geradas e salvas nos settings');
    }
    webpush.setVapidDetails('mailto:solucoesvalorize@gmail.com', _vapid.publicKey, _vapid.privateKey);
    console.log('✅ Web Push pronto');
  } catch (e) { console.error('Push init error:', e.message); }
}
initPush();

app.get('/push/public-key', (req, res) => res.json({ key: _vapid ? _vapid.publicKey : null }));

app.post('/push/subscribe', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const sub = req.body?.subscription;
  if (!sub?.endpoint) return res.status(400).json({ error: 'subscription inválida' });
  const { error } = await supabase.from('push_subscriptions').upsert(
    { endpoint: sub.endpoint, subscription: sub, owner: req.owner || null, updated_at: new Date().toISOString() },
    { onConflict: 'endpoint' }
  );
  if (error) { console.error('Push subscribe error:', error.message); return res.status(500).json({ error: error.message }); }
  res.json({ success: true });
});

app.post('/push/unsubscribe', async (req, res) => {
  if (supabase && req.body?.endpoint) await supabase.from('push_subscriptions').delete().eq('endpoint', req.body.endpoint);
  res.json({ success: true });
});

// Teste de ponta a ponta: envia uma notificação real e devolve o diagnóstico
app.post('/push/test', async (req, res) => {
  if (!webpush || !_vapid) return res.status(500).json({ error: 'web-push não está ativo no servidor' });
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  try {
    // Total geral (para diferenciar "tabela vazia" de "owner diferente")
    const { count: totalAll, error: tblErr } = await supabase
      .from('push_subscriptions').select('endpoint', { count: 'exact', head: true });
    if (tblErr) return res.status(500).json({ error: 'Tabela push_subscriptions: ' + tblErr.message });

    let q = supabase.from('push_subscriptions').select('endpoint, subscription');
    q = req.owner ? q.eq('owner', req.owner) : q.is('owner', null);
    const { data: subs } = await q;

    const results = [];
    for (const s of subs || []) {
      try {
        await webpush.sendNotification(s.subscription,
          JSON.stringify({ title: 'VETRA', body: '🔔 Notificações funcionando!', tag: 'push-test' }), { TTL: 300 });
        results.push({ ok: true });
      } catch (e) {
        results.push({ ok: false, status: e.statusCode || null, msg: String(e.body || e.message || '').substring(0, 150) });
        if (e.statusCode === 404 || e.statusCode === 410) {
          await supabase.from('push_subscriptions').delete().eq('endpoint', s.endpoint);
        }
      }
    }
    res.json({ owner: req.owner || null, minhas_inscricoes: (subs || []).length, total_geral: totalAll || 0, results });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Envia push para todos os aparelhos do dono; remove inscrições mortas (404/410)
async function sendPushToOwner(owner, payload) {
  if (!webpush || !_vapid || !supabase) return;
  try {
    // Total de conversas não lidas → número (badge) no ícone do app
    try {
      let bq = supabase.from('contacts').select('unread_count').gt('unread_count', 0);
      bq = owner ? bq.eq('owner', owner) : bq.is('owner', null);
      const { data: rows } = await bq;
      payload.badge = (rows || []).length;
    } catch (_) {}

    let q = supabase.from('push_subscriptions').select('endpoint, subscription');
    q = owner ? q.eq('owner', owner) : q.is('owner', null);
    const { data: subs } = await q;
    for (const s of subs || []) {
      try {
        await webpush.sendNotification(s.subscription, JSON.stringify(payload), { TTL: 3600 });
      } catch (e) {
        if (e.statusCode === 404 || e.statusCode === 410) {
          await supabase.from('push_subscriptions').delete().eq('endpoint', s.endpoint);
          console.log('🧹 Inscrição push expirada removida');
        } else {
          console.error('Push send error:', e.statusCode || e.message);
        }
      }
    }
  } catch (e) { console.error('Push error:', e.message); }
}

// Teste de conexão N8N — envia evento de teste para o webhook configurado
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
// EVOLUTION API — Conexão via QR Code
// ═══════════════════════════════════════

const EVOLUTION_URL = (process.env.EVOLUTION_API_URL || 'https://evolution-api-production-ac49c.up.railway.app').replace(/\/$/, '');
const EVOLUTION_KEY = process.env.EVOLUTION_API_KEY || 'meucrm2024';
const BACKEND_URL   = process.env.BACKEND_PUBLIC_URL || 'https://meucrm-backend-production-d4f4.up.railway.app';

const evoHdr = () => ({ apikey: EVOLUTION_KEY, 'Content-Type': 'application/json' });

// Cache de QR Code por instância — preenchido pelo webhook QRCODE_UPDATED (QR assíncrono)
const qrCache = {};

// Envia mensagem via Evolution API
async function sendViaEvolution(instanceName, to, text) {
  // Evolution API v2: body usa "text" direto
  const r = await axios.post(`${EVOLUTION_URL}/message/sendText/${instanceName}`, {
    number: to,
    text,
    options: { delay: 1000 }
  }, { headers: evoHdr(), timeout: 15000 });
  return r.data;
}

// POST /evolution/connect — limpa instâncias antigas, cria nova e retorna QR
app.post('/evolution/connect', async (req, res) => {
  const instanceName = `meucrm_${Date.now()}`;
  try {
    // 1. Limpa instâncias antigas desconectadas (evita acúmulo)
    try {
      const { data: list } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
      for (const inst of list || []) {
        const name = inst.instance?.instanceName || inst.instanceName || inst.name;
        const status = inst.instance?.connectionStatus || inst.connectionStatus;
        if (name && name.startsWith('meucrm_') && status !== 'open') {
          await axios.delete(`${EVOLUTION_URL}/instance/delete/${name}`, { headers: evoHdr(), timeout: 8000 }).catch(() => {});
          console.log('🗑️ Instância antiga removida:', name);
        }
      }
    } catch(cleanErr) { console.warn('Cleanup warn:', cleanErr.message); }

    // 2. Cria nova instância
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

    console.log('Evolution create raw keys:', Object.keys(data || {}));

    // QR pode vir imediatamente na resposta de criação
    let qr = data?.qrcode?.base64 || data?.base64 || null;

    // Se não veio, faz APENAS algumas tentativas rápidas (não trava a requisição).
    // O QR também chega de forma assíncrona via webhook (qrCache) e pelo polling do frontend.
    if (!qr) {
      console.log(`⏳ QR não veio na criação, tentando rápido via /instance/connect...`);
      for (let i = 0; i < 3; i++) {
        await new Promise(r => setTimeout(r, 2000)); // 3 tentativas x 2s = 6s no máximo
        try {
          const { data: qrData } = await axios.get(
            `${EVOLUTION_URL}/instance/connect/${instanceName}`,
            { headers: evoHdr(), timeout: 8000 }
          );
          console.log(`QR attempt ${i+1}:`, JSON.stringify(qrData).substring(0, 200));
          qr = qrData?.base64 || qrData?.qrcode?.base64 || null;
          if (qr) { console.log(`✅ QR obtido na tentativa ${i+1}`); break; }
        } catch(qrErr) {
          console.warn(`QR attempt ${i+1} error:`, qrErr.response?.status, qrErr.message);
        }
      }
    }

    // Retorna já — o frontend continua buscando o QR em /evolution/qr (cache do webhook + connect)
    console.log(`Instância criada: ${instanceName}, QR: ${qr ? 'SIM' : 'NAO (frontend faz polling)'}`);
    res.json({ success: true, instance: instanceName, qr });
  } catch(e) {
    console.error('Evolution create error:', e.response?.data || e.message);
    res.status(500).json({ error: e.response?.data?.message || e.message });
  }
});

// GET /evolution/qr/:instance — QR code (Evolution API v2)
app.get('/evolution/qr/:instance', async (req, res) => {
  // 0. Se o webhook já entregou o QR, serve do cache (mais rápido e confiável)
  if (qrCache[req.params.instance]) {
    return res.json({ qr: qrCache[req.params.instance], code: null, pairingCode: null, raw: { cached: true } });
  }
  try {
    // Evolution API v2: o QR vem de GET /instance/connect/:instance
    // Resposta: { pairingCode, code, base64, count }
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/connect/${req.params.instance}`, {
      headers: evoHdr(), timeout: 10000
    });
    console.log('Evolution QR v2 raw:', JSON.stringify(data).substring(0, 400));
    const qr = data?.base64 || data?.qrcode?.base64 || null;
    const code = data?.code || data?.qrcode?.code || null;
    res.json({ qr, code, pairingCode: data?.pairingCode || null, raw: data });
  } catch(e) {
    console.error('Evolution QR error:', e.response?.data || e.message);
    // Fallback: endpoint legado /instance/qrcode
    try {
      const { data: d2 } = await axios.get(`${EVOLUTION_URL}/instance/qrcode/${req.params.instance}`, { headers: evoHdr(), timeout: 8000, params: { image: true } });
      const qr = d2?.base64 || d2?.qrcode?.base64 || null;
      const code = d2?.code || d2?.qrcode?.code || null;
      return res.json({ qr, code, raw: d2 });
    } catch(e2) {}
    res.status(500).json({ error: e.message, qr: null, raw: e.response?.data });
  }
});

// GET /evolution/debug — mostra info bruta da Evolution API
app.get('/evolution/debug', async (req, res) => {
  try {
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
    res.json({ instances: data, url: EVOLUTION_URL });
  } catch(e) {
    res.status(500).json({ error: e.message, url: EVOLUTION_URL, detail: e.response?.data });
  }
});

// GET /evolution/status/:instance — verifica estado (Evolution API v2)
app.get('/evolution/status/:instance', async (req, res) => {
  try {
    const { data } = await axios.get(`${EVOLUTION_URL}/instance/connectionState/${req.params.instance}`, { headers: evoHdr(), timeout: 10000 });
    console.log('Evolution status raw:', JSON.stringify(data).substring(0, 200));
    // v2: { instance: { instanceName, state } } ou { state }
    const state = data?.instance?.state || data?.state || 'close';
    let phone = null;
    if (state === 'open') {
      try {
        const { data: list } = await axios.get(`${EVOLUTION_URL}/instance/fetchInstances`, { headers: evoHdr(), timeout: 10000 });
        const inst = (list || []).find(i => (i.instance?.instanceName || i.instanceName || i.name) === req.params.instance);
        const ownerJid = inst?.instance?.ownerJid || inst?.ownerJid || '';
        if (ownerJid) phone = ownerJid.replace('@s.whatsapp.net', '').replace(/\D/g, '') || null;
      } catch(e2) { console.warn('Fetch instances err:', e2.message); }
    }
    res.json({ state, phone });
  } catch(e) {
    res.status(500).json({ error: e.message, state: 'close' });
  }
});

// POST /evolution/save-account — salva conta Evolution no Supabase após conexão
app.post('/evolution/save-account', async (req, res) => {
  const { instance, phone } = req.body;
  if (!instance) return res.status(400).json({ error: 'instance obrigatório' });
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const name = phone ? `WhatsApp ${phone}` : `WhatsApp QR (${instance})`;
  const { data, error } = await supabase.from('accounts')
    .upsert({ name, type: 'evolution', evolution_instance: instance, phone_display: phone || null, phone_number_id: instance, token: '' }, { onConflict: 'phone_number_id' })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  console.log('✅ Conta Evolution salva:', name);
  res.json({ success: true, data });
});

// DELETE /evolution/disconnect/:instance
app.delete('/evolution/disconnect/:instance', async (req, res) => {
  try {
    await axios.delete(`${EVOLUTION_URL}/instance/delete/${req.params.instance}`, { headers: evoHdr(), timeout: 10000 });
    if (supabase) await supabase.from('accounts').delete().eq('evolution_instance', req.params.instance);
    res.json({ success: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /evolution-webhook — recebe mensagens da Evolution API
app.post('/evolution-webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const { event, instance: instanceName, data } = req.body;
    console.log('📩 Evolution webhook:', event, instanceName);

    if (event === 'messages.upsert') {
      if (!data) return;
      const fromMe    = !!data.key?.fromMe;        // true = mensagem enviada pelo celular/CRM
      const remoteJid = data.key?.remoteJid || '';
      if (remoteJid.includes('@g.us')) return; // ignora grupos

      let phone       = remoteJid.replace('@s.whatsapp.net', '');
      const name      = data.pushName || phone;
      const timestamp = new Date((data.messageTimestamp || Date.now() / 1000) * 1000).toISOString();
      const wamid     = data.key?.id || null;
      const direction = fromMe ? 'outbound' : 'inbound';

      // Extrai conteúdo
      let content = fromMe ? '[Mensagem enviada]' : '[Mensagem recebida]', type = 'text';
      const msg = data.message || {};
      if      (msg.conversation)          { content = msg.conversation; type = 'text'; }
      else if (msg.extendedTextMessage)   { content = msg.extendedTextMessage.text || ''; type = 'text'; }
      else if (msg.imageMessage)          { content = msg.imageMessage.caption || '[Imagem]'; type = 'image'; }
      else if (msg.audioMessage || msg.pttMessage) { content = '[Áudio]'; type = 'audio'; }
      else if (msg.videoMessage)          { content = msg.videoMessage.caption || '[Vídeo]'; type = 'video'; }
      else if (msg.documentMessage)       { content = msg.documentMessage.fileName || '[Documento]'; type = 'document'; }

      // Busca account_id + dono (owner) — sem o owner a mensagem não aparece no CRM
      let accountId = null;
      let ownerEmail = null;
      if (supabase && instanceName) {
        const { data: acc } = await supabase.from('accounts').select('id, owner').eq('evolution_instance', instanceName).maybeSingle();
        if (acc) { accountId = acc.id; ownerEmail = acc.owner || null; }
      }

      // Unifica a conversa se o contato já existe com/sem o nono dígito
      phone = await resolveExistingPhone(phone, ownerEmail);

      if (supabase) {
        // Dedup: evita duplicar mensagens já salvas (ex.: enviadas pelo próprio CRM)
        if (wamid) {
          const { data: exists } = await supabase.from('messages').select('id').eq('wamid', wamid).maybeSingle();
          if (exists) return;
        }

        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = { phone, name, last_message_at: timestamp, last_message_preview: preview, last_message_direction: direction };
        if (accountId) contactData.account_id = accountId;
        if (ownerEmail) contactData.owner = ownerEmail;
        const { error: cErr } = await supabase.from('contacts').upsert(contactData, { onConflict: 'owner,phone' });
        if (cErr) console.error('❌ Evolution: erro ao salvar contato:', cErr.message);

        // Incrementa não-lidos só para mensagens RECEBIDAS
        if (!fromMe) {
          const { data: cRow } = await supabase.from('contacts').select('unread_count, first_unread_at').eq('phone', phone).eq('owner', ownerEmail || ' ').maybeSingle();
          const currentUnread = cRow?.unread_count || 0;
          const unreadUpdate = { unread_count: currentUnread + 1 };
          if (currentUnread === 0) unreadUpdate.first_unread_at = timestamp;
          await supabase.from('contacts').update(unreadUpdate).eq('phone', phone).eq('owner', ownerEmail || ' ');
        }

        const msgData = { phone, content, type, direction, timestamp, wamid };
        if (accountId) msgData.account_id = accountId;
        if (ownerEmail) msgData.owner = ownerEmail;
        const { error: mErr } = await supabase.from('messages').insert(msgData);
        if (mErr) console.error('❌ Evolution: erro ao salvar mensagem:', mErr.message);

        // Notificação push só para mensagens RECEBIDAS
        if (!fromMe) sendPushToOwner(ownerEmail, { title: name || phone, body: preview, phone, tag: 'chat-' + phone }).catch(() => {});

        // Bot e n8n só para mensagens RECEBIDAS
        if (!fromMe && type === 'text' && content) {
          try { await handleBotReply(phone, content, ownerEmail); } catch(be) { console.error('Bot reply error:', be.message); }
        }
        if (!fromMe) {
          const n8nUrl = _settings['n8n_webhook_url'];
          if (n8nUrl) {
            try { await axios.post(n8nUrl, { event: 'message_received', phone, name, content, type, timestamp, account_id: accountId || null }, { timeout: 8000 }); } catch(ne) {}
          }
        }
      }
    } else if (event === 'qrcode.updated') {
      // Evolution gera o QR de forma assíncrona e o entrega aqui
      const b64 = data?.qrcode?.base64 || data?.base64 || null;
      if (b64) {
        qrCache[instanceName] = b64.startsWith('data:') ? b64 : 'data:image/png;base64,' + b64;
        console.log(`📲 QR cacheado para ${instanceName}`);
      }
    } else if (event === 'connection.update') {
      console.log(`🔌 Evolution ${instanceName}: ${data?.state}`);
      // Ao conectar (ou desconectar), o QR antigo não serve mais
      if (data?.state === 'open' || data?.state === 'close') delete qrCache[instanceName];
    }
  } catch(err) {
    console.error('Evolution webhook error:', err.message);
  }
});

app.listen(PORT, () => console.log(`🚀 MeuCRM na porta ${PORT}`));
