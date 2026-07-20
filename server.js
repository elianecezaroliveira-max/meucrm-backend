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
const SUPABASE_ANON = process.env.SUPABASE_PUBLISHABLE_KEY || 'sb_publishable_xoU54iyT3KyxNR6i7fh3aw_1qpEKpua';
const _tokenOwner = {}; // cache token -> { email, ts }
async function resolveOwner(req) {
  const a = req.headers.authorization || '';
  const tok = a.startsWith('Bearer ') ? a.slice(7) : null;
  if (!tok || !SUPABASE_URL) return null;
  const c = _tokenOwner[tok];
  if (c && Date.now() - c.ts < 300000) return c.email;
  try {
    const r = await axios.get(`${SUPABASE_URL}/auth/v1/user`, { headers: { Authorization: 'Bearer ' + tok, apikey: SUPABASE_ANON } });
    let email = (r.data?.email || '').toLowerCase() || null;
    // Cofre compartilhado: todo e-mail autenticado enxerga o cofre principal.
    // 1) 'owner_aliases' mapeia e-mails específicos; 2) 'owner_default' vale para
    // todos os demais — assim novos membros só precisam entrar na lista de login.
    if (email) {
      try {
        const aliases = JSON.parse(_settings['owner_aliases'] || '{}');
        if (aliases[email]) email = String(aliases[email]).toLowerCase();
        else if (_settings['owner_default']) email = String(_settings['owner_default']).toLowerCase();
      } catch (_) {}
      _tokenOwner[tok] = { email, ts: Date.now() };
    }
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
  131030: 'Número do destinatário NÃO está na lista de permitidos. Este número da API oficial ainda está em MODO DE TESTE na Meta — nesse modo só é possível enviar para destinatários cadastrados. Para enviar a qualquer número: conclua a verificação do negócio e coloque o app em modo PRODUÇÃO (Live) no Meta for Developers. Para testar agora: adicione o número do destinatário na lista de teste (WhatsApp > API Setup > destinatários).',
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

// Escada de status: nunca REBAIXAR (os webhooks da Meta podem chegar fora de
// ordem — um "sent" atrasado não pode apagar o ✓✓ de um "delivered" já aplicado)
const _ST_RANK = { pending: 0, sent: 1, delivered: 2, read: 3 };
async function updateMsgStatus(wamid, upd) {
  if (!supabase || !wamid || !upd?.status) return;
  if (upd.status === 'failed') { await supabase.from('messages').update(upd).eq('wamid', wamid); return; }
  const rank = _ST_RANK[upd.status];
  if (rank === undefined) return;
  const lower = Object.keys(_ST_RANK).filter(s => _ST_RANK[s] < rank);
  await supabase.from('messages').update(upd).eq('wamid', wamid)
    .or('status.is.null,status.in.(' + lower.join(',') + ')');
}

// Buffer de status que chegam ANTES da mensagem ser salva (corrige ✓ que não vira ✓✓)
const _pendingStatuses = {}; // wamid -> { status, error_info, ts }
function _cachePendingStatus(wamid, upd) {
  if (!wamid) return;
  const prev = _pendingStatuses[wamid];
  // Mantém o status mais avançado já guardado (não rebaixa)
  if (prev && upd.status !== 'failed' && (_ST_RANK[prev.status] ?? -1) > (_ST_RANK[upd.status] ?? -1)) {
    prev.ts = Date.now();
    return;
  }
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
  await updateMsgStatus(wamid, u);
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
            await updateMsgStatus(wamid, upd);
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
      let accountToken = process.env.WHATSAPP_TOKEN || null;
      if (supabase && phoneNumberId) {
        const { data: account, error: accErr } = await supabase
          .from("accounts").select("id, owner, token").eq("phone_number_id", phoneNumberId).maybeSingle();
        if (accErr) console.error("❌ Erro ao buscar conta:", accErr.message);
        if (account) {
          accountId = account.id;
          ownerEmail = account.owner || null;
          if (account.token) accountToken = account.token;
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
        const durS = await getAudioDurationSecs(mediaId, accountToken);
        content = "🎤 Mensagem de voz" + (durS ? ` (${_fmtDur(durS)})` : "");
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
          .from("contacts").select("name, unread_count, first_unread_at, account_id").eq("phone", from).eq("owner", ownerEmail || ' ').maybeSingle();

        // Salva contato com prévia da última mensagem
        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = {
          phone: from, last_message_at: timestamp,
          last_message_preview: preview,
          last_message_direction: 'inbound',
        };
        if (!existing) contactData.name = name; // só define o nome do WhatsApp na CRIAÇÃO; depois respeita o editado
        // NÚMERO da conversa: mantém o número do ÚLTIMO ENVIO seu. Mensagem recebida
        // NÃO troca o número; só define quando o contato é novo ou ainda não tem número.
        if (accountId && (!existing || existing.account_id == null)) contactData.account_id = accountId;
        if (ownerEmail) contactData.owner = ownerEmail; // dono = dono da conta de WhatsApp

        const { error: contactErr } = await supabase
          .from("contacts")
          .upsert(contactData, { onConflict: "owner,phone" });

        if (contactErr) {
          console.error("❌ Erro ao salvar contato:", contactErr.message, contactErr.details);
        } else {
          console.log("✅ Contato salvo:", from);
        }

        // Foto de perfil via motor QR (serve também para contatos da API oficial)
        // Prefere uma instância QR do MESMO dono (privacidade da foto)
        const avatarInst = (await anyOpenWaInstanceForOwner(ownerEmail).catch(() => null)) || anyOpenWaInstance();
        if (avatarInst) waFetchAvatar(avatarInst, from, ownerEmail).catch(() => {});

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
        // IA: regra de "contato errado" primeiro; se não tratou, tenta o FAQ
        if (['text','button','interactive'].includes(type) && content) {
          try {
            const wp = await handleWrongPerson(from, content, ownerEmail, accountId);
            if (!wp) await handleFaqAutoReply(from, content, ownerEmail, accountId);
          } catch(fe) { console.error('IA auto-reply error:', fe.message); }
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
  // Status de conexão: QR = tempo real (estado do motor embutido); API = checagem com cache
  const out = await Promise.all((data || []).map(async acc => {
    let status = 'unknown';
    if (acc.evolution_instance) {
      const st = _waState[acc.evolution_instance];
      status = st === 'open' ? 'connected' : (st === 'connecting' ? 'connecting' : 'disconnected');
    } else if (acc.phone_number_id) {
      status = await cloudApiStatus(acc.id);
    }
    return { ...acc, status };
  }));
  res.json(out);
});

// Status da conta da API oficial (checa o token na Meta, com cache de 5 min
// para não gastar recursos do Railway a cada carregamento)
const _acctStatusCache = {};
async function cloudApiStatus(accId) {
  const c = _acctStatusCache[accId];
  if (c && Date.now() - c.ts < 5 * 60000) return c.status;
  let status = 'disconnected';
  try {
    const { data: a } = await supabase.from('accounts').select('phone_number_id, token').eq('id', accId).maybeSingle();
    if (a?.phone_number_id && a?.token) {
      const r = await axios.get(`https://graph.facebook.com/v23.0/${a.phone_number_id}?fields=id`,
        { params: { access_token: a.token }, timeout: 6000 });
      if (r.data?.id) status = 'connected';
    }
  } catch (_) { status = 'disconnected'; }
  _acctStatusCache[accId] = { status, ts: Date.now() };
  return status;
}

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

// ── Renomear conta (API oficial ou QR Code) ──
app.patch("/accounts/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const name = String(req.body?.name || '').trim();
  if (!name) return res.status(400).json({ error: "Informe o novo nome" });
  if (name.length > 40) return res.status(400).json({ error: "Nome muito longo (máx. 40 caracteres)" });
  const { data, error } = await supabase.from("accounts")
    .update({ name }).eq("id", req.params.id).eq("owner", req.owner || ' ')
    .select("id, name").maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  if (!data) return res.status(404).json({ error: "Conta não encontrada" });
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
        status: 'pending', wamid, owner: req.owner || null,
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

  // Conta QR (motor embutido): reage direto pelo WhatsApp pareado
  if (WA_EMBEDDED && supabase && account_id) {
    const { data: accQ } = await supabase.from('accounts')
      .select('type, evolution_instance').eq('id', account_id).maybeSingle();
    if (accQ?.type === 'evolution' && accQ.evolution_instance) {
      try {
        const sock = _waSocks[accQ.evolution_instance];
        if (!sock || _waState[accQ.evolution_instance] !== 'open')
          return res.status(400).json({ error: 'WhatsApp QR desconectado — gere o QR novamente em Contas' });
        const { data: msgRow } = await supabase.from('messages').select('direction').eq('wamid', wamid).maybeSingle();
        const jid = await waResolveJid(sock, to);
        await sock.sendMessage(jid, {
          react: { text: emoji || '', key: { remoteJid: jid, fromMe: msgRow?.direction === 'outbound', id: wamid } },
        });
        await supabase.from('messages').update({ reaction: emoji || null, reaction_by: 'me' }).eq('wamid', wamid);
        return res.json({ success: true, via: 'qr' });
      } catch (e) {
        console.error('❌ Reação via QR:', e.message);
        return res.status(500).json({ error: 'Falha ao reagir pelo WhatsApp QR: ' + e.message });
      }
    }
  }

  const acct = await botGetAcct(account_id, req.owner);
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
      // Receita EXATA das mensagens de voz nativas do WhatsApp: 16 kHz, ~16 kbps, mono.
      // Em 48 kHz/32 kbps o acelerador (1,5x/2x) do WhatsApp distorcia a reprodução.
      .audioCodec('libopus').audioBitrate('16k').audioChannels(1).audioFrequency(16000)
      // Normaliza o volume da fala (gravamos o microfone cru, que fica baixo)
      .audioFilters('loudnorm=I=-16:TP=-1.5:LRA=11')
      .outputOptions(['-application', 'voip', '-vbr', 'on'])
      .format('ogg')
      .on('end', () => {
        try { const out = fs.readFileSync(outFile); cleanup(); resolve(out); }
        catch (e) { cleanup(); reject(e); }
      })
      .on('error', err => { cleanup(); reject(err); })
      .save(outFile);
  });
}

// Formata segundos como M:SS (para "🎤 Mensagem de voz (0:07)")
function _fmtDur(s) { s = Math.max(0, Math.round(s || 0)); return Math.floor(s / 60) + ":" + String(s % 60).padStart(2, "0"); }

// Mede a duração de um áudio recebido (a Meta não envia a duração no webhook)
async function getAudioDurationSecs(mediaId, token) {
  if (!_ffmpeg || !mediaId || !token) return null;
  const os = require("os"), fs = require("fs"), path = require("path");
  const f = path.join(os.tmpdir(), "dur_" + Date.now() + "_" + Math.random().toString(36).slice(2));
  try {
    const metaRes = await axios.get(`https://graph.facebook.com/v23.0/${mediaId}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 15000 });
    if (!metaRes.data?.url) return null;
    const media = await axios.get(metaRes.data.url, {
      headers: { Authorization: `Bearer ${token}`, "User-Agent": "WhatsApp/2.0" },
      responseType: "arraybuffer", timeout: 20000, maxContentLength: 20 * 1024 * 1024 });
    fs.writeFileSync(f, Buffer.from(media.data));
    const secs = await new Promise(resolve => {
      const { execFile } = require("child_process");
      execFile(require("@ffmpeg-installer/ffmpeg").path, ["-i", f], (err, so, se) => {
        const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(String(se || ""));
        resolve(m ? (+m[1]) * 3600 + (+m[2]) * 60 + parseFloat(m[3]) : null);
      });
    });
    return secs;
  } catch (e) { return null; }
  finally { try { fs.unlinkSync(f); } catch (_) {} }
}

// Calcula os "pauzinhos" da mensagem de voz: envelope de volume em 64 barras (0-99)
async function computeWaveform(audioBuf) {
  if (!_ffmpeg) return null;
  const os = require('os'), fs = require('fs'), path = require('path');
  const inFile = path.join(os.tmpdir(), 'wf_' + Date.now() + '_' + Math.random().toString(36).slice(2));
  const outFile = inFile + '.raw';
  try {
    fs.writeFileSync(inFile, audioBuf);
    await new Promise((resolve, reject) => {
      _ffmpeg(inFile).audioChannels(1).audioFrequency(8000).format('s16le')
        .on('end', resolve).on('error', reject).save(outFile);
    });
    const raw = fs.readFileSync(outFile);
    const samples = new Int16Array(raw.buffer, raw.byteOffset, Math.floor(raw.length / 2));
    if (!samples.length) return null;
    const bars = 64, per = Math.max(1, Math.floor(samples.length / bars));
    const amps = []; let maxAmp = 1;
    for (let i = 0; i < bars; i++) {
      let sum = 0, n = 0;
      for (let j = i * per; j < Math.min((i + 1) * per, samples.length); j++) { sum += Math.abs(samples[j]); n++; }
      const a = n ? sum / n : 0;
      amps.push(a); if (a > maxAmp) maxAmp = a;
    }
    const wf = new Uint8Array(bars);
    for (let i = 0; i < bars; i++) wf[i] = Math.min(99, Math.round((amps[i] / maxAmp) * 99));
    return wf;
  } catch (e) { return null; }
  finally { try { fs.unlinkSync(inFile); } catch (_) {} try { fs.unlinkSync(outFile); } catch (_) {} }
}

// Converte vídeo para MP4 (o iPhone grava .mov, que o WhatsApp via QR não aceita).
// 1ª tentativa: remux (-c copy, instantâneo); se falhar, recodifica de verdade.
function convertVideoToMp4(buf) {
  return new Promise((resolve, reject) => {
    if (!_ffmpeg) return reject(new Error('ffmpeg indisponível'));
    const os = require('os'), fs = require('fs'), path = require('path');
    const inFile = path.join(os.tmpdir(), 'vid_' + Date.now() + '_' + Math.random().toString(36).slice(2));
    const outFile = inFile + '.mp4';
    const cleanup = () => { try { fs.unlinkSync(inFile); } catch (_) {} try { fs.unlinkSync(outFile); } catch (_) {} };
    const finish = () => { try { const out = fs.readFileSync(outFile); cleanup(); resolve(out); } catch (e) { cleanup(); reject(e); } };
    fs.writeFileSync(inFile, buf);
    _ffmpeg(inFile)
      .outputOptions(['-c', 'copy', '-movflags', '+faststart']).format('mp4')
      .on('end', finish)
      .on('error', () => {
        _ffmpeg(inFile)
          .videoCodec('libx264').audioCodec('aac')
          .outputOptions(['-preset', 'veryfast', '-crf', '28', '-movflags', '+faststart'])
          .format('mp4')
          .on('end', finish)
          .on('error', err2 => { cleanup(); reject(err2); })
          .save(outFile);
      })
      .save(outFile);
  });
}

// ── Enviar mídia (imagem, PDF, vídeo, etc.) ──
app.post("/send-media", async (req, res) => {
  let { to, account_id, fileBase64, fileName, mimeType } = req.body;
  if (!to || !fileBase64 || !fileName || !mimeType)
    return res.status(400).json({ error: "Informe to, fileBase64, fileName e mimeType" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito

  let phoneNumberId, token, accountType = 'cloudapi', evolutionInstance = null;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("phone_number_id, token, type, evolution_instance").eq("id", account_id).single();
    if (account) {
      phoneNumberId = account.phone_number_id; token = account.token;
      accountType = account.type || 'cloudapi'; evolutionInstance = account.evolution_instance || null;
    }
  }

  // ── Conta QR (motor embutido): envia a mídia direto pelo WhatsApp pareado ──
  if (accountType === 'evolution' && evolutionInstance && WA_EMBEDDED) {
    try {
      const sock = _waSocks[evolutionInstance];
      if (!sock || _waState[evolutionInstance] !== 'open')
        return res.status(400).json({ error: 'WhatsApp QR desconectado — gere o QR novamente em Contas' });
      let fileBuf = Buffer.from(fileBase64, "base64");
      const baseMime = String(mimeType).split(";")[0].trim();
      const jid = await waResolveJid(sock, to); // endereço real (resolve o nono dígito)
      const durSecs = Number(req.body.duration) || 0;
      let sent, content, msgType, qrSentMime = baseMime;
      if (baseMime.startsWith('audio/')) {
        if (baseMime !== 'audio/ogg') {
          try { fileBuf = await convertAudioToOpus(fileBuf); }
          catch (e) { console.error('⚠️ Conversão (QR) falhou, enviando original:', e.message); }
        }
        // Envelope de volume (os "pauzinhos" da mensagem de voz)
        const wf = req.body.voice === true ? await computeWaveform(fileBuf) : null;
        sent = await sock.sendMessage(jid, {
          audio: fileBuf, mimetype: 'audio/ogg; codecs=opus',
          ptt: req.body.voice === true, seconds: durSecs || undefined,
          ...(wf ? { waveform: wf } : {}),
        });
        msgType = 'audio'; qrSentMime = 'audio/ogg';
        content = req.body.voice === true
          ? '🎤 Mensagem de voz' + (durSecs ? ` (${_fmtDur(durSecs)})` : '')
          : `[Áudio: ${fileName}]`;
      } else if (baseMime.startsWith('image/')) {
        sent = await sock.sendMessage(jid, { image: fileBuf, mimetype: baseMime });
        msgType = 'image'; content = `[Imagem: ${fileName}]`;
      } else if (baseMime.startsWith('video/')) {
        let vMime = 'video/mp4';
        if (baseMime !== 'video/mp4') {
          try { fileBuf = await convertVideoToMp4(fileBuf); }
          catch (ve) { console.error('⚠️ Conversão de vídeo falhou, enviando original:', ve.message); vMime = baseMime; }
        }
        sent = await sock.sendMessage(jid, { video: fileBuf, mimetype: vMime });
        msgType = 'video'; qrSentMime = vMime; content = `[Vídeo: ${fileName}]`;
      } else {
        sent = await sock.sendMessage(jid, { document: fileBuf, mimetype: baseMime, fileName });
        msgType = 'document'; content = `[Documento: ${fileName}]`;
      }
      if (supabase) {
        const wamid = sent?.key?.id || null;
        // Guarda a mídia enviada para poder reproduzi-la no CRM
        let mediaPathOut = null;
        const outMime = qrSentMime;
        try {
          const extOut = (outMime.split('/')[1] || 'bin').split('+')[0];
          mediaPathOut = `qr/${evolutionInstance}/out_${wamid || Date.now()}.${extOut}`;
          const { error: upErr } = await supabase.storage.from('wa-media')
            .upload(mediaPathOut, fileBuf, { contentType: outMime, upsert: true });
          if (upErr) { console.error('Storage (saída):', upErr.message); mediaPathOut = null; }
        } catch (_) { mediaPathOut = null; }
        await supabase.from('contacts').upsert(
          { phone: to, last_message_at: new Date().toISOString(), account_id: account_id || null,
            last_message_preview: content, last_message_direction: 'outbound', owner: req.owner || null },
          { onConflict: 'owner,phone' });
        await supabase.from('messages').insert({
          phone: to, content, type: msgType, direction: 'outbound',
          timestamp: new Date().toISOString(), account_id: account_id || null,
          status: 'sent', wamid, owner: req.owner || null,
          media_id: mediaPathOut, media_mime_type: mediaPathOut ? outMime : null });
      }
      console.log(`📤 Mídia (${msgType}) enviada via WhatsApp QR: ${evolutionInstance}`);
      return res.json({ success: true, via: 'qr' });
    } catch (e) {
      console.error('❌ Mídia via QR:', e.message);
      return res.status(500).json({ error: 'Falha ao enviar pelo WhatsApp QR: ' + e.message });
    }
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
    // Mensagem de VOZ (foto de perfil + forma de onda no WhatsApp) — exige OGG/Opus
    if (msgType === "audio" && req.body.voice === true && sendMime === "audio/ogg") mediaObj.voice = true;

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
      const durSecs = Number(req.body.duration) || 0;
      const content = (msgType === "audio" && req.body.voice === true)
        ? "🎤 Mensagem de voz" + (durSecs ? ` (${_fmtDur(durSecs)})` : "")
        : `[${label}: ${fileName}]`;
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId,
          last_message_preview: content, last_message_direction: 'outbound', owner: req.owner || null },
        { onConflict: "owner,phone" }
      );
      await supabase.from("messages").insert({
        phone: to, content,
        type: msgType, direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: 'pending', wamid: mediaWamid, owner: req.owner || null,
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

  // Mídia de conta QR: servida do Supabase Storage (com suporte a Range para áudio/vídeo)
  if (mediaId.startsWith('qr/')) {
    try {
      if (!supabase) return res.status(500).json({ error: 'Storage indisponível' });
      const { data: blob, error } = await supabase.storage.from('wa-media').download(mediaId);
      if (error || !blob) return res.status(404).json({ error: 'Mídia não encontrada' });
      const buf = Buffer.from(await blob.arrayBuffer());
      const total = buf.length;
      const ctype = req.query.mime || blob.type || 'application/octet-stream';
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Accept-Ranges', 'bytes');
      res.setHeader('Content-Type', ctype);
      if (download === '1') {
        const safeFilename = (filename ? decodeURIComponent(filename) : 'midia').replace(/["\r\n]/g, '');
        res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
        res.setHeader('Content-Length', total);
        return res.status(200).end(buf);
      }
      res.setHeader('Cache-Control', 'public, max-age=600');
      const range = req.headers.range;
      if (range) {
        const m = /bytes=(\d*)-(\d*)/.exec(range);
        let start = m && m[1] !== '' ? parseInt(m[1], 10) : 0;
        let end   = m && m[2] !== '' ? parseInt(m[2], 10) : total - 1;
        if (isNaN(start)) start = 0;
        if (isNaN(end) || end >= total) end = total - 1;
        if (start > end || start >= total) { res.setHeader('Content-Range', `bytes */${total}`); return res.status(416).end(); }
        res.status(206);
        res.setHeader('Content-Range', `bytes ${start}-${end}/${total}`);
        res.setHeader('Content-Length', end - start + 1);
        return res.end(buf.subarray(start, end + 1));
      }
      res.setHeader('Content-Length', total);
      return res.status(200).end(buf);
    } catch (e) {
      console.error('❌ Mídia QR:', e.message);
      return res.status(500).json({ error: 'Falha ao carregar mídia' });
    }
  }

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
  _tagCatalog().forEach(t => set.add(t)); // inclui tags criadas no gerenciador (catálogo)
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
      status: 'pending', wamid: tplWamid, owner: req.owner || null,
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
    .from("contacts").select("phone, name, account_id, stage_id, tags, unread_count, first_unread_at, last_message_at, last_message_preview, last_message_direction, favorite, avatar")
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

// ── Editar mensagem já enviada (igual ao WhatsApp — só QR Code; a API oficial
// da Meta não suporta edição). Janela do WhatsApp: até 15 minutos após o envio. ──
app.post('/edit-message', async (req, res) => {
  const { to, wamid, text, account_id } = req.body || {};
  if (!to || !wamid || !text) return res.status(400).json({ error: 'to, wamid e text obrigatórios' });
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  let acct = null;
  if (account_id) {
    const { data } = await supabase.from('accounts').select('type, evolution_instance').eq('id', account_id).maybeSingle();
    acct = data;
  }
  if (!acct?.evolution_instance)
    return res.status(400).json({ error: 'Editar mensagem só é possível em conversas do QR Code — a API oficial da Meta não permite edição.' });
  const sock = _waSocks[acct.evolution_instance];
  if (!sock || _waState[acct.evolution_instance] !== 'open')
    return res.status(400).json({ error: 'WhatsApp desconectado — gere o QR novamente em Contas.' });
  try {
    const jid = await waResolveJid(sock, to);
    await sock.sendMessage(jid, { text, edit: { remoteJid: jid, fromMe: true, id: wamid } });
    await supabase.from('messages').update({ content: text }).eq('wamid', wamid).eq('phone', to);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Falha ao editar: ' + (e.message || 'erro desconhecido') });
  }
});

// ── Indicador "digitando…"/"gravando áudio…" para o cliente (igual ao WhatsApp) ──
// QR Code: presença nativa da Baileys (digitando E gravando). API oficial: indicador
// de digitação da Meta (dura até 25s e marca a última recebida como lida).
app.post('/typing', async (req, res) => {
  try {
    const { to, account_id, state } = req.body || {};
    if (!to || !supabase) return res.json({ success: false });
    const st = ['composing', 'recording', 'paused'].includes(state) ? state : 'composing';
    let acct = null;
    if (account_id) {
      const { data } = await supabase.from('accounts').select('evolution_instance, phone_number_id, token').eq('id', account_id).maybeSingle();
      acct = data;
    }
    // QR Code (Baileys)
    if (acct?.evolution_instance && _waSocks[acct.evolution_instance] && _waState[acct.evolution_instance] === 'open') {
      const sock = _waSocks[acct.evolution_instance];
      const jid = await waResolveJid(sock, to);
      await sock.sendPresenceUpdate(st, jid);
      return res.json({ success: true, via: 'qr' });
    }
    // API oficial (Meta) — só "digitando"; precisa do id da última mensagem recebida
    if (acct?.phone_number_id && acct?.token && st !== 'paused') {
      const { data: lastIn } = await supabase.from('messages').select('wamid')
        .eq('phone', to).eq('direction', 'inbound').eq('account_id', account_id)
        .not('wamid', 'is', null).order('timestamp', { ascending: false }).limit(1).maybeSingle();
      if (lastIn?.wamid) {
        await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`, {
          messaging_product: 'whatsapp', status: 'read', message_id: lastIn.wamid,
          typing_indicator: { type: 'text' }
        }, { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } }).catch(() => {});
        return res.json({ success: true, via: 'cloud' });
      }
    }
    res.json({ success: false });
  } catch (_) { res.json({ success: false }); }
});

// Apaga SÓ as mensagens da conversa — o lead continua no CRM e no Pipeline
// (etapa, etiquetas, anotações e tarefas são preservados)
app.delete("/contacts/:phone/messages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const phone = decodeURIComponent(req.params.phone);
  await supabase.from("messages").delete().eq("phone", phone).eq("owner", req.owner || ' ');
  const { error } = await supabase.from("contacts")
    .update({ last_message_preview: null, last_message_direction: null, unread_count: 0, first_unread_at: null })
    .eq("phone", phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
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

// Registra no CRM uma mensagem de bot que FALHOU — fica visível na conversa com ⚠️ e o MOTIVO,
// para o usuário saber que houve tentativa e por que não foi enviada.
async function _recordBotFail(phone, shown, errText, accountId, owner, type) {
  if (!supabase) return;
  try {
    const ts = new Date().toISOString();
    const content = shown || '[Falha no envio]';
    // Identifica QUAL número tentou enviar (útil no Round Robin com vários números)
    let acctName = '';
    if (accountId) { try { const { data: a } = await supabase.from('accounts').select('name, phone_display').eq('id', accountId).maybeSingle(); if (a) acctName = a.phone_display || a.name || ''; } catch(_){} }
    const fullErr = acctName ? `[Número: ${acctName}] ${errText || 'Falha no envio'}` : (errText || 'Falha no envio');
    await supabase.from('messages').insert({
      phone, content, type: type || 'text', direction: 'outbound', timestamp: ts,
      account_id: accountId || null, status: 'failed', error_info: fullErr, owner: owner || null
    });
    const prev = ('⚠️ ' + content).slice(0, 80);
    await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound' }).eq('phone', phone).eq('owner', owner || ' ');
  } catch(e) { console.error('recordBotFail:', e.message); }
}

async function sendBotMsg(phone, accountId, text, owner) {
  // Resolve a conta com fallback inteligente (conta excluída → conta oficial ativa do dono)
  const acct = await botGetAcct(accountId, owner);
  const phoneNumberId = acct.phone_number_id, token = acct.token;
  const usedAcctId = acct.id || accountId || null;
  // Conta QR Code: envia pelo PRÓPRIO número QR (igual ao envio manual)
  if (acct.evolution_instance) {
    try {
      const r = await waSendText(acct.evolution_instance, phone, text);
      const wamid = r?.key?.id || null;
      if (supabase) {
        const ts = new Date().toISOString();
        await supabase.from('messages').insert({ phone, content: text, type: 'text', direction: 'outbound', timestamp: ts, account_id: usedAcctId, status: 'pending', wamid, owner: owner || null });
        const prev = text.length > 80 ? text.substring(0, 80) + '…' : text;
        await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
      }
      return wamid || true;
    } catch (e) {
      console.error('❌ Bot sendMsg (QR):', e.message);
      await _recordBotFail(phone, text, 'Falha no envio pelo QR Code: ' + (e.message || 'WhatsApp desconectado'), usedAcctId, owner, 'text');
      return null;
    }
  }
  if (!phoneNumberId || !token) {
    await _recordBotFail(phone, text, 'Este número não tem credenciais da API oficial (Phone Number ID/Token). Mensagem do bot não pode ser enviada por ele.', usedAcctId, owner, 'text');
    return null;
  }
  try {
    const r = await axios.post(`https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product:'whatsapp', to:phone, type:'text', text:{body:text} },
      { headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' } });
    const wamid = r.data?.messages?.[0]?.id || null;
    if (supabase) {
      const ts = new Date().toISOString();
      await supabase.from('messages').insert({ phone, content:text, type:'text', direction:'outbound', timestamp:ts, account_id:usedAcctId, status:'pending', wamid, owner:owner||null });
      await applyPendingStatus(wamid); // aplica status que chegou antes do insert
      const prev = text.length>80 ? text.substring(0,80)+'…' : text;
      await supabase.from('contacts').update({ last_message_at:ts, last_message_preview:prev, last_message_direction:'outbound', unread_count:0, first_unread_at:null }).eq('phone',phone).eq('owner',owner||' ');
    }
    return wamid;
  } catch(e) {
    console.error('❌ Bot sendMsg:', e.response?.data||e.message);
    await _recordBotFail(phone, text, metaErrorText(e.response?.data?.error) || (e.message || 'Falha no envio'), usedAcctId, owner, 'text');
    return null;
  }
}

async function botGetAcct(accountId, owner) {
  if (supabase && accountId) {
    const { data } = await supabase.from('accounts').select('id,phone_number_id,token,waba_id,type,evolution_instance').eq('id', accountId).maybeSingle();
    // Conta QR Code escolhida → respeita a escolha (o bot envia pelo próprio QR,
    // sem desviar para a conta da API oficial)
    if (data && data.evolution_instance) return data;
    if (data && data.phone_number_id && data.token) return data;
  }
  // Conta não encontrada (ex.: foi EXCLUÍDA) ou sem API oficial (ex.: QR Code) —
  // usa a conta oficial ATIVA do dono (a mesma do envio manual), em vez de cair
  // num número antigo do .env que pode estar em MODO DE TESTE na Meta (erro 131030).
  if (supabase) {
    try {
      let q = supabase.from('accounts').select('id,phone_number_id,token,waba_id,owner').order('created_at', { ascending: true });
      if (owner !== undefined) q = q.eq('owner', owner || ' ');
      const { data: list } = await q;
      const alt = (list || []).find(a => a.phone_number_id && a.token);
      if (alt) {
        if (accountId) console.warn(`⚠️ Bot: conta ${accountId} não existe mais/não é API oficial — usando a conta oficial ${alt.id} do dono no lugar.`);
        return alt;
      }
    } catch (e) { console.error('botGetAcct fallback:', e.message); }
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
  const acct = await botGetAcct(accountId, owner);
  const usedAcctId = acct.id || accountId || null;
  if (!acct.phone_number_id || !acct.token) {
    await _recordBotFail(phone, `[Modelo: ${cfg.template_name}]`, 'Este número não é da API oficial (sem Phone Number ID/Token). Modelos só podem ser enviados por número da API oficial — não por número de QR Code.', usedAcctId, owner, 'template');
    return null;
  }
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
      await supabase.from('messages').insert({ phone, content: shown, type: 'template', direction: 'outbound', timestamp: ts, account_id: usedAcctId, status: 'pending', wamid: tWamid, owner: owner || null });
      await applyPendingStatus(tWamid);
      await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
    }
    return true;
  } catch(e) {
    console.error('❌ Bot template:', e.response?.data || e.message);
    const shown = bodyText ? renderTemplateBody(bodyText, vars) : `[Modelo: ${cfg.template_name}]`;
    await _recordBotFail(phone, shown, metaErrorText(e.response?.data?.error) || (e.message || 'Falha no envio do modelo'), usedAcctId, owner, 'template');
    return null;
  }
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
      // Rodízio À PROVA DE CONCORRÊNCIA: incrementa o contador de forma ATÔMICA no banco.
      // Dois leads simultâneos recebem índices distintos (o Postgres serializa na linha).
      let idx = 0;
      const { data: rr, error: rrErr } = await supabase.rpc('rr_next', { p_key: key });
      if (!rrErr && rr != null) {
        idx = Number(rr);
      } else {
        // Fallback: se a função rr_next ainda não existir no banco, usa o contador antigo
        if (rrErr) console.warn('rr_next indisponível, usando contador não-atômico:', rrErr.message);
        const { data:s } = await supabase.from('settings').select('value').eq('key', key).maybeSingle();
        idx = parseInt(s?.value || '0', 10); if (isNaN(idx)) idx = 0;
        await supabase.from('settings').upsert({ key, value: String(idx + 1), updated_at: new Date().toISOString() });
      }
      branchIdx = idx % branches.length;
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
    const lb = e.label.toLowerCase();
    // PALAVRA INTEIRA: antes usava "contém", então "sim" casava com "simulação",
    // "assim", etc. — e qualquer resposta aleatória disparava o ramo errado
    const rx = new RegExp('(^|[^\\p{L}\\p{N}])' + lb.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '($|[^\\p{L}\\p{N}])', 'u');
    if (tl === lb || rx.test(tl)) { matched = e; break; }
  }
  // Sem correspondência: usa a saída "outros"/padrão SE existir; senão ENCERRA a
  // espera — a 1ª RESPOSTA do cliente é a que vale. Se depois ele clicar num
  // botão antigo, o bot NÃO segue (a conversa já passou para o atendimento).
  if (!matched) matched = edges.find(e=>e.label==='__other__') || edges.find(e=>!e.label||e.label===''||e.label==='default') || null;
  if (!matched) { await stopRun(run.id, 'stopped'); return true; }
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
      // Prioriza a conta do LEAD (o número com que ele já conversa) — a conta gravada
      // no bot pode ter sido excluída ou ser de outro número
      await startBot(bot.id, phone, leadAcct || bot.account_id, owner || bot.owner);
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

// Timer: retoma runs pausadas/expiradas do bot.
// 30s (era 5s) — economiza CPU/banda no Railway; as esperas dos bots são de
// minutos/horas, então até 30s de folga não muda nada na prática.
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
}, 30000);

// ═══════════════════════════════════════════════════════════════════
//  IA / FAQ — responde automaticamente SÓ a perguntas cadastradas
// ═══════════════════════════════════════════════════════════════════

// Normaliza texto: minúsculas, sem acento, sem pontuação, espaços colapsados
function _faqNorm(s) {
  return (s || '')
    .toString()
    .toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '') // remove acentos (marcas combinantes)
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')                // pontuação -> espaço
    .replace(/\s+/g, ' ')
    .trim();
}

// Palavras muito comuns (não contam na comparação por palavras-chave)
const _FAQ_STOP = new Set(('de a o que e do da em um para com nao uma os no se na por mais as dos ' +
  'como mas ao ele das a seu sua ou quando muito nos ja eu tambem so pelo pela ate isso ela entre ' +
  'era depois sem mesmo aos seus quem nas me esse eles voce essa num nem suas meu as minha numa ' +
  'pelos elas qual nos lhe deles essas esses pelas este dele tu te voces vos ai oi ola bom dia boa ' +
  'tarde noite por favor gostaria queria quero saber sobre voce voces tem teria').split(' '));

function _faqTokens(norm) {
  return norm.split(' ').filter(t => t && t.length > 1 && !_FAQ_STOP.has(t));
}

// Uma palavra "casa" se for igual OU prefixo (>=4 letras) — tolera conjugação/plural
// ex.: fica/ficam, preco/precos, entreg/entrega
function _faqTokHit(kw, msgTokens) {
  return msgTokens.some(m => m === kw || (kw.length >= 4 && (m.startsWith(kw) || kw.startsWith(m))));
}

// Pontua o quão bem a mensagem casa com UM gatilho (0 a 1)
function _faqScoreTrigger(msgNorm, msgTokens, trigger) {
  const tNorm = _faqNorm(trigger);
  if (!tNorm) return 0;

  // modo palavras-chave: gatilho com vírgula = TODAS as palavras precisam aparecer
  if (trigger.includes(',')) {
    const groups = tNorm.split(' ').filter(Boolean); // já sem vírgula após normalizar
    const kws = _faqTokens(tNorm);
    const need = kws.length ? kws : groups;
    if (!need.length) return 0;
    const hit = need.every(k => _faqTokHit(k, msgTokens));
    return hit ? 0.95 : 0;
  }

  // frase exata
  if (msgNorm === tNorm) return 1;
  // frase contida na mensagem (com limites de palavra)
  if ((' ' + msgNorm + ' ').includes(' ' + tNorm + ' ')) return 0.95;

  // sobreposição de palavras-chave (quantas palavras do gatilho aparecem na msg)
  const tTokens = _faqTokens(tNorm);
  if (!tTokens.length) return 0;
  const inter = tTokens.filter(t => _faqTokHit(t, msgTokens)).length;
  const ratio = inter / tTokens.length;
  if (ratio >= 0.8) return 0.85;
  if (ratio >= 0.6) return 0.7;
  return 0;
}

// Escolhe o melhor FAQ para uma mensagem. Retorna { faq, score } ou null.
// (Estruturado para, no futuro, trocar/complementar por um LLM sem mexer no resto.)
async function matchFaq(text, owner) {
  if (!supabase || !text) return null;
  let q = supabase.from('faqs').select('*').eq('enabled', true);
  q = owner ? q.eq('owner', owner) : q.is('owner', null);
  const { data: faqs } = await q;
  if (!faqs || !faqs.length) return null;

  const msgNorm = _faqNorm(text);
  const msgTokens = _faqTokens(msgNorm);
  const THRESHOLD = 0.6;

  let best = null;
  for (const faq of faqs) {
    // gatilhos = 1 por linha; se vazio, usa a própria pergunta como gatilho
    const lines = (faq.triggers || '').split('\n').map(s => s.trim()).filter(Boolean);
    if (!lines.length && faq.question) lines.push(faq.question);
    let score = 0;
    for (const line of lines) {
      const s = _faqScoreTrigger(msgNorm, msgTokens, line);
      if (s > score) score = s;
      if (score >= 1) break;
    }
    if (score >= THRESHOLD && (!best || score > best.score)) best = { faq, score };
  }
  return best;
}

// Executa a auto-resposta: valida interruptor, casa a pergunta, respeita "1x por cliente" e envia
async function handleFaqAutoReply(phone, text, owner, accountId) {
  if (!supabase) return false;
  if ((_settings['faq_enabled'] || 'off') !== 'on') return false; // interruptor GERAL

  // Filtro por conta de WhatsApp: se 'faq_accounts' foi configurado (lista JSON de IDs),
  // a IA só responde nas contas dessa lista. Se nunca foi configurado, vale para TODAS.
  const accSetting = _settings['faq_accounts'];
  if (accSetting !== undefined && accSetting !== null && accSetting !== '') {
    try {
      const list = JSON.parse(accSetting);
      if (Array.isArray(list) && !list.map(String).includes(String(accountId))) return false;
    } catch (_) {}
  }

  // Modo IA (Groq): entende o contexto da conversa. Se falhar, cai no texto grátis.
  let m = null;
  if (_faqAiOn()) {
    try { m = await matchFaqLLM(phone, text, owner); }
    catch (e) { console.error('🤖 IA classificador falhou, usando texto:', e.response?.data?.error?.message || e.message); m = await matchFaq(text, owner); }
  } else {
    m = await matchFaq(text, owner);
  }
  if (!m) return false;

  // "só 1x por cliente/pergunta": já respondeu esse FAQ para esse contato?
  const { data: already } = await supabase.from('faq_replies')
    .select('id').eq('owner', owner || null).eq('phone', phone).eq('faq_id', m.faq.id).maybeSingle();
  if (already) { console.log(`🤖 FAQ #${m.faq.id} já respondido a ${phone} — ignorado`); return false; }

  // descobre a conta de WhatsApp certa (número do lead), se não veio
  let acct = accountId;
  if (!acct) {
    let cq = supabase.from('contacts').select('account_id').eq('phone', phone);
    if (owner) cq = cq.eq('owner', owner);
    const { data: ct } = await cq.maybeSingle();
    acct = ct?.account_id || null;
  }

  // Reserva JÁ o "respondido" (o índice único evita corrida/duplicidade se chegarem
  // mais mensagens durante o atraso). Se o envio falhar depois, a reserva é removida.
  const { error: resErr } = await supabase.from('faq_replies')
    .insert({ owner: owner || null, phone, faq_id: m.faq.id });
  if (resErr) { console.log(`🤖 FAQ #${m.faq.id} já respondido a ${phone} — ignorado`); return false; }

  // Atraso humanizado antes de enviar. Padrão 25s; ajustável via settings 'faq_delay_seconds'.
  const delaySec = parseInt(_settings['faq_delay_seconds'], 10);
  const delayMs = Math.max(0, (Number.isFinite(delaySec) ? delaySec : 25) * 1000);
  setTimeout(async () => {
    try {
      const wamid = await sendBotMsg(phone, acct, m.faq.answer, owner);
      if (!wamid) {
        // envio falhou: remove a reserva para permitir nova tentativa numa próxima mensagem
        await supabase.from('faq_replies').delete()
          .eq('owner', owner || null).eq('phone', phone).eq('faq_id', m.faq.id);
        console.error('🤖 FAQ: falha ao enviar resposta a', phone, '(sem conta/token ou fora da janela 24h)');
        return;
      }
      console.log(`🤖 FAQ #${m.faq.id} respondido a ${phone} (score ${m.score.toFixed(2)}, após ${delayMs/1000}s)`);
    } catch (e) {
      try {
        await supabase.from('faq_replies').delete()
          .eq('owner', owner || null).eq('phone', phone).eq('faq_id', m.faq.id);
      } catch (_) {}
      console.error('🤖 FAQ: erro no envio atrasado a', phone, e.message);
    }
  }, delayMs);

  return true;
}

// ── CRUD de FAQ (perguntas/respostas da IA) ──
app.get('/faqs', async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase.from('faqs')
    .select('*').eq('owner', req.owner || ' ').order('created_at', { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

app.post('/faqs', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const { question, answer, triggers, enabled } = req.body || {};
  if (!question || !answer) return res.status(400).json({ error: 'pergunta e resposta são obrigatórias' });
  const { data, error } = await supabase.from('faqs').insert({
    owner: req.owner || null,
    question: String(question).trim(),
    answer: String(answer),
    triggers: String(triggers || '').trim(),
    enabled: enabled !== false
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.put('/faqs/:id', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const upd = { updated_at: new Date().toISOString() };
  if (req.body.question !== undefined) upd.question = String(req.body.question).trim();
  if (req.body.answer !== undefined) upd.answer = String(req.body.answer);
  if (req.body.triggers !== undefined) upd.triggers = String(req.body.triggers).trim();
  if (req.body.enabled !== undefined) upd.enabled = !!req.body.enabled;
  const { data, error } = await supabase.from('faqs')
    .update(upd).eq('id', req.params.id).eq('owner', req.owner || ' ').select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete('/faqs/:id', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const { error } = await supabase.from('faqs')
    .delete().eq('id', req.params.id).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// Teste rápido: "o que a IA responderia para esta mensagem?" (não envia nada)
app.post('/faqs/test', async (req, res) => {
  const text = (req.body && req.body.text) || '';
  const m = await matchFaq(text, req.owner || null);
  if (!m) return res.json({ match: false });
  res.json({ match: true, question: m.faq.question, answer: m.faq.answer, score: m.score });
});

// ═══════════════════════ Classificador por IA (Groq) ═══════════════════════
// A IA NÃO escreve respostas: ela lê o contexto da conversa e escolhe QUAL das
// perguntas cadastradas encaixa (ou nenhuma). A resposta enviada é a sua, pronta.
const GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';
const GROQ_DEFAULT_MODEL = 'openai/gpt-oss-20b'; // barato e ativo (jul/2026); trocável via settings
const _faqAiOn = () => _settings['faq_mode'] === 'ai' && !!process.env.GROQ_API_KEY;

// Últimas mensagens da conversa (contexto), mais antigas primeiro
async function getRecentConversation(phone, owner, limit) {
  if (!supabase) return [];
  let q = supabase.from('messages').select('direction, content, timestamp').eq('phone', phone);
  if (owner) q = q.eq('owner', owner);
  const { data } = await q.order('timestamp', { ascending: false }).limit(limit || 12);
  const rows = (data || []).reverse();
  return rows.map(r => ({
    who: r.direction === 'outbound' ? 'atendente' : 'cliente',
    text: (r.content || '').toString().slice(0, 300)
  }));
}

// Classifica via Groq: retorna { faq, score } ou null. Lança erro se a API falhar.
async function matchFaqLLM(phone, text, owner) {
  if (!supabase) return null;
  const { data: faqs } = await supabase.from('faqs').select('*')
    .eq('enabled', true).eq('owner', owner || ' '); // mesmo filtro do GET /faqs
  if (!faqs || !faqs.length) return null;

  const list = faqs.map((f, i) => `${i + 1}) ${f.question}`).join('\n');
  const convo = await getRecentConversation(phone, owner, 12);
  const convoTxt = convo.map(m => `${m.who}: ${m.text}`).join('\n')
    || `cliente: ${(text || '').toString().slice(0, 300)}`;

  const sys = 'Você classifica a intenção do cliente em um atendimento por WhatsApp. '
    + 'Receberá uma lista de PERGUNTAS numeradas e a CONVERSA. Considerando o contexto de toda a conversa, '
    + 'identifique qual PERGUNTA corresponde à intenção ATUAL do cliente (a última coisa que ele quis dizer). '
    + 'Responda SOMENTE com JSON no formato {"id": N}, onde N é o número da pergunta. '
    + 'Se nenhuma corresponder, responda {"id": 0}. Não escreva mais nada.';
  const usr = `PERGUNTAS:\n${list}\n\nCONVERSA:\n${convoTxt}`;

  const model = _settings['faq_ai_model'] || GROQ_DEFAULT_MODEL;
  const body = {
    model,
    messages: [{ role: 'system', content: sys }, { role: 'user', content: usr }],
    temperature: 0,
    max_tokens: 800 // modelos de raciocínio (gpt-oss) usam parte do orçamento pensando
  };
  // gpt-oss aceita esforço de raciocínio: baixo = mais rápido e barato
  if (/gpt-oss/i.test(model)) body.reasoning_effort = 'low';
  const r = await axios.post(GROQ_URL, body, {
    headers: { Authorization: 'Bearer ' + process.env.GROQ_API_KEY, 'Content-Type': 'application/json' },
    timeout: 15000
  });

  const msg = r.data?.choices?.[0]?.message || {};
  const out = ((msg.content || '') + ' ' + (msg.reasoning || '')).trim();
  let idNum = null;
  const jm = out.match(/["']?id["']?\s*[:=]\s*(\d+)/i); // procura o "id": N que pedimos
  if (jm) idNum = parseInt(jm[1], 10);
  else { const nums = out.match(/\d+/g); if (nums) idNum = parseInt(nums[nums.length - 1], 10); } // senão, último número
  if (!idNum || idNum < 1 || idNum > faqs.length) return null; // 0 ou inválido = nenhuma
  return { faq: faqs[idNum - 1], score: 1 };
}

// GET /faqs/ai-status — modo atual, se a chave está no servidor, e o modelo
app.get('/faqs/ai-status', (req, res) => {
  res.json({
    mode: _settings['faq_mode'] || 'text',
    keyConfigured: !!process.env.GROQ_API_KEY,
    model: _settings['faq_ai_model'] || GROQ_DEFAULT_MODEL
  });
});

// POST /faqs/ai-test — testa a classificação por IA com uma mensagem de exemplo
app.post('/faqs/ai-test', async (req, res) => {
  if (!process.env.GROQ_API_KEY) return res.json({ ok: false, error: 'Chave GROQ_API_KEY não configurada no servidor.' });
  const text = (req.body && req.body.text) || 'quanto tempo demora pra liberar o dinheiro?';
  // diagnóstico: quantas perguntas ativas existem para este dono
  let faqCount = 0;
  try { const { data } = await supabase.from('faqs').select('id').eq('enabled', true).eq('owner', req.owner || ' '); faqCount = (data || []).length; } catch (_) {}
  try {
    const m = await matchFaqLLM('__teste__' + Date.now(), text, req.owner || null);
    if (!m) return res.json({ ok: true, match: false, faqCount });
    res.json({ ok: true, match: true, question: m.faq.question, answer: m.faq.answer, faqCount });
  } catch (e) {
    res.json({ ok: false, error: e.response?.data?.error?.message || e.message, faqCount });
  }
});

// ═══════════════════════ Regra "contato errado" ═══════════════════════
// Quando o cliente avisa que a mensagem foi para a pessoa errada, envia um
// pedido de desculpas e aplica uma TAG no lead. Config via settings (dedicada,
// não polui o cadastro geral de perguntas). Reusa scoring e atraso do FAQ.
const WRONGPERSON_DEFAULT_TRIGGERS = [
  'pessoa errada','numero errado','foi engano','nao sou essa pessoa','nao te conheco',
  'esse nome','com esse nome','descadastrar','me descadastrar','remover meu contato',
  'tirar meu contato','sair da lista','nao quero receber','parar de receber'
].join('\n');
const WRONGPERSON_DEFAULT_ANSWER = 'Desculpe o incômodo, vou retirar seu contato da lista 🙏🏼';
const WRONGPERSON_DEFAULT_TAG = 'REMOVER';
const WRONGPERSON_FAQ_ID = -1; // sentinela no controle de "1x por contato" (tabela faq_replies)

function matchWrongPerson(text) {
  const raw = _settings['wrongperson_triggers'] || WRONGPERSON_DEFAULT_TRIGGERS;
  const lines = raw.split('\n').map(s => s.trim()).filter(Boolean);
  const msgNorm = _faqNorm(text);
  const msgTokens = _faqTokens(msgNorm);
  let score = 0;
  for (const line of lines) {
    const s = _faqScoreTrigger(msgNorm, msgTokens, line);
    if (s > score) score = s;
    if (score >= 1) break;
  }
  return score >= 0.6 ? score : 0;
}

async function addTagToContact(phone, owner, tag) {
  if (!supabase || !tag) return;
  const { data: ct } = await supabase.from('contacts').select('tags')
    .eq('phone', phone).eq('owner', owner || ' ').maybeSingle();
  const tags = Array.isArray(ct?.tags) ? ct.tags.slice() : [];
  if (!tags.includes(tag)) {
    tags.push(tag);
    await supabase.from('contacts').update({ tags }).eq('phone', phone).eq('owner', owner || ' ');
  }
}

// Retorna true se assumiu a resposta (para o FAQ não responder também)
async function handleWrongPerson(phone, text, owner, accountId) {
  if (!supabase) return false;
  if ((_settings['wrongperson_enabled'] || 'off') !== 'on') return false;

  // mesmo filtro de contas do FAQ
  const accSetting = _settings['faq_accounts'];
  if (accSetting !== undefined && accSetting !== null && accSetting !== '') {
    try {
      const list = JSON.parse(accSetting);
      if (Array.isArray(list) && !list.map(String).includes(String(accountId))) return false;
    } catch (_) {}
  }

  if (!matchWrongPerson(text)) return false;

  // 1x por contato (reserva antes do atraso; índice único evita duplicidade)
  const { error: resErr } = await supabase.from('faq_replies')
    .insert({ owner: owner || null, phone, faq_id: WRONGPERSON_FAQ_ID });
  if (resErr) { console.log('🤖 Contato errado já tratado para', phone, '— ignorado'); return true; }

  let acct = accountId;
  if (!acct) {
    let cq = supabase.from('contacts').select('account_id').eq('phone', phone);
    if (owner) cq = cq.eq('owner', owner);
    const { data: ct } = await cq.maybeSingle();
    acct = ct?.account_id || null;
  }

  const answer = _settings['wrongperson_answer'] || WRONGPERSON_DEFAULT_ANSWER;
  const tag = _settings['wrongperson_tag'] || WRONGPERSON_DEFAULT_TAG;
  const delaySec = parseInt(_settings['faq_delay_seconds'], 10);
  const delayMs = Math.max(0, (Number.isFinite(delaySec) ? delaySec : 25) * 1000);

  setTimeout(async () => {
    try {
      const wamid = await sendBotMsg(phone, acct, answer, owner);
      if (!wamid) {
        await supabase.from('faq_replies').delete()
          .eq('owner', owner || null).eq('phone', phone).eq('faq_id', WRONGPERSON_FAQ_ID);
        console.error('🤖 Contato errado: falha ao enviar a', phone, '(sem conta/token ou fora da janela 24h)');
        return;
      }
      await addTagToContact(phone, owner, tag);
      console.log(`🤖 Contato errado tratado: ${phone} (tag "${tag}", após ${delayMs/1000}s)`);
    } catch (e) {
      try {
        await supabase.from('faq_replies').delete()
          .eq('owner', owner || null).eq('phone', phone).eq('faq_id', WRONGPERSON_FAQ_ID);
      } catch (_) {}
      console.error('🤖 Contato errado: erro no envio a', phone, e.message);
    }
  }, delayMs);

  return true;
}

// ═══════════════════════ Gerenciador de Tags ═══════════════════════
// Catálogo de tags "criadas" (mesmo sem lead) em settings 'tag_catalog' (array JSON)
function _tagCatalog() {
  try { const a = JSON.parse(_settings['tag_catalog'] || '[]'); return Array.isArray(a) ? a : []; }
  catch (_) { return []; }
}
async function _saveTagCatalog(arr) {
  const uniq = Array.from(new Set(arr.filter(Boolean)));
  await supabase.from('settings').upsert({ key: 'tag_catalog', value: JSON.stringify(uniq), updated_at: new Date().toISOString() });
  _settings['tag_catalog'] = JSON.stringify(uniq);
}

// Lista tags com contagem de leads (inclui as do catálogo com contagem 0)
app.get('/tags/manage', async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase.from('contacts').select('tags').eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  const counts = {};
  (data || []).forEach(c => (c.tags || []).forEach(t => { if (t) counts[t] = (counts[t] || 0) + 1; }));
  _tagCatalog().forEach(t => { if (!(t in counts)) counts[t] = 0; });
  const out = Object.keys(counts).sort((a, b) => a.localeCompare(b)).map(name => ({ name, count: counts[name] }));
  res.json(out);
});

// Cria/cadastra uma tag no catálogo
app.post('/tags/manage', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const name = ((req.body && req.body.name) || '').toString().trim();
  if (!name) return res.status(400).json({ error: 'nome obrigatório' });
  await _saveTagCatalog([..._tagCatalog(), name]);
  res.json({ ok: true, name });
});

// Exclui uma tag de TODOS os leads e do catálogo
app.delete('/tags/manage', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const name = ((req.body && req.body.name) || '').toString();
  if (!name) return res.status(400).json({ error: 'nome obrigatório' });
  const { data } = await supabase.from('contacts').select('phone, tags').eq('owner', req.owner || ' ');
  for (const c of data || []) {
    if ((c.tags || []).includes(name)) {
      const tags = c.tags.filter(t => t !== name);
      await supabase.from('contacts').update({ tags }).eq('phone', c.phone).eq('owner', req.owner || ' ');
    }
  }
  await _saveTagCatalog(_tagCatalog().filter(t => t !== name));
  res.json({ ok: true });
});

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

// Disparo EM MASSA para uma LISTA de leads selecionados (telefones enviados pelo front).
// Responde na hora com a contagem e processa em segundo plano (com throttle e dedupe).
app.post('/bots/:id/start-bulk', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const owner = req.owner || ' ';
  const botId = req.params.id;
  const { data: own } = await supabase.from('bots').select('id, account_id').eq('id',botId).eq('owner', owner).maybeSingle();
  if (!own) return res.status(404).json({error:'Bot não encontrado'});
  let phones = Array.isArray(req.body?.phones) ? req.body.phones.filter(Boolean).map(String) : [];
  phones = [...new Set(phones)];
  if (!phones.length) return res.status(400).json({error:'Nenhum lead selecionado'});
  // Segurança: só dispara para contatos do próprio dono
  const { data: contacts } = await supabase.from('contacts').select('phone, account_id').eq('owner', owner).in('phone', phones);
  const valid = contacts || [];
  res.json({ success:true, total: valid.length }); // responde já; processa em background
  if (!valid.length) return;

  (async () => {
    let started=0, skipped=0;
    for (const c of valid) {
      try {
        const { data: active } = await supabase.from('bot_runs').select('id')
          .eq('contact_phone',c.phone).eq('bot_id',botId)
          .in('status',['running','waiting_reply','paused']).maybeSingle();
        if (active) { skipped++; continue; }
        const run = await startBot(botId, c.phone, c.account_id || own.account_id || null, req.owner);
        if (run) started++; else skipped++;
      } catch(e){ skipped++; console.error('start-bulk:', c.phone, e.message); }
      await new Promise(r=>setTimeout(r, 200)); // ~5/seg
    }
    console.log(`📢 Disparo em massa (selecionados) bot ${botId}: ${started} iniciados, ${skipped} pulados de ${valid.length}`);
  })().catch(e=>console.error('Disparo em massa falhou:', e.message));
});

// Disparo EM MASSA de um bot para todos os leads com TAREFA EM ABERTO (não concluída).
// Responde imediatamente com a contagem e processa em segundo plano (com throttle).
app.post('/bots/:id/start-open-tasks', async (req,res) => {
  if (!supabase) return res.status(500).json({error:'Supabase não configurado'});
  const owner = req.owner || ' ';
  const botId = req.params.id;
  // confirma que o bot é do dono
  const { data: own } = await supabase.from('bots').select('id, account_id, active').eq('id',botId).eq('owner', owner).maybeSingle();
  if (!own) return res.status(404).json({error:'Bot não encontrado'});
  // Leads com tarefa em aberto (done=false) e com telefone
  const { data: tasks } = await supabase.from('tasks').select('phone').eq('owner', owner).eq('done', false).not('phone','is',null);
  const phones = [...new Set((tasks||[]).map(t=>t.phone).filter(Boolean))];
  res.json({ success:true, total: phones.length }); // responde já; processa em background
  if (!phones.length) return;

  (async () => {
    // account_id de cada contato (o bot dispara pelo número do lead)
    const { data: contacts } = await supabase.from('contacts').select('phone, account_id').eq('owner', owner).in('phone', phones);
    const acctByPhone = {}; (contacts||[]).forEach(c=>{ acctByPhone[c.phone]=c.account_id; });
    let started=0, skipped=0;
    for (const phone of phones) {
      try {
        // pula quem já está com ESTE bot rodando (evita disparo duplicado)
        const { data: active } = await supabase.from('bot_runs').select('id')
          .eq('contact_phone',phone).eq('bot_id',botId)
          .in('status',['running','waiting_reply','paused']).maybeSingle();
        if (active) { skipped++; continue; }
        const run = await startBot(botId, phone, acctByPhone[phone] || own.account_id || null, req.owner);
        if (run) started++; else skipped++;
      } catch(e){ skipped++; console.error('start-open-tasks:', phone, e.message); }
      await new Promise(r=>setTimeout(r, 200)); // ~5/seg — respeita limites do WhatsApp
    }
    console.log(`📢 Disparo em massa (tarefas abertas) bot ${botId}: ${started} iniciados, ${skipped} pulados de ${phones.length}`);
  })().catch(e=>console.error('Disparo em massa falhou:', e.message));
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
setInterval(loadSettings, 5 * 60 * 1000); // recarrega settings (ex.: novos membros da equipe) sem precisar de redeploy

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

// ═══════════════════════════════════════
// MOTOR DE WHATSAPP QR EMBUTIDO (Baileys)
// Sem Evolution externa configurada, o QR roda DENTRO deste backend — custo zero.
// Sessões ficam no Supabase (tabela wa_sessions) e sobrevivem a redeploys.
// ═══════════════════════════════════════
const WA_EMBEDDED = !process.env.EVOLUTION_API_URL;
// Node 18 não tem WebCrypto global (o Baileys precisa) — este polyfill resolve
if (typeof globalThis.crypto === 'undefined' || !globalThis.crypto?.subtle) {
  try { globalThis.crypto = require('crypto').webcrypto; } catch (_) {}
}
let _baileys = null, _qrcode = null, _pino = null;
if (WA_EMBEDDED) {
  try {
    _baileys = require('@whiskeysockets/baileys');
    _qrcode = require('qrcode');
    _pino = require('pino');
    // Deixa o ffmpeg visível no PATH (o motor usa para gerar miniaturas de vídeo)
    try {
      const _p = require('path');
      process.env.PATH = (process.env.PATH || '') + _p.delimiter + _p.dirname(require('@ffmpeg-installer/ffmpeg').path);
    } catch (_) {}
    console.log('✅ Motor de WhatsApp QR embutido (Baileys) carregado');
  } catch (e) { console.log('⚠️ Baileys não instalado — conexão por QR indisponível:', e.message); }
}

const _waSocks = {}, _waState = {}, _waPhone = {}, _waErr = {};
const _waQrRetries = {}, _waCreatedAt = {}, _waRegistered = {}; // controle de instâncias que nunca parearam
const _waReconnDelay = {}; // espera progressiva entre reconexões (economia no Railway)
let _waVerCache = { v: null, ts: 0 }; // cache da versão do Baileys (evita consulta na internet a cada reconexão)
let _waVersion = null;

// Encerra e limpa uma instância que nunca chegou a parear (evita "zumbis" que
// ficam gerando QR para sempre — a Meta detecta o excesso e bloqueia o pareamento
// com "não foi possível conectar, tente mais tarde")
async function waCleanupInstance(inst) {
  try { _waSocks[inst]?.end?.(undefined); } catch (_) {}
  delete _waSocks[inst]; delete _waState[inst]; delete _waPhone[inst];
  delete _waErr[inst]; delete qrCache[inst]; delete _waQrRetries[inst]; delete _waCreatedAt[inst]; delete _waRegistered[inst];
  try { if (supabase) await supabase.from('wa_sessions').delete().eq('instance', inst); } catch (_) {}
  console.log(`🧹 Instância não pareada encerrada: ${inst}`);
}

// Diagnóstico do motor embutido (para depurar sem acesso aos logs)
app.get('/wa/debug', async (req, res) => {
  const instances = {};
  for (const k of new Set([...Object.keys(_waSocks), ...Object.keys(_waState)])) {
    instances[k] = { state: _waState[k] || null, phone: _waPhone[k] || null, err: _waErr[k] || null, temQr: !!qrCache[k] };
  }
  let contatosComFoto = null;
  try {
    const { count } = await supabase.from('contacts').select('phone', { count: 'exact', head: true }).not('avatar', 'is', null);
    contatosComFoto = count;
  } catch (_) {}
  res.json({ embedded: WA_EMBEDDED, baileysCarregado: !!_baileys, versaoWA: _waVersion, contatosComFoto, instances });
});

// Serve a foto de perfil (rota simples, sem barra codificada na URL)
app.get('/avatar/:file', async (req, res) => {
  try {
    if (!supabase) return res.status(500).end();
    const file = String(req.params.file).replace(/[^\w.\-]/g, '');
    const { data: blob, error } = await supabase.storage.from('wa-media').download(`qr/avatars/${file}`);
    if (error || !blob) return res.status(404).end();
    const buf = Buffer.from(await blob.arrayBuffer());
    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.end(buf);
  } catch (e) { res.status(500).end(); }
});

// Guarda credenciais/chaves da sessão no Supabase (preserva Buffers via BufferJSON)
async function useSupabaseAuthState(instance) {
  const { initAuthCreds, BufferJSON, proto } = _baileys;
  const read = async (key) => {
    const { data } = await supabase.from('wa_sessions').select('data').eq('instance', instance).eq('key', key).maybeSingle();
    return data ? JSON.parse(JSON.stringify(data.data), BufferJSON.reviver) : null;
  };
  const write = async (key, value) => {
    await supabase.from('wa_sessions').upsert(
      { instance, key, data: JSON.parse(JSON.stringify(value, BufferJSON.replacer)), updated_at: new Date().toISOString() },
      { onConflict: 'instance,key' });
  };
  const del = async (key) => { await supabase.from('wa_sessions').delete().eq('instance', instance).eq('key', key); };
  const creds = (await read('creds')) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          for (const id of ids) {
            let v = await read(`${type}-${id}`);
            if (type === 'app-state-sync-key' && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
            out[id] = v;
          }
          return out;
        },
        set: async (data) => {
          for (const type in data) for (const id in data[type]) {
            const v = data[type][id];
            if (v) await write(`${type}-${id}`, v); else await del(`${type}-${id}`);
          }
        },
      },
    },
    saveCreds: async () => write('creds', creds),
  };
}

async function waStart(instanceName) {
  if (!_baileys || !supabase) throw new Error('Motor de QR indisponível no servidor');
  if (_waSocks[instanceName]) { try { _waSocks[instanceName].end(undefined); } catch (_) {} delete _waSocks[instanceName]; }
  const { state, saveCreds } = await useSupabaseAuthState(instanceName);
  _waRegistered[instanceName] = !!state?.creds?.registered; // já pareado antes? (protege da limpeza)
  // Versão do WhatsApp Web: consulta na internet no máximo a cada 6h (cache)
  let version = _waVerCache.v;
  if (!version || Date.now() - _waVerCache.ts > 6 * 3600000) {
    const r = await _baileys.fetchLatestBaileysVersion().catch(e => {
      _waErr[instanceName] = 'fetchVersion: ' + e.message;
      return { version: undefined };
    });
    version = r.version;
    if (version) _waVerCache = { v: version, ts: Date.now() };
  }
  _waVersion = version || 'padrão da lib';
  const sock = _baileys.default({
    version,
    auth: state,
    logger: _pino({ level: 'silent' }),
    printQRInTerminal: false,
    // Identidade reconhecida pelo WhatsApp — nomes personalizados fazem o
    // pareamento falhar com "não foi possível conectar novos dispositivos".
    // "Desktop" = aparece como app WhatsApp Desktop (identidade mais natural),
    // o que reduz o aviso de "suspeita de golpe" na hora de escanear o QR.
    browser: _baileys.Browsers ? _baileys.Browsers.macOS('Desktop') : ['Mac OS', 'Desktop', '10.15.7'],
    syncFullHistory: false,
    // "online" = o WhatsApp entrega as mensagens na hora (offline ele segura/atrasa)
    markOnlineOnConnect: true,
  });
  _waSocks[instanceName] = sock;
  _waState[instanceName] = 'connecting';

  sock.ev.on('creds.update', saveCreds);

  // Tiques de entrega/leitura das mensagens enviadas por QR (✓✓ e ✓✓ azul)
  sock.ev.on('messages.update', async (updates) => {
    if (!supabase) return;
    for (const u of updates || []) {
      const st = u.update?.status, id = u.key?.id;
      if (!st || !id) continue;
      const mapped = st === 4 || st === 'READ' ? 'read' : (st === 3 || st === 'DELIVERY_ACK' ? 'delivered' : null);
      if (mapped) { try { await updateMsgStatus(id, { status: mapped }); } catch (_) {} }
    }
  });

  sock.ev.on('connection.update', (u) => {
    const { connection, qr, lastDisconnect } = u;
    if (qr && _qrcode) {
      console.log(`📲 QR emitido para ${instanceName}`);
      _qrcode.toDataURL(qr).then(url => { qrCache[instanceName] = url; }).catch(e => { _waErr[instanceName] = 'qrcode: ' + e.message; });
    }
    if (connection === 'open') {
      _waState[instanceName] = 'open';
      delete qrCache[instanceName];
      _waQrRetries[instanceName] = 0; // pareou — zera o contador de tentativas
      _waRegistered[instanceName] = true;
      _waReconnDelay[instanceName] = 4000; // conexão ok — volta à espera mínima
      _waPhone[instanceName] = String(sock.user?.id || '').split(':')[0].split('@')[0] || null;
      console.log(`✅ WhatsApp QR conectado: ${instanceName} (${_waPhone[instanceName]})`);
    }
    if (connection === 'close') {
      _waState[instanceName] = 'close';
      const code = lastDisconnect?.error?.output?.statusCode;
      _waErr[instanceName] = `close ${code || '?'}: ${lastDisconnect?.error?.message || 'sem detalhe'}`;
      // Instância que NUNCA pareou (usuário abandonou a tela do QR): não fica
      // tentando para sempre — 3 ciclos de QR e para. Evita o bloqueio da Meta
      // ("não foi possível conectar, tente mais tarde") por excesso de tentativas.
      const nuncaPareou = !state?.creds?.registered;
      if (nuncaPareou && code !== _baileys.DisconnectReason.restartRequired) {
        _waQrRetries[instanceName] = (_waQrRetries[instanceName] || 0) + 1;
        if (_waQrRetries[instanceName] > 3) { waCleanupInstance(instanceName); return; }
      }
      if (code === _baileys.DisconnectReason.loggedOut) {
        console.log(`🔌 ${instanceName}: sessão encerrada (logout no celular)`);
        delete _waSocks[instanceName];
        supabase.from('wa_sessions').delete().eq('instance', instanceName).then(() => {}, () => {});
      } else if (_waSocks[instanceName] === sock) {
        // 515 (restartRequired) chega LOGO APÓS escanear o QR: o WhatsApp exige
        // reiniciar a conexão imediatamente para concluir o pareamento. Esperar 4s
        // aqui fazia o celular desistir com "Não foi possível conectar o dispositivo".
        const restartNow = code === _baileys.DisconnectReason.restartRequired;
        let waitMs;
        if (restartNow) {
          waitMs = 300;
          _waReconnDelay[instanceName] = 4000;
        } else {
          // Espera PROGRESSIVA: 4s → 8s → 16s… até 5 min. Se o WhatsApp ficar fora
          // por horas (celular desligado), o servidor não gasta CPU tentando a cada 4s.
          waitMs = _waReconnDelay[instanceName] || 4000;
          _waReconnDelay[instanceName] = Math.min(waitMs * 2, 5 * 60000);
        }
        console.log(`↩️ ${instanceName}: reconectando em ${waitMs}ms (código ${code || '?'}${restartNow ? ' — pós-pareamento' : ''})`);
        setTimeout(() => waStart(instanceName).catch(e => console.error('WA reconnect:', e.message)), waitMs);
      }
    }
  });

  // Mensagens recebidas → reaproveita TODO o fluxo existente do /evolution-webhook
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify' && type !== 'append') return;
    for (const m of messages || []) {
      if (!m.message) continue;
      // Conversa "com você mesmo" (recados no próprio número conectado): ignora —
      // sem isso, ao escanear o QR aparecia um chat com o próprio número no CRM
      try {
        const own = String(sock.user?.id || '').split(':')[0].split('@')[0].replace(/\D/g, '');
        const rjSelf = String(m.key?.remoteJid || '');
        const chatDigits = (String(m.key?.remoteJidAlt || '') || rjSelf).split('@')[0].replace(/\D/g, '');
        if (own && chatDigits && chatDigits === own) continue;
      } catch (_) {}
      // Mensagem EDITADA (pelo CRM ou pelo celular) → atualiza o texto da bolha
      // original, em vez de criar uma bolha nova "[Mensagem enviada]"
      const _pm = m.message.protocolMessage;
      if (_pm && _pm.editedMessage && _pm.key?.id) {
        const novoTxt = _pm.editedMessage.conversation || _pm.editedMessage.extendedTextMessage?.text || null;
        if (novoTxt && supabase) {
          try { await supabase.from('messages').update({ content: novoTxt }).eq('wamid', _pm.key.id); } catch (_) {}
        }
        continue;
      }
      // Outras mensagens de protocolo (controle interno do WhatsApp) não viram bolha
      if (_pm) continue;
      // Reação (emoji sobre uma mensagem) → atualiza a mensagem alvo, não cria nova
      if (m.message.reactionMessage) {
        const r = m.message.reactionMessage;
        if (supabase && r.key?.id) {
          try {
            await supabase.from('messages')
              .update({ reaction: r.text || null, reaction_by: m.key?.fromMe ? 'me' : 'contact' })
              .eq('wamid', r.key.id);
          } catch (_) {}
        }
        continue;
      }
      // Baixa a mídia (foto/áudio/vídeo/documento) e guarda no Supabase Storage
      let mediaPath = null, mediaMime = null;
      try {
        const mm = m.message.imageMessage || m.message.audioMessage || m.message.videoMessage
                || m.message.documentMessage || m.message.stickerMessage;
        if (mm && supabase) {
          const buf = await _baileys.downloadMediaMessage(m, 'buffer', {}, {
            logger: _pino({ level: 'silent' }), reuploadRequest: sock.updateMediaMessage });
          mediaMime = (mm.mimetype || 'application/octet-stream').split(';')[0];
          const ext = mediaMime.split('/')[1] || 'bin';
          mediaPath = `qr/${instanceName}/${(m.key.id || Date.now())}.${ext}`;
          const { error: upErr } = await supabase.storage.from('wa-media')
            .upload(mediaPath, buf, { contentType: mediaMime, upsert: true });
          if (upErr) { console.error('Storage upload:', upErr.message); mediaPath = null; }
        }
      } catch (me) { console.error('Download de mídia QR:', me.message); mediaPath = null; }
      // Quando o chat usa o id oculto (@lid), descobre o número REAL do contato:
      // 1) remoteJidAlt (Baileys 7 traz o número real do chat, em qualquer direção),
      // 2) senderPn/participantPn (só em mensagens recebidas — em enviadas seria o SEU número),
      // 3) mapa LID→número interno do Baileys
      let realPn = null, lidJid = null;
      const _rj = String(m.key?.remoteJid || '');
      if (_rj.endsWith('@lid')) {
        lidJid = _rj;
        realPn = m.key?.remoteJidAlt || null;
        if (!realPn && !m.key?.fromMe) realPn = m.key?.senderPn || m.key?.participantPn || null;
        if (!realPn) { try { realPn = await sock.signalRepository?.lidMapping?.getPNForLID?.(_rj) || null; } catch (_) {} }
      } else if (!m.key?.fromMe) {
        realPn = m.key?.senderPn || m.key?.participantPn || null;
      }
      try {
        await axios.post(`http://127.0.0.1:${PORT}/evolution-webhook`, {
          event: 'messages.upsert',
          instance: instanceName,
          data: {
            mediaPath, mediaMime,
            key: m.key,
            senderPn: realPn, // número real já resolvido (ou null se impossível)
            lidJid,           // id oculto original — usado para migrar contatos salvos errados
            pushName: m.pushName || '',
            messageTimestamp: Number(m.messageTimestamp) || Math.floor(Date.now() / 1000),
            message: m.message,
          },
        }, { timeout: 10000 });
      } catch (e) { console.error('WA→webhook interno:', e.message); }
    }
  });
  return sock;
}

// Descobre o "endereço" (JID) REAL do número no WhatsApp — resolve o nono dígito.
// Enviar para a variante errada não dá erro: a mensagem simplesmente não chega.
async function waResolveJid(sock, to) {
  const s = String(to).trim();
  // Endereço já pronto (ex.: id oculto "@lid" ou jid completo) → usa como está
  if (s.endsWith('@lid') || s.endsWith('@s.whatsapp.net')) return s;
  const num = s.replace(/\D/g, '');
  try {
    const r = await sock.onWhatsApp(num);
    if (r && r[0] && r[0].exists && r[0].jid) return r[0].jid;
  } catch (e) { console.warn('onWhatsApp falhou, usando número direto:', e.message); }
  return num + '@s.whatsapp.net';
}

// Qualquer instância QR conectada (usada como "fotógrafo" para todos os contatos)
function anyOpenWaInstance() {
  for (const k in _waSocks) if (_waState[k] === 'open') return k;
  return null;
}

// Instância QR conectada DO MESMO DONO — importante para fotos de perfil:
// por privacidade, a foto de muitos contatos só é visível para o número que
// conversa com eles. Usar a instância de outra conta volta sem foto.
const _instOwnerCache = { ts: 0, map: {} };
async function anyOpenWaInstanceForOwner(owner) {
  if (!owner || !supabase) return null;
  try {
    if (Date.now() - _instOwnerCache.ts > 5 * 60000) {
      const { data } = await supabase.from('accounts').select('evolution_instance, owner').not('evolution_instance', 'is', null);
      _instOwnerCache.map = {};
      (data || []).forEach(a => { if (a.evolution_instance) _instOwnerCache.map[a.evolution_instance] = a.owner || null; });
      _instOwnerCache.ts = Date.now();
    }
  } catch (_) {}
  for (const k in _waSocks) if (_waState[k] === 'open' && _instOwnerCache.map[k] === owner) return k;
  return null;
}

// Busca a foto de perfil do cliente (1x por contato) e guarda no cofre de mídias
async function waFetchAvatar(instanceName, phone, owner) {
  try {
    const sock = _waSocks[instanceName];
    if (!sock || !supabase) return;
    const { data: c } = await supabase.from('contacts').select('avatar')
      .eq('phone', phone).eq('owner', owner || ' ').maybeSingle();
    if (c && c.avatar) return; // já tem foto
    const jid = String(phone).endsWith('@lid') ? String(phone) : String(phone).replace(/\D/g, '') + '@s.whatsapp.net';
    const url = await sock.profilePictureUrl(jid, 'image').catch(() => null);
    if (!url) return; // sem foto ou privacidade
    const img = await axios.get(url, { responseType: 'arraybuffer', timeout: 15000 });
    const p = `qr/avatars/${String(phone).replace(/\W/g, '') || Date.now()}.jpg`;
    const { error: upErr } = await supabase.storage.from('wa-media')
      .upload(p, Buffer.from(img.data), { contentType: 'image/jpeg', upsert: true });
    if (upErr) return;
    await supabase.from('contacts').update({ avatar: p }).eq('phone', phone).eq('owner', owner || ' ');
    console.log(`🖼️ Foto de perfil salva: ${phone}`);
  } catch (_) {}
}

async function waSendText(instanceName, to, text) {
  const sock = _waSocks[instanceName];
  if (!sock || _waState[instanceName] !== 'open') throw new Error('WhatsApp desconectado — gere o QR novamente em Contas');
  const jid = await waResolveJid(sock, to);
  return await sock.sendMessage(jid, { text });
}

// Reconecta as contas QR já cadastradas quando o servidor sobe
async function initEmbeddedWa() {
  if (!WA_EMBEDDED || !_baileys || !supabase) return;
  // Garante o "cofre" de mídias das contas QR (ignora se já existir)
  try {
    const { error: bErr } = await supabase.storage.createBucket('wa-media', { public: false });
    if (!bErr) console.log('🗂️ Bucket wa-media criado');
  } catch (_) {}
  try {
    const { data } = await supabase.from('accounts').select('evolution_instance').eq('type', 'evolution');
    for (const a of data || []) {
      if (!a.evolution_instance) continue;
      waStart(a.evolution_instance).catch(e => console.error('WA boot:', a.evolution_instance, e.message));
      await new Promise(r => setTimeout(r, 1500));
    }
  } catch (e) { console.error('initEmbeddedWa:', e.message); }

  // Varredura de fotos: espera alguma instância QR abrir (re-tenta por ~3 min).
  // Cada contato usa o "fotógrafo" CERTO: a instância da própria conta/dono —
  // por privacidade, a foto pode ser visível só para o número que fala com ele.
  let _sweepTries = 0;
  const _avatarSweep = async () => {
    try {
      if (!anyOpenWaInstance()) {
        if (++_sweepTries < 10) setTimeout(_avatarSweep, 20000);
        return;
      }
      const { data: rows } = await supabase.from('contacts')
        .select('phone, owner, account_id').is('avatar', null)
        .not('last_message_at', 'is', null)
        .order('last_message_at', { ascending: false }).limit(40);
      // Mapa: conta → instância aberta / dono → instância aberta
      const { data: accs } = await supabase.from('accounts').select('id, owner, evolution_instance');
      const instByAcct = {}, instByOwner = {};
      (accs || []).forEach(a => {
        if (a.evolution_instance && _waState[a.evolution_instance] === 'open') {
          instByAcct[a.id] = a.evolution_instance;
          if (!instByOwner[a.owner || ' ']) instByOwner[a.owner || ' '] = a.evolution_instance;
        }
      });
      for (const r of rows || []) {
        const inst = instByAcct[r.account_id] || instByOwner[r.owner || ' '] || anyOpenWaInstance();
        if (!inst) continue;
        await waFetchAvatar(inst, r.phone, r.owner);
        await new Promise(rs => setTimeout(rs, 400)); // ritmo suave, sem parecer robô
      }
      console.log(`🖼️ Varredura de fotos concluída (${(rows || []).length} contatos verificados)`);
    } catch (e) { console.error('Varredura de fotos:', e.message); }
    // Repete a cada 6 horas — pega fotos de contatos novos gastando o mínimo
    setTimeout(_avatarSweep, 6 * 3600000);
  };
  setTimeout(_avatarSweep, 20000);
}
setTimeout(initEmbeddedWa, 2500);

// Envia mensagem via Evolution API
async function sendViaEvolution(instanceName, to, text) {
  if (WA_EMBEDDED) return await waSendText(instanceName, to, text); // motor embutido
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
  if (WA_EMBEDDED) {
    try {
      // Limpa instâncias antigas que nunca parearam (QRs abandonados) com mais de
      // 5 min — menos tentativas simultâneas = menos chance de bloqueio da Meta
      for (const k of Object.keys(_waSocks)) {
        // NUNCA mexe em instância já pareada (conta real reconectando) — só limpa
        // QRs abandonados (nunca pareados) com mais de 5 minutos
        if (!_waRegistered[k] && _waState[k] !== 'open' && Date.now() - (_waCreatedAt[k] || 0) > 5 * 60000) {
          await waCleanupInstance(k);
        }
      }
      _waCreatedAt[instanceName] = Date.now();
      await waStart(instanceName);
      let qr = null;
      for (let i = 0; i < 16 && !qr; i++) { await new Promise(r => setTimeout(r, 500)); qr = qrCache[instanceName] || null; }
      console.log(`Instância embutida criada: ${instanceName}, QR: ${qr ? 'SIM' : 'NAO (polling)'}`);
      return res.json({ success: true, instance: instanceName, qr });
    } catch (e) {
      console.error('WA connect error:', e.message);
      return res.status(500).json({ error: e.message });
    }
  }
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
  if (WA_EMBEDDED) return res.json({ qr: null, code: null, pairingCode: null, raw: { embedded: true } });
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
  if (WA_EMBEDDED) {
    return res.json({ state: _waState[req.params.instance] || 'close', phone: _waPhone[req.params.instance] || null });
  }
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
    .upsert({ name, type: 'evolution', evolution_instance: instance, phone_display: phone || null, phone_number_id: instance, token: '', owner: req.owner || null }, { onConflict: 'phone_number_id' })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  console.log('✅ Conta Evolution salva:', name);
  res.json({ success: true, data });
});

// DELETE /evolution/disconnect/:instance
app.delete('/evolution/disconnect/:instance', async (req, res) => {
  const inst = req.params.instance;
  if (WA_EMBEDDED) {
    try {
      const sock = _waSocks[inst];
      if (sock) {
        try { await sock.logout(); } catch (_) {}
        try { sock.end(undefined); } catch (_) {}
        delete _waSocks[inst];
      }
      delete _waState[inst]; delete _waPhone[inst]; delete qrCache[inst];
      if (supabase) {
        await supabase.from('wa_sessions').delete().eq('instance', inst);
        await supabase.from('accounts').delete().eq('evolution_instance', inst);
      }
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }
  try {
    await axios.delete(`${EVOLUTION_URL}/instance/delete/${inst}`, { headers: evoHdr(), timeout: 10000 });
    if (supabase) await supabase.from('accounts').delete().eq('evolution_instance', inst);
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
      const _rjRaw    = data.key?.remoteJid || '';
      if (_rjRaw.includes('@g.us')) return; // ignora grupos
      if (_rjRaw.includes('@broadcast') || _rjRaw.includes('@newsletter')) return; // ignora status/canais
      // Prefere o número REAL quando o chat usa o id oculto @lid:
      // senderPn já chega resolvido da conexão interna; remoteJidAlt (Baileys 7)
      // vale para qualquer direção; senderPn/participantPn do key só em recebidas
      const chatPn    = data.senderPn || data.key?.remoteJidAlt
                     || (!fromMe ? (data.key?.senderPn || data.key?.participantPn) : null) || null;
      const remoteJid = String(chatPn || _rjRaw);

      let phone       = remoteJid.replace('@s.whatsapp.net', '');
      const isLid     = String(phone).endsWith('@lid');
      const name      = data.pushName || (isLid ? 'Contato (número oculto)' : phone);
      const timestamp = new Date((data.messageTimestamp || Date.now() / 1000) * 1000).toISOString();
      const wamid     = data.key?.id || null;
      const direction = fromMe ? 'outbound' : 'inbound';

      // Extrai conteúdo
      let content = fromMe ? '[Mensagem enviada]' : '[Mensagem recebida]', type = 'text';
      const msg = data.message || {};
      if      (msg.conversation)          { content = msg.conversation; type = 'text'; }
      else if (msg.extendedTextMessage)   { content = msg.extendedTextMessage.text || ''; type = 'text'; }
      else if (msg.imageMessage)          { content = msg.imageMessage.caption || '[Imagem]'; type = 'image'; }
      else if (msg.audioMessage || msg.pttMessage) {
        const secsEv = (msg.audioMessage?.seconds || msg.pttMessage?.seconds) || 0;
        content = '🎤 Mensagem de voz' + (secsEv ? ` (${_fmtDur(secsEv)})` : '');
        type = 'audio';
      }
      else if (msg.videoMessage)          { content = msg.videoMessage.caption || '[Vídeo]'; type = 'video'; }
      else if (msg.documentMessage)       { content = `[Documento: ${msg.documentMessage.fileName || 'arquivo'}]`; type = 'document'; }
      else if (msg.stickerMessage)        { content = '[Figurinha]'; type = 'sticker'; }

      // Busca account_id + dono (owner) — sem o owner a mensagem não aparece no CRM
      let accountId = null;
      let ownerEmail = null;
      if (supabase && instanceName) {
        const { data: acc } = await supabase.from('accounts').select('id, owner').eq('evolution_instance', instanceName).maybeSingle();
        if (acc) { accountId = acc.id; ownerEmail = acc.owner || null; }
      }

      // Unifica a conversa se o contato já existe com/sem o nono dígito
      phone = await resolveExistingPhone(phone, ownerEmail);

      // Se este contato foi salvo antes com o id oculto (@lid), migra para o número real
      if (supabase && data.lidJid && phone && !String(phone).endsWith('@lid')) {
        try {
          const { data: lidC } = await supabase.from('contacts').select('id').eq('phone', data.lidJid).eq('owner', ownerEmail || ' ').maybeSingle();
          if (lidC) {
            const { data: realC } = await supabase.from('contacts').select('id').eq('phone', phone).eq('owner', ownerEmail || ' ').maybeSingle();
            if (realC) await supabase.from('contacts').delete().eq('id', lidC.id);  // já existe com o número certo — remove o duplicado @lid
            else await supabase.from('contacts').update({ phone }).eq('id', lidC.id); // corrige o número do contato
            await supabase.from('messages').update({ phone }).eq('phone', data.lidJid); // histórico acompanha
            console.log(`🔁 Contato @lid migrado para o número real: ${data.lidJid} → ${phone}`);
          }
        } catch (e) { console.error('Migração @lid:', e.message); }
      }

      if (supabase) {
        // Dedup: evita duplicar mensagens já salvas (ex.: o eco das enviadas pelo próprio CRM).
        // IMPORTANTE: filtra também pelo telefone da conversa — quando DOIS números
        // conectados no VETRA conversam entre si, a mensagem chega com o MESMO wamid
        // nos dois lados; sem o filtro, o lado que recebia era descartado como "eco"
        // e a mensagem nunca aparecia para quem recebeu.
        if (wamid) {
          const { data: exists } = await supabase.from('messages').select('id').eq('wamid', wamid).eq('phone', phone).maybeSingle();
          if (exists) return;
        }

        const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
        const contactData = { phone, last_message_at: timestamp, last_message_preview: preview, last_message_direction: direction };
        if (ownerEmail) contactData.owner = ownerEmail;
        // Estado atual do contato (para decidir NOME e NÚMERO sem sobrescrever indevidamente)
        const { data: existC } = await supabase.from('contacts').select('id, account_id, name').eq('phone', phone).eq('owner', ownerEmail || ' ').maybeSingle();
        // NOME: só define na CRIAÇÃO do contato — depois RESPEITA o nome editado no CRM.
        // Exceção: se o nome atual é só o número/id (nunca foi personalizado), adota o
        // nome público do WhatsApp (pushName) quando a pessoa escreve.
        if (!existC) {
          contactData.name = !fromMe ? name : (isLid ? 'Contato (número oculto)' : phone);
        } else if (!fromMe && data.pushName) {
          const atual = String(existC.name || '');
          // Só adota o pushName se o nome atual nunca foi personalizado
          if (!atual || atual === phone || atual === 'Contato (número oculto)') contactData.name = name;
        }
        // NÚMERO da conversa: seu envio (fromMe) fixa no número usado; recebida só define se ainda não houver.
        if (accountId && (fromMe || !existC || existC.account_id == null)) contactData.account_id = accountId;
        // Você respondeu pelo CELULAR/WhatsApp Web → a conversa deixa de ser "não lida"
        // no CRM (mensagens enviadas pelo próprio CRM não passam por aqui — dedupe acima)
        if (fromMe) { contactData.unread_count = 0; contactData.first_unread_at = null; }
        const { error: cErr } = await supabase.from('contacts').upsert(contactData, { onConflict: 'owner,phone' });
        if (cErr) console.error('❌ Evolution: erro ao salvar contato:', cErr.message);

        // Foto de perfil do cliente (busca em segundo plano, só se ainda não tiver)
        if (!fromMe) waFetchAvatar(instanceName, phone, ownerEmail).catch(() => {});

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
        if (data.mediaPath) { msgData.media_id = data.mediaPath; msgData.media_mime_type = data.mediaMime || null; }
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
