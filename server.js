const express = require("express");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const cors = require("cors");

const app = express();
app.use(express.json());
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

app.get("/", (req, res) => res.send("✅ MeuCRM Backend funcionando!"));

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

// ── Receber mensagens ──
app.post("/webhook", async (req, res) => {
  try {
    const body = req.body;

    // Log para debug - mostra o que chegou
    console.log("📩 Webhook recebido:", JSON.stringify(body).substring(0, 300));

    if (body.object !== "whatsapp_business_account") {
      console.log("⚠️ Objeto ignorado:", body.object);
      return res.sendStatus(200);
    }

    const changes = body.entry?.[0]?.changes;
    if (!changes?.length) return res.sendStatus(200);

    for (const change of changes) {
      const value = change.value;

      // Handle status updates (read receipts)
      if (value?.statuses?.length && supabase) {
        for (const st of value.statuses) {
          const { id: wamid, status } = st;
          if (wamid && ['sent','delivered','read','failed'].includes(status)) {
            await supabase.from("messages").update({ status }).eq("wamid", wamid);
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

      console.log(`📨 Mensagem de ${name} (${from}) via número ${phoneNumberId}`);

      // Busca account_id (pode ser null se não cadastrado)
      let accountId = null;
      if (supabase && phoneNumberId) {
        const { data: account, error: accErr } = await supabase
          .from("accounts").select("id").eq("phone_number_id", phoneNumberId).maybeSingle();
        if (accErr) console.error("❌ Erro ao buscar conta:", accErr.message);
        if (account) {
          accountId = account.id;
          console.log("✅ Conta encontrada:", accountId);
        } else {
          console.log("⚠️ Nenhuma conta com phone_number_id:", phoneNumberId, "- salvando sem account_id");
        }
      }

      // Extrai conteúdo da mensagem
      let content = "";
      const type = message.type;
      if (type === "text") content = message.text?.body || "";
      else if (type === "image") content = "[Imagem recebida]";
      else if (type === "audio") content = "[Áudio recebido]";
      else if (type === "document") content = "[Documento recebido]";
      else if (type === "video") content = "[Vídeo recebido]";
      else content = `[Mensagem do tipo: ${type}]`;

      if (supabase) {
        // Salva contato
        const contactData = { phone: from, name, last_message_at: timestamp };
        if (accountId) contactData.account_id = accountId; // só inclui se não for null

        const { error: contactErr } = await supabase
          .from("contacts")
          .upsert(contactData, { onConflict: "phone" });

        if (contactErr) {
          console.error("❌ Erro ao salvar contato:", contactErr.message, contactErr.details);
        } else {
          console.log("✅ Contato salvo:", from);
        }

        // Salva mensagem
        const messageData = {
          phone: from,
          content,
          type,
          direction: "inbound",
          timestamp,
        };
        if (accountId) messageData.account_id = accountId; // só inclui se não for null

        const { error: msgErr } = await supabase.from("messages").insert(messageData);

        if (msgErr) {
          console.error("❌ Erro ao salvar mensagem:", msgErr.message, msgErr.details);
        } else {
          console.log("✅ Mensagem salva:", content.substring(0, 50));
        }
      }
    }

    res.sendStatus(200);
  } catch (err) {
    console.error("❌ Erro no webhook:", err.message);
    res.sendStatus(500);
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
    .from("accounts").select("id, name, phone_number_id, phone_display, created_at")
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
    .from("accounts").insert({ name, phone_number_id, token }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

// ── Remover conta ──
app.delete("/accounts/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("accounts").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Enviar mensagem ──
app.post("/send", async (req, res) => {
  const { to, message, account_id } = req.body;
  if (!to || !message) return res.status(400).json({ error: "Informe 'to' e 'message'" });

  let phoneNumberId, token;

  // 1. Tenta buscar conta do banco de dados pelo account_id
  if (supabase && account_id) {
    const { data: account, error: accErr } = await supabase
      .from("accounts").select("phone_number_id, token").eq("id", account_id).single();
    if (accErr) console.error("❌ Erro ao buscar conta para envio:", accErr.message);
    if (account) { phoneNumberId = account.phone_number_id; token = account.token; }
  }

  // 2. Fallback: usa variáveis de ambiente (PHONE_NUMBER_ID + WHATSAPP_TOKEN)
  if (!phoneNumberId || !token) {
    phoneNumberId = process.env.PHONE_NUMBER_ID;
    token = process.env.WHATSAPP_TOKEN;
    if (phoneNumberId && token) {
      console.log("⚠️ Conta não encontrada no banco — usando credenciais das variáveis de ambiente");
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
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId },
        { onConflict: "phone" }
      );
      const wamid = response.data?.messages?.[0]?.id || null;
      const { error: msgErr } = await supabase.from("messages").insert({
        phone: to, content: message, type: "text", direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: 'sent', wamid,
      });
      if (msgErr) {
        console.error("❌ Erro ao salvar mensagem enviada:", msgErr.message, msgErr.details);
      } else {
        console.log("✅ Mensagem enviada salva no banco:", message.substring(0, 50));
      }
    }
    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("❌ Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem", detail: err.response?.data });
  }
});

// ── Enviar mídia (imagem, PDF, vídeo, etc.) ──
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
    // 1. Faz upload da mídia para a Meta
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
    console.log("✅ Mídia enviada para Meta, id:", mediaId);

    // 2. Determina o tipo de mensagem WhatsApp
    let msgType = "document";
    if (mimeType.startsWith("image/")) msgType = "image";
    else if (mimeType.startsWith("video/")) msgType = "video";
    else if (mimeType.startsWith("audio/")) msgType = "audio";

    const mediaObj = { id: mediaId };
    if (msgType === "document") mediaObj.filename = fileName;

    // 3. Envia a mensagem de mídia
    await axios.post(
      `https://graph.facebook.com/v23.0/${phoneNumberId}/messages`,
      { messaging_product: "whatsapp", to, type: msgType, [msgType]: mediaObj },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    // 4. Salva no Supabase
    if (supabase) {
      const safeAccountId = account_id || null;
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId },
        { onConflict: "phone" }
      );
      const mediaWamid = null; // media endpoint doesn't return wamid in same way
      await supabase.from("messages").insert({
        phone: to, content: `[${msgType === "image" ? "Imagem" : msgType === "video" ? "Vídeo" : msgType === "audio" ? "Áudio" : "Documento"}: ${fileName}]`,
        type: msgType, direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: 'sent', wamid: mediaWamid,
      });
    }
    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erro ao enviar mídia:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mídia", detail: err.response?.data });
  }
});

// ── Tags por contato ──
app.put("/contacts/:phone/tags", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { tags } = req.body;
  if (!Array.isArray(tags)) return res.status(400).json({ error: "tags deve ser array" });
  const { error } = await supabase
    .from("contacts").update({ tags }).eq("phone", req.params.phone);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Anotações por contato ──
app.get("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.json({ notes: "" });
  const { data, error } = await supabase
    .from("contacts").select("notes").eq("phone", req.params.phone).maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ notes: data?.notes || "" });
});

app.put("/contacts/:phone/notes", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { notes } = req.body;
  const { error } = await supabase
    .from("contacts")
    .update({ notes: notes ?? "" })
    .eq("phone", req.params.phone);
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
    .upsert({ phone: cleanPhone, name, account_id: account_id || null, last_message_at: new Date().toISOString() }, { onConflict: "phone" })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

// ── Importar lista de contatos ──
app.post("/contacts/import", async (req, res) => {
  const { contacts, account_id } = req.body;
  if (!contacts || !Array.isArray(contacts)) return res.status(400).json({ error: "Lista inválida" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const toInsert = contacts
    .map(c => ({ phone: String(c.phone || '').replace(/\D/g, ''), name: c.name || 'Desconhecido', account_id: account_id || null, last_message_at: new Date().toISOString() }))
    .filter(c => c.phone.length >= 8);
  if (!toInsert.length) return res.status(400).json({ error: "Nenhum contato válido encontrado" });
  const { error } = await supabase.from("contacts").upsert(toInsert, { onConflict: "phone" });
  if (error) return res.status(500).json({ error: error.message });
  console.log(`✅ ${toInsert.length} contatos importados`);
  res.json({ success: true, count: toInsert.length });
});

// ── Listar contatos ──
app.get("/contacts", async (req, res) => {
  if (!supabase) return res.json([]);
  let query = supabase.from("contacts").select("*").order("last_message_at", { ascending: false });
  if (req.query.account_id) query = query.eq("account_id", req.query.account_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ── Mensagens de um contato ──
app.get("/messages/:phone", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("messages").select("*").eq("phone", req.params.phone)
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
app.delete("/templates/:template_id", async (req, res) => {
  const { account_id } = req.query;
  if (!supabase || !account_id) return res.status(400).json({ error: "account_id obrigatório" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("token, waba_id").eq("id", account_id).single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  try {
    await axios.delete(`https://graph.facebook.com/v23.0/${req.params.template_id}`, {
      params: { access_token: account.token },
    });
    res.json({ success: true });
  } catch (err) {
    console.error("❌ Erro ao deletar template:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao deletar template" });
  }
});

// ── Enviar template ──
app.post("/send-template", async (req, res) => {
  const { to, account_id, template_name, language_code, components } = req.body;
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
    await supabase.from("contacts").upsert(
      { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId },
      { onConflict: "phone" }
    );
    await supabase.from("messages").insert({
      phone: to, content: `[Template: ${template_name}]`, type: "template",
      direction: "outbound", timestamp: new Date().toISOString(), account_id: safeAccountId,
    });
    console.log("✅ Template enviado:", template_name, "→", to);
    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("❌ Erro ao enviar template:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data?.error?.message || "Erro ao enviar template" });
  }
});

// ── Pipeline / Kanban ──

// Listar estágios
app.get("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("pipeline_stages").select("*").order("position", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// Criar estágio
app.post("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const { data, error } = await supabase
    .from("pipeline_stages").insert({ name, position: position || 0 }).select().single();
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
    .from("pipeline_stages").update(updates).eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Excluir estágio (move leads para sem-status)
app.delete("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  await supabase.from("contacts").update({ stage_id: null }).eq("stage_id", req.params.id);
  const { error } = await supabase.from("pipeline_stages").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Listar contatos (com stage_id)
app.get("/contacts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("contacts").select("phone, name, account_id, stage_id, tags, last_message_at")
    .order("last_message_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// Mover lead para estágio
app.put("/contacts/:phone/stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { stage_id } = req.body;
  const { error } = await supabase
    .from("contacts").update({ stage_id: stage_id || null }).eq("phone", req.params.phone);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Bulk actions ──
app.put("/contacts/bulk-stage", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones, stage_id } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  const { error } = await supabase.from("contacts")
    .update({ stage_id: stage_id || null })
    .in("phone", phones);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.delete("/contacts/bulk-delete", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  await supabase.from("messages").delete().in("phone", phones);
  const { error } = await supabase.from("contacts").delete().in("phone", phones);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.put("/contacts/bulk-tags", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones, tags } = req.body;
  if (!Array.isArray(phones) || !Array.isArray(tags)) return res.status(400).json({ error: "phones e tags obrigatórios" });
  // For each phone, merge new tags with existing
  for (const phone of phones) {
    const { data: contact } = await supabase.from("contacts").select("tags").eq("phone", phone).single();
    const merged = Array.from(new Set([...(contact?.tags || []), ...tags]));
    await supabase.from("contacts").update({ tags: merged }).eq("phone", phone);
  }
  res.json({ success: true });
});

app.listen(PORT, () => console.log(`🚀 MeuCRM na porta ${PORT}`));
