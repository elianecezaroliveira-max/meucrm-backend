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
    if (body.object !== "whatsapp_business_account" || !body.entry?.[0]?.changes?.[0]?.value?.messages) {
      return res.sendStatus(200);
    }
    const value = body.entry[0].changes[0].value;
    const message = value.messages[0];
    const contact = value.contacts?.[0];
    const from = message.from;
    const name = contact?.profile?.name || "Desconhecido";
    const timestamp = new Date(parseInt(message.timestamp) * 1000).toISOString();

    const phoneNumberId = value.metadata?.phone_number_id;
    let accountId = null;
    if (supabase && phoneNumberId) {
      const { data: account } = await supabase
        .from("accounts").select("id").eq("phone_number_id", phoneNumberId).single();
      if (account) accountId = account.id;
    }

    let content = "";
    const type = message.type;
    if (type === "text") content = message.text.body;
    else if (type === "image") content = "[Imagem recebida]";
    else if (type === "audio") content = "[Áudio recebido]";
    else if (type === "document") content = "[Documento recebido]";
    else if (type === "video") content = "[Vídeo recebido]";
    else content = `[Mensagem do tipo: ${type}]`;

    if (supabase) {
      await supabase.from("contacts").upsert(
        { phone: from, name, last_message_at: timestamp, account_id: accountId },
        { onConflict: "phone" }
      );
      await supabase.from("messages").insert({
        phone: from, content, type, direction: "inbound", timestamp, account_id: accountId,
      });
    }
    res.sendStatus(200);
  } catch (err) {
    console.error("Erro webhook:", err.message);
    res.sendStatus(500);
  }
});

// ── Embedded Signup: recebe código do Facebook e salva conta automaticamente ──
app.post("/auth/whatsapp", async (req, res) => {
  const { code } = req.body;
  if (!code) return res.status(400).json({ error: "Código não informado" });
  if (!APP_ID || !APP_SECRET) return res.status(500).json({ error: "APP_ID e APP_SECRET não configurados no Railway" });

  try {
    // 1. Troca código pelo access token
    const tokenRes = await axios.get("https://graph.facebook.com/v19.0/oauth/access_token", {
      params: { client_id: APP_ID, client_secret: APP_SECRET, code }
    });
    const userToken = tokenRes.data.access_token;

    // 2. Busca empresas e WABAs do usuário
    const bizRes = await axios.get("https://graph.facebook.com/v19.0/me/businesses", {
      params: { access_token: userToken, fields: "id,name,owned_whatsapp_business_accounts{id,name}" }
    });

    const businesses = bizRes.data.data || [];
    const savedAccounts = [];

    for (const biz of businesses) {
      const wabas = biz.owned_whatsapp_business_accounts?.data || [];
      for (const waba of wabas) {
        // 3. Busca números de telefone
        const phonesRes = await axios.get(`https://graph.facebook.com/v19.0/${waba.id}/phone_numbers`, {
          params: { access_token: userToken, fields: "id,display_phone_number,verified_name" }
        });
        const phones = phonesRes.data.data || [];

        for (const phone of phones) {
          // 4. Inscreve WABA no webhook
          try {
            await axios.post(`https://graph.facebook.com/v19.0/${waba.id}/subscribed_apps`, {},
              { params: { access_token: userToken } }
            );
          } catch(e) {
            console.log("Aviso webhook:", e.response?.data?.error?.message);
          }

          // 5. Salva conta no Supabase
          const accountData = {
            name: phone.verified_name || waba.name || biz.name,
            phone_number_id: phone.id,
            phone_display: phone.display_phone_number,
            token: userToken,
            waba_id: waba.id,
          };

          if (supabase) {
            const { data, error } = await supabase
              .from("accounts")
              .upsert(accountData, { onConflict: "phone_number_id" })
              .select().single();
            if (!error) savedAccounts.push(data);
          } else {
            savedAccounts.push(accountData);
          }
        }
      }
    }

    if (savedAccounts.length === 0) {
      return res.status(400).json({ error: "Nenhum número de WhatsApp encontrado nesta conta do Facebook." });
    }

    res.json({ success: true, accounts: savedAccounts });
  } catch (err) {
    console.error("Erro auth:", err.response?.data || err.message);
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
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("phone_number_id, token").eq("id", account_id).single();
    if (account) { phoneNumberId = account.phone_number_id; token = account.token; }
  }
  if (!phoneNumberId || !token)
    return res.status(400).json({ error: "Conta não encontrada." });

  try {
    const response = await axios.post(
      `https://graph.facebook.com/v19.0/${phoneNumberId}/messages`,
      { messaging_product: "whatsapp", to, type: "text", text: { body: message } },
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );
    if (supabase) {
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id },
        { onConflict: "phone" }
      );
      await supabase.from("messages").insert({
        phone: to, content: message, type: "text", direction: "outbound",
        timestamp: new Date().toISOString(), account_id,
      });
    }
    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem" });
  }
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

app.listen(PORT, () => console.log(`🚀 MeuCRM na porta ${PORT}`));
