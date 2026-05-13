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
const PORT = process.env.PORT || 3000;

let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log("✅ Supabase conectado!");
} else {
  console.log("⚠️ Supabase não configurado.");
}

// ── Teste ──
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

// ── Receber mensagens (multi-conta) ──
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

    // Identifica qual conta recebeu a mensagem
    const phoneNumberId = value.metadata?.phone_number_id;
    let accountId = null;

    if (supabase && phoneNumberId) {
      const { data: account } = await supabase
        .from("accounts")
        .select("id")
        .eq("phone_number_id", phoneNumberId)
        .single();
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

    console.log(`📩 [Conta: ${accountId || "desconhecida"}] ${name} (${from}): ${content}`);

    if (supabase) {
      await supabase.from("contacts").upsert(
        { phone: from, name, last_message_at: timestamp, account_id: accountId },
        { onConflict: "phone" }
      );
      await supabase.from("messages").insert({
        phone: from,
        content,
        type,
        direction: "inbound",
        timestamp,
        account_id: accountId,
      });
    }

    res.sendStatus(200);
  } catch (err) {
    console.error("Erro webhook:", err.message);
    res.sendStatus(500);
  }
});

// ── Enviar mensagem (usa credenciais da conta correta) ──
app.post("/send", async (req, res) => {
  const { to, message, account_id } = req.body;
  if (!to || !message) return res.status(400).json({ error: "Informe 'to' e 'message'" });

  let phoneNumberId, token;

  // Busca credenciais da conta selecionada
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts")
      .select("phone_number_id, token")
      .eq("id", account_id)
      .single();
    if (account) {
      phoneNumberId = account.phone_number_id;
      token = account.token;
    }
  }

  if (!phoneNumberId || !token) {
    return res.status(400).json({ error: "Conta não encontrada. Informe um account_id válido." });
  }

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
        phone: to,
        content: message,
        type: "text",
        direction: "outbound",
        timestamp: new Date().toISOString(),
        account_id,
      });
    }

    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem" });
  }
});

// ── Listar contas ──
app.get("/accounts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("accounts")
    .select("id, name, phone_number_id, created_at")
    .order("created_at", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// ── Adicionar conta ──
app.post("/accounts", async (req, res) => {
  const { name, phone_number_id, token } = req.body;
  if (!name || !phone_number_id || !token)
    return res.status(400).json({ error: "Informe name, phone_number_id e token" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });

  const { data, error } = await supabase
    .from("accounts")
    .insert({ name, phone_number_id, token })
    .select()
    .single();
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

// ── Listar contatos (por conta) ──
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
    .from("messages")
    .select("*")
    .eq("phone", req.params.phone)
    .order("timestamp", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.listen(PORT, () => console.log(`🚀 MeuCRM na porta ${PORT}`));
