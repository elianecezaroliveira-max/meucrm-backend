// =============================================
//  MeuCRM — Servidor Backend
//  Node.js + WhatsApp Business API + Supabase
// =============================================

const express = require("express");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(express.json());

// ── Variáveis de ambiente (configure no Railway) ──
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const PORT = process.env.PORT || 3000;

// ── Cliente do Supabase (só conecta se as variáveis existirem) ──
let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log("✅ Supabase conectado!");
} else {
  console.log("⚠️ Supabase não configurado — mensagens não serão salvas ainda.");
}

// =============================================
//  ROTA 1 — Verificação do Webhook pela Meta
// =============================================
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    console.log("✅ Webhook verificado com sucesso!");
    res.status(200).send(challenge);
  } else {
    console.log("❌ Token de verificação incorreto.");
    res.sendStatus(403);
  }
});

// =============================================
//  ROTA 2 — Receber mensagens do WhatsApp
// =============================================
app.post("/webhook", async (req, res) => {
  try {
    const body = req.body;

    if (
      body.object !== "whatsapp_business_account" ||
      !body.entry?.[0]?.changes?.[0]?.value?.messages
    ) {
      return res.sendStatus(200);
    }

    const value = body.entry[0].changes[0].value;
    const message = value.messages[0];
    const contact = value.contacts?.[0];

    const from = message.from;
    const name = contact?.profile?.name || "Desconhecido";
    const timestamp = new Date(parseInt(message.timestamp) * 1000).toISOString();

    let content = "";
    let type = message.type;

    if (type === "text") {
      content = message.text.body;
    } else if (type === "image") {
      content = "[Imagem recebida]";
    } else if (type === "audio") {
      content = "[Áudio recebido]";
    } else if (type === "document") {
      content = "[Documento recebido]";
    } else if (type === "video") {
      content = "[Vídeo recebido]";
    } else {
      content = `[Mensagem do tipo: ${type}]`;
    }

    console.log(`📩 Mensagem de ${name} (${from}): ${content}`);

    if (supabase) {
      const { error: contactError } = await supabase
        .from("contacts")
        .upsert({ phone: from, name: name }, { onConflict: "phone" });

      if (contactError) {
        console.error("Erro ao salvar contato:", contactError.message);
      }

      const { error: msgError } = await supabase.from("messages").insert({
        phone: from,
        content: content,
        type: type,
        direction: "inbound",
        timestamp: timestamp,
      });

      if (msgError) {
        console.error("Erro ao salvar mensagem:", msgError.message);
      }
    }

    res.sendStatus(200);
  } catch (err) {
    console.error("Erro no webhook:", err.message);
    res.sendStatus(500);
  }
});

// =============================================
//  ROTA 3 — Enviar mensagem de texto
// =============================================
app.post("/send", async (req, res) => {
  const { to, message } = req.body;

  if (!to || !message) {
    return res.status(400).json({ error: "Informe 'to' e 'message'" });
  }

  try {
    const response = await axios.post(
      `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to: to,
        type: "text",
        text: { body: message },
      },
      {
        headers: {
          Authorization: `Bearer ${WHATSAPP_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log(`📤 Mensagem enviada para ${to}: ${message}`);

    if (supabase) {
      await supabase.from("messages").insert({
        phone: to,
        content: message,
        type: "text",
        direction: "outbound",
        timestamp: new Date().toISOString(),
      });
    }

    res.json({ success: true, data: response.data });
  } catch (err) {
    console.error("Erro ao enviar:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mensagem" });
  }
});

// ── Rota de teste ──
app.get("/", (req, res) => {
  res.send("✅ MeuCRM Backend está funcionando!");
});

// ── Inicia o servidor ──
app.listen(PORT, () => {
  console.log(`🚀 MeuCRM rodando na porta ${PORT}`);
});
