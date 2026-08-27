const express = require("express");
const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");
const cors = require("cors");

const app = express();
app.set('trust proxy', true); // Railway fica atrás de proxy: req.ip = IP real
app.use(express.json({ limit: '30mb', verify: (req, _res, buf) => { req.rawBody = buf; } })); // rawBody = assinatura do webhook da Meta
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
    // 🔒 CADA E-MAIL É UMA CONTA SEPARADA. Só entra na conta de outra pessoa quem
    // estiver na lista de EQUIPE (owner_aliases), adicionada de propósito pela dona
    // da conta. O antigo "todos caem na conta principal" (owner_default) NÃO existe
    // mais — era o que fazia um cliente enxergar (e disparar) a conta de outro.
    if (email) {
      try {
        const aliases = JSON.parse(_settings['owner_aliases'] || '{}');
        if (aliases[email]) email = String(aliases[email]).toLowerCase();
      } catch (_) {}
      _tokenOwner[tok] = { email, ts: Date.now() };
    }
    return email;
  } catch (e) { return null; }
}
app.use(async (req, res, next) => {
  try { req.owner = await resolveOwner(req); } catch (_) { req.owner = null; }
  // Integração externa: token de LONGA DURAÇÃO no cabeçalho X-Api-Token
  // identifica o dono (para n8n, Zapier, planilhas e outras ferramentas)
  if (!req.owner) {
    const t = req.headers['x-api-token'];
    if (t) {
      for (const k in _settings) {
        if (k.startsWith('api_token::') && _settings[k] === t) {
          const ow = k.slice('api_token::'.length);
          req.owner = ow === ' ' ? null : ow;
          break;
        }
      }
    }
  }
  next();
});

// 🛡️ REDE DE SEGURANÇA: uma falha inesperada dentro de uma rota (ex.: dado
// estranho vindo de fora) só vira log — nunca derruba o CRM inteiro.
process.on('unhandledRejection', (e) => console.error('⚠️ Falha não tratada:', (e && e.message) || e));
process.on('uncaughtException', (e) => console.error('⚠️ Erro não tratado:', (e && e.stack) || e));
// Exige estar logado (ou usar o token de integração) — usada nas rotas sensíveis
// Decodifica sem quebrar: "%zz" derrubava a rota (e o processo)
function _decSeguro(v) { try { return decodeURIComponent(String(v == null ? '' : v)); } catch (_) { return String(v == null ? '' : v); } }
function _exigeLogin(req, res) {
  if (req.owner) return true;
  res.status(401).json({ error: 'Faça login no CRM' });
  return false;
}
app.get("/", (req, res) => res.send("✅ VETRA Backend funcionando!"));
// Diagnóstico: qual versão do servidor está NO AR (confere se o Railway publicou)
const SERVER_VER = 228;
// Diagnóstico de CONTAS: diz (sem expor e-mails) se este servidor está com o
// "login compartilhado" ligado — nesse modo TODOS que entram viram a MESMA conta
function _contasCompartilhadas() {
  let equipe = 0;
  try { equipe = Object.keys(JSON.parse(_settings['owner_aliases'] || '{}')).length; } catch (_) {}
  return {
    login_compartilhado: false, // NUNCA mais: cada e-mail é uma conta
    config_antiga_no_banco: !!_settings['owner_default'], // se true, está ignorada
    membros_de_equipe: equipe
  };
}
app.get('/versao', async (req, res) => {
  let presCount = 0, presKeys = [];
  try { presKeys = Object.keys(_waPresence || {}); presCount = presKeys.length; } catch (_) {}
  // 🗄️ Quantas cópias de 6 meses já existem no cofre (diagnóstico do arquivamento)
  let copias = null;
  try {
    if (supabase) {
      let tot = 0;
      for (let p = 0; p < 10; p++) {
        const { data: its } = await supabase.storage.from('wa-media').list('api', { limit: 100, offset: p * 100 });
        if (!its || !its.length) break;
        tot += its.length;
        if (its.length < 100) break;
      }
      copias = tot;
    }
  } catch (_) {}
  res.json({ contas: _contasCompartilhadas(), server: SERVER_VER, presencas: presCount, exemplos: presKeys.slice(0, 3),
    copias_6meses: copias, cofre_ultimo_ok: _cofreUltimoOk, cofre_ultimo_erro: _cofreUltimoErro,
    // 📦 Espaço do Storage (última medição da faxina) — o plano grátis do Supabase dá 1 GB
    storage_mb: (_espacoCache ? _espacoCache.mb : null),
    storage_teto_mb: COFRE_TETO_MB,
    storage_medido_em: (_espacoCache ? new Date(_espacoCache.ts).toISOString() : null),
    storage_cheio: _cofreCheio });
});

// 🩺 RAIO-X de um arquivo que não abre: mostra se há cópia no cofre e o que a
// Meta responde para CADA conta. Use a mesma URL do arquivo trocando
// /media-proxy/ por /midia-diag/ (precisa estar logada no CRM).
app.get('/midia-diag/:mediaId', async (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM para usar o diagnóstico' });
  if (!supabase) return res.status(500).json({ error: 'Supabase indisponível' });
  const { mediaId } = req.params;
  const saida = { mediaId, copia_no_cofre: false, contas: [] };
  try {
    const { data: b } = await supabase.storage.from('wa-media').download('api/' + mediaId);
    saida.copia_no_cofre = !!b;
  } catch (_) {}
  try {
    const { data: accs } = await supabase.from('accounts').select('id, name, token').not('token', 'is', null).eq('owner', req.owner);
    for (const a of (accs || [])) {
      try {
        const r = await axios.get(`https://graph.facebook.com/v23.0/${mediaId}`, {
          headers: { Authorization: `Bearer ${a.token}` }, timeout: 15000
        });
        saida.contas.push({ conta: a.name || a.id, ok: true, tem_url: !!r.data?.url });
      } catch (e) {
        saida.contas.push({ conta: a.name || a.id, ok: false, meta_disse: e.response?.data?.error?.message || e.message });
      }
    }
  } catch (e) { saida.erro = e.message; }
  res.json(saida);
});

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
// Espelha o status na PRÉVIA da conversa (tiques na lista) — só se for a última mensagem
// 🔢 Contador de não lidas à prova de mensagens simultâneas.
// Quando o lead manda vários arquivos de uma vez, os avisos chegam juntos e
// todos liam o MESMO valor antes de somar 1 — o total saía menor. Aqui cada
// telefone tem uma fila: uma soma espera a outra terminar.
const _filaUnread = {};
async function _somaNaoLida(phone, owner, timestamp) {
  if (!supabase) return;
  const chave = (owner || ' ') + '|' + phone;
  const anterior = _filaUnread[chave] || Promise.resolve();
  const agora = anterior.catch(() => {}).then(async () => {
    try {
      const { data: c } = await supabase.from('contacts').select('unread_count')
        .eq('phone', phone).eq('owner', owner || ' ').maybeSingle();
      const atual = c?.unread_count || 0;
      const upd = { unread_count: atual + 1 };
      if (atual === 0) upd.first_unread_at = timestamp;
      await supabase.from('contacts').update(upd).eq('phone', phone).eq('owner', owner || ' ');
    } catch (e) { console.error('Contador de não lidas:', e.message); }
  });
  _filaUnread[chave] = agora;
  await agora;
  if (_filaUnread[chave] === agora) delete _filaUnread[chave]; // limpa a fila terminada
}

async function _mirrorContactStatus(wamid, status) {
  try {
    // SÓ mensagens ENVIADAS por nós. Quando o CRM marca a mensagem DO LEAD como
    // lida, a Meta devolve um "read" com o id DELA — isso pintava de azul as
    // minhas mensagens (inclusive as do bot) sem o lead ter visto nada.
    const { data: rows } = await supabase.from('messages').select('phone, owner, timestamp')
      .eq('wamid', wamid).eq('direction', 'outbound').limit(3);
    const lower = _ST_RANK[status] !== undefined ? Object.keys(_ST_RANK).filter(s => _ST_RANK[s] < _ST_RANK[status]) : null;
    for (const r of (rows || [])) {
      let q = supabase.from('contacts').update({ last_message_status: status })
        .eq('phone', r.phone).eq('last_message_direction', 'outbound')
        .lte('last_message_at', r.timestamp);
      q = r.owner ? q.eq('owner', r.owner) : q.is('owner', null);
      if (lower) q = q.or('last_message_status.is.null,last_message_status.in.(' + lower.join(',') + ')');
      await q; // se a coluna ainda não existir no banco, só retorna erro silencioso

      // Semântica do WhatsApp: LEU uma mensagem = leu TODAS as anteriores
      // (o recibo muitas vezes vem só para a última — as bolhas antigas ficavam
      //  cinza enquanto a prévia ficava azul; isto elimina a divergência)
      if (status === 'read' || status === 'delivered') {
        try {
          const abaixo = status === 'read' ? 'pending,sent,delivered' : 'pending,sent';
          let q2 = supabase.from('messages').update({ status })
            .eq('phone', r.phone).eq('direction', 'outbound')
            .lt('timestamp', r.timestamp)
            .or('status.is.null,status.in.(' + abaixo + ')');
          q2 = r.owner ? q2.eq('owner', r.owner) : q2.is('owner', null);
          await q2;
        } catch (_) {}
      }
    }
  } catch (_) {}
}
async function updateMsgStatus(wamid, upd) {
  if (!supabase || !wamid || !upd?.status) return;
  // Tique de entrega/leitura vale SÓ para mensagem enviada por mim (outbound)
  if (upd.status === 'failed') { await supabase.from('messages').update(upd).eq('wamid', wamid).eq('direction', 'outbound'); _mirrorContactStatus(wamid, 'failed'); return; }
  const rank = _ST_RANK[upd.status];
  if (rank === undefined) return;
  const lower = Object.keys(_ST_RANK).filter(s => _ST_RANK[s] < rank);
  // Grava também O HORÁRIO da entrega/leitura (para o "Dados da mensagem")
  const upd2 = { ...upd };
  if (upd.status === 'delivered') upd2.delivered_at = new Date().toISOString();
  if (upd.status === 'read') upd2.read_at = new Date().toISOString();
  const { error: eCol } = await supabase.from('messages').update(upd2).eq('wamid', wamid).eq('direction', 'outbound')
    .or('status.is.null,status.in.(' + lower.join(',') + ')');
  if (eCol) { // colunas de horário ainda não existem no banco: segue só com o status
    await supabase.from('messages').update(upd).eq('wamid', wamid).eq('direction', 'outbound')
      .or('status.is.null,status.in.(' + lower.join(',') + ')');
  }
  _mirrorContactStatus(wamid, upd.status);
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
// 🔏 Assinatura do webhook da Meta (X-Hub-Signature-256 = HMAC do corpo com APP_SECRET).
// Modo padrão: só AVISA no log quando não bate (contas de portfólios/apps diferentes
// podem ter outro segredo). Com WEBHOOK_STRICT=1 no Railway, rejeita o que não bater.
function _assinaturaMetaOk(req) {
  const sec = process.env.APP_SECRET;
  const sig = String(req.headers['x-hub-signature-256'] || '');
  if (!sec || !sig || !req.rawBody) return null; // sem como conferir
  try {
    const crypto = require('crypto');
    const esperado = 'sha256=' + crypto.createHmac('sha256', sec).update(req.rawBody).digest('hex');
    const a = Buffer.from(esperado), b = Buffer.from(sig);
    return a.length === b.length && crypto.timingSafeEqual(a, b);
  } catch (_) { return false; }
}
app.post("/webhook", async (req, res) => {
  const okSig = _assinaturaMetaOk(req);
  if (okSig === false) {
    console.warn('🔏 Webhook da Meta com assinatura INVÁLIDA' + (process.env.WEBHOOK_STRICT === '1' ? ' — rejeitado' : ' — aceito (modo aviso; defina WEBHOOK_STRICT=1 para rejeitar)'));
    if (process.env.WEBHOOK_STRICT === '1') return res.sendStatus(401);
  }
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

      // 🏛️ Vereditos e eventos da CONTA na Meta (análise aprovada/rejeitada,
      // banimento, restrição, qualidade) → viram AVISO + push na hora
      // 📋 MODELOS: aprovado / reprovado / pausado / desativado / categoria ou qualidade
      // alterada → AVISO + push (precisa do campo "message_template_status_update"
      // assinado no webhook do app na Meta)
      if (['message_template_status_update', 'template_category_update', 'message_template_quality_update'].includes(change.field)) {
        try {
          const wabaId = String((body.entry || [])[0]?.id || '');
          let ownerN = null, nomeN = '';
          if (supabase) {
            let { data: accsW } = wabaId ? await supabase.from('accounts').select('name, owner').eq('waba_id', wabaId) : { data: null };
            accsW = (accsW || []).filter(Boolean);
            if (!accsW.length) { const { data: a1 } = await supabase.from('accounts').select('name, owner').not('owner', 'is', null).limit(1).maybeSingle(); if (a1) accsW = [a1]; }
            if (accsW.length) { ownerN = accsW[0].owner; nomeN = accsW.map(a => a.name).filter(Boolean).join(', '); }
          }
          const tName = value?.message_template_name || value?.name || '?';
          const tLang = value?.message_template_language || value?.language || '';
          const sufixo = nomeN ? ` (${nomeN.includes(',') ? 'números: ' : 'conta '}${nomeN})` : '';
          const modelo = `"${tName}"${tLang ? ' · ' + tLang : ''}`;
          let txt = null;
          if (change.field === 'message_template_status_update') {
            const ev = String(value?.event || '').toUpperCase();
            const motivo = value?.reason && String(value.reason).toUpperCase() !== 'NONE' ? ` — motivo: ${value.reason}` : '';
            const mapa = {
              APPROVED: `✅ Modelo APROVADO pela Meta: ${modelo}${sufixo} — já pode ser usado.`,
              REJECTED: `❌ Modelo REPROVADO pela Meta: ${modelo}${sufixo}${motivo}`,
              PAUSED: `⏸ Modelo PAUSADO pela Meta: ${modelo}${sufixo}${motivo}`,
              DISABLED: `🚫 Modelo DESATIVADO pela Meta: ${modelo}${sufixo}${motivo}`,
              PENDING_DELETION: `🗑 Modelo marcado para EXCLUSÃO: ${modelo}${sufixo}`,
              FLAGGED: `⚠️ Modelo SINALIZADO pela Meta (qualidade baixa): ${modelo}${sufixo}${motivo}`,
              IN_APPEAL: `📨 Recurso do modelo em análise: ${modelo}${sufixo}`,
              PENDING: `⏳ Modelo em análise: ${modelo}${sufixo}`
            };
            txt = mapa[ev] || `ℹ️ Modelo ${modelo}: ${ev || 'atualização'}${sufixo}${motivo}`;
          } else if (change.field === 'template_category_update') {
            txt = `🏷 Categoria do modelo ${modelo} alterada pela Meta: ${value?.previous_category || '?'} → ${value?.new_category || '?'}${sufixo}`;
          } else {
            txt = `📶 Qualidade do modelo ${modelo}: ${value?.previous_quality_score || '?'} → ${value?.new_quality_score || '?'}${sufixo}`;
          }
          if (txt) addNotice(ownerN, txt, 'tmpl:' + change.field + ':' + wabaId + ':' + tName + ':' + (value?.event || value?.new_category || value?.new_quality_score || ''));
        } catch (e) { console.error('Evento de modelo Meta:', e.message); }
        continue;
      }
      if (['account_review_update', 'account_update', 'phone_number_quality_update'].includes(change.field)) {
        try {
          const wabaId = String((body.entry || [])[0]?.id || '');
          let ownerN = null, nomeN = '';
          if (supabase) {
            // O evento é do PORTFÓLIO (WABA), que pode ter vários números — cita todos
            let { data: accsW } = wabaId ? await supabase.from('accounts').select('name, owner').eq('waba_id', wabaId) : { data: null };
            accsW = (accsW || []).filter(Boolean);
            if (!accsW.length) { const { data: a1 } = await supabase.from('accounts').select('name, owner').not('owner', 'is', null).limit(1).maybeSingle(); if (a1) accsW = [a1]; }
            if (accsW.length) { ownerN = accsW[0].owner; nomeN = accsW.map(a => a.name).filter(Boolean).join(', '); }
          }
          const sufixo = nomeN ? (nomeN.includes(',') ? ` (números: ${nomeN})` : ` (conta "${nomeN}")`) : '';
          let txt = null;
          if (change.field === 'account_review_update') {
            const d = String(value?.decision || '').toUpperCase();
            txt = d === 'APPROVED'
              ? `✅ ANÁLISE DA META APROVADA${sufixo} — a conta foi liberada!`
              : d === 'REJECTED'
                ? `❌ ANÁLISE DA META REJEITADA${sufixo} — o recurso foi negado.`
                : `ℹ️ Atualização da análise da Meta${sufixo}: ${d || 'sem detalhes'}`;
          } else if (change.field === 'account_update') {
            const ev = String(value?.event || '').toUpperCase();
            const mapaEv = {
              DISABLED_UPDATE: '🚫 Conta DESATIVADA pela Meta',
              ACCOUNT_RESTRICTION: '⚠️ Conta RESTRINGIDA pela Meta',
              ACCOUNT_VIOLATION: '⚠️ Violação de política registrada pela Meta',
              ACCOUNT_DELETED: '🗑 Conta EXCLUÍDA na Meta',
              VERIFIED_ACCOUNT: '✅ Conta VERIFICADA pela Meta'
            };
            const extra = value?.ban_info?.waba_ban_state ? ` — estado: ${value.ban_info.waba_ban_state}` : '';
            // Eventos técnicos de bastidor (parceiro instalado, termos assinados, número
            // adicionado ao portfólio…) NÃO viram aviso — só ficam no log
            const _silenciosos = /^(PARTNER_|MM_LITE_|BUSINESS_CAPABILITY|CAPABILITY_UPDATE|PRIMARY_BUSINESS_LOCATION|AD_ACCOUNT_LINKED|OBO_)/;
            if (mapaEv[ev]) txt = mapaEv[ev] + sufixo + extra;
            else if (_silenciosos.test(ev) || !ev) { console.log('ℹ️ Evento de conta Meta (silencioso):', ev, sufixo); txt = null; }
            else txt = 'ℹ️ Atualização da conta na Meta: ' + ev + sufixo + extra;
          } else {
            const ev = String(value?.event || '');
            txt = `📶 Qualidade do número atualizada${sufixo}: ${ev}${value?.current_limit ? ` — limite de envio: ${value.current_limit}` : ''}`;
          }
          if (txt) addNotice(ownerN, txt, 'meta:' + change.field + ':' + wabaId + ':' + (value?.decision || value?.event || ''));
        } catch (e) { console.error('Evento de conta Meta:', e.message); }
        continue;
      }

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
          // Prévia da lista IGUAL ao WhatsApp: "Reagiu com ❤️ a: …" (e sobe a conversa)
          if (emoji) { try {
            const { data: alvo } = await supabase.from('messages').select('content, phone, owner').eq('wamid', targetWamid).maybeSingle();
            if (alvo) {
              const trecho = String(alvo.content || 'sua mensagem').replace(/\s+/g, ' ').slice(0, 40);
              let q = supabase.from('contacts').update({
                last_message_preview: `Reagiu com ${emoji} a: ${trecho}`,
                last_message_at: new Date().toISOString(),
                last_message_direction: 'inbound', last_message_status: null
              }).eq('phone', alvo.phone);
              if (alvo.owner) q = q.eq('owner', alvo.owner);
              await q;
            }
          } catch (_) {} }
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
      } else if (type === "location") {
        const la = message.location?.latitude, lo = message.location?.longitude;
        content = `📍 ${message.location?.name || 'Localização'}` + (la != null ? `\nhttps://maps.google.com/?q=${la},${lo}` : '');
      } else if (type === "contacts") {
        const ct0 = (message.contacts || [])[0];
        content = `👤 ${ct0?.name?.formatted_name || 'Contato'}` + (ct0?.phones?.[0]?.phone ? `\n${ct0.phones[0].phone}` : '');
      } else if (type === "button") {
        // Botão de resposta rápida de um template aprovado
        content = message.button?.text || "[Botão]";
      } else if (type === "interactive") {
        // Botões/listas interativas
        const it = message.interactive;
        content = it?.button_reply?.title || it?.list_reply?.title || it?.nfm_reply?.name || "[Resposta interativa]";
      } else if (type === 'unsupported') {
        content = '⚠️ Mensagem não suportada pela API — veja no aplicativo do WhatsApp';
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

        // Incrementa o contador de não lidas SEM se atrapalhar quando chegam
        // várias mensagens ao mesmo tempo (4 arquivos seguidos mostravam 3)
        await _somaNaoLida(from, ownerEmail, timestamp);

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

        // 🗄️ Guarda uma cópia do arquivo por 6 meses (a Meta apaga em ~30 dias)
        if (mediaId && accountToken) arquivaMidiaApi(mediaId, accountToken, mediaMimeType).catch(() => {});

        if (msgErr) {
          console.error("❌ Erro ao salvar mensagem:", msgErr.message, msgErr.details);
        } else {
          console.log("✅ Mensagem salva:", content.substring(0, 50));
        }
        // Etiqueta "Encaminhada" (opcional — ignora se a coluna não existir)
        try {
          if (message.context && (message.context.forwarded || message.context.frequently_forwarded) && message.id)
            await supabase.from('messages').update({ forwarded: true }).eq('wamid', message.id);
        } catch (_) {}

        // Notificação push nos aparelhos do dono (não bloqueia o processamento)
        // — a menos que a conversa esteja SILENCIADA (🔇)
        if (await _isContactMuted(from, ownerEmail)) { /* silenciada: sem push */ } else
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
        // Encaminha para o N8N configurado PELA DONA desta conta (separado por conta)
        const n8nUrl = _cfg('n8n_webhook_url', ownerEmail);
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
        // Conta JÁ existente = PRESERVA o nome que você deu (a Meta não manda mais
        // o nome dela por cima quando você reconecta/atualiza o token)
        let _nomeAtual = null;
        try {
          const { data: _ex } = await supabase.from('accounts').select('name').eq('phone_number_id', phone.id).maybeSingle();
          _nomeAtual = _ex?.name || null;
        } catch (_) {}
        const accountData = {
          name: _nomeAtual || phone.verified_name || wabaName,
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
      // Sem meio-termo: ou está conectada, ou está DESCONECTADA (o "conectando"
      // eterno das re-tentativas confundia — melhor avisar que caiu)
      status = _waState[acc.evolution_instance] === 'open' ? 'connected' : 'disconnected';
    } else if (acc.phone_number_id) {
      status = await cloudApiStatus(acc.id);
      const mot = _acctStatusCache[acc.id]?.motivo;
      if (status === 'disconnected' && mot) return { ...acc, status, status_motivo: mot };
    }
    return { ...acc, status };
  }));
  res.json(out);
});

// ── CENTRAL DE AVISOS: registra eventos importantes (ex.: número desconectado)
// e manda push. Guardado em settings (notices::<dono>), últimos 50, sem repetir
// o mesmo aviso em menos de 1 hora.
async function addNotice(owner, text, dedupeKey) {
  if (!supabase) return;
  try {
    const K = 'notices::' + (owner || ' ');
    const { data } = await supabase.from('settings').select('value').eq('key', K).maybeSingle();
    let list = [];
    try { list = data?.value ? JSON.parse(data.value) : []; } catch (_) {}
    const now = Date.now();
    if (dedupeKey && list.some(n => n.k === dedupeKey && now - n.ts < 3600000)) return;
    // Aviso de DESCONEXÃO não se repete enquanto a conta não voltar (nem após
    // reinício do servidor): só avisa de novo se a conta reconectou entre um e outro
    if (dedupeKey && dedupeKey.startsWith('disc:')) {
      const idAlvo = dedupeKey.slice(5);
      const ultDisc = list.find(n => n.k === dedupeKey);
      const ultReconn = list.find(n => n.k === 'reconn:' + idAlvo);
      if (ultDisc && (!ultReconn || ultDisc.ts > ultReconn.ts)) return;
    }
    list.unshift({ text, ts: now, k: dedupeKey || null, read: false });
    list = list.slice(0, 50);
    await supabase.from('settings').upsert({ key: K, value: JSON.stringify(list), updated_at: new Date().toISOString() });
    sendPushToOwner(owner || null, { title: '⚠️ VETRA — Aviso', body: text, tag: 'notice' }).catch(() => {});
  } catch (e) { console.error('addNotice:', e.message); }
}
// Conta voltou → neutraliza o marcador de "desconectada" (permite avisar numa próxima queda)
async function clearNoticeDisc(owner, key) {
  if (!supabase) return;
  try {
    const K = 'notices::' + (owner || ' ');
    const { data } = await supabase.from('settings').select('value').eq('key', K).maybeSingle();
    let list = []; try { list = data?.value ? JSON.parse(data.value) : []; } catch (_) {}
    let mudou = false;
    list = list.map(n => { if (n.k === key) { mudou = true; return { ...n, k: key + ':ok' }; } return n; });
    if (mudou) await supabase.from('settings').upsert({ key: K, value: JSON.stringify(list), updated_at: new Date().toISOString() });
  } catch (_) {}
}
app.get('/notices', async (req, res) => {
  if (!supabase) return res.json({ value: [] });
  const { data } = await supabase.from('settings').select('value').eq('key', 'notices::' + (req.owner || ' ')).maybeSingle();
  let v = [];
  try { v = data?.value ? JSON.parse(data.value) : []; } catch (_) {}
  res.json({ value: v });
});
app.put('/notices/read', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const K = 'notices::' + (req.owner || ' ');
  const { data } = await supabase.from('settings').select('value').eq('key', K).maybeSingle();
  let v = [];
  try { v = data?.value ? JSON.parse(data.value) : []; } catch (_) {}
  v.forEach(n => n.read = true);
  await supabase.from('settings').upsert({ key: K, value: JSON.stringify(v), updated_at: new Date().toISOString() });
  res.json({ success: true });
});

// Status da conta da API oficial (checa o token na Meta, com cache de 5 min
// para não gastar recursos do Railway a cada carregamento)
const _acctStatusCache = {};
async function cloudApiStatus(accId) {
  const c = _acctStatusCache[accId];
  if (c && Date.now() - c.ts < 5 * 60000) return c.status;
  let status = 'disconnected', motivo = 'token inválido ou expirado';
  try {
    const { data: a } = await supabase.from('accounts').select('phone_number_id, token, evolution_instance').eq('id', accId).maybeSingle();
    // BLINDAGEM: conta QR nunca é avaliada como API (mesmo com credencial de resquício)
    if (a?.evolution_instance) {
      const stQr = _waState[a.evolution_instance] === 'open' ? 'connected' : 'disconnected';
      _acctStatusCache[accId] = { status: stQr, ts: Date.now() };
      return stQr;
    }
    if (a?.phone_number_id && a?.token) {
      // Pergunta o ESTADO REAL do número (não só se o token responde): a Meta pode
      // ter DESATIVADO/RESTRINGIDO o número mesmo com o token funcionando.
      const r = await axios.get(`https://graph.facebook.com/v23.0/${a.phone_number_id}`,
        { params: { access_token: a.token, fields: 'id,status,quality_rating,name_status,code_verification_status' }, timeout: 8000 });
      const st = String(r.data?.status || '').toUpperCase();
      if (r.data?.id && (st === 'CONNECTED' || st === '')) status = 'connected';
      else if (r.data?.id) {
        status = 'disconnected';
        const mapa = {
          DISCONNECTED: 'número desconectado na Meta',
          RESTRICTED: 'número RESTRITO pela Meta (limite de envio)',
          FLAGGED: 'número SINALIZADO pela Meta (qualidade baixa)',
          BANNED: 'número BANIDO pela Meta',
          PENDING: 'número pendente de aprovação',
          MIGRATED: 'número migrado para outra conta',
          UNVERIFIED: 'número não verificado',
          RATE_LIMITED: 'número temporariamente limitado'
        };
        motivo = mapa[st] || ('estado na Meta: ' + st);
      }
    } else motivo = 'faltam credenciais (Phone Number ID/Token) no CRM';
  } catch (e) {
    status = 'disconnected';
    const m = e.response?.data?.error?.message;
    if (m) motivo = m;
  }
  // Mudou para DESCONECTADA (inclusive na primeira checagem após reiniciar) → avisa
  const prev = _acctStatusCache[accId]?.status;
  if (status === 'disconnected' && prev !== 'disconnected') {
    try {
      const { data: a } = await supabase.from('accounts').select('name, owner').eq('id', accId).maybeSingle();
      if (a) addNotice(a.owner, `🔌 A conta da API oficial "${a.name}" está DESCONECTADA — ${motivo}. Verifique em Contas.`, 'disc:' + accId);
    } catch (_) {}
  }
  // Estava DESCONECTADA e VOLTOU → avisa a boa notícia também
  if (status === 'connected' && prev === 'disconnected') {
    try {
      const { data: a } = await supabase.from('accounts').select('name, owner').eq('id', accId).maybeSingle();
      if (a) addNotice(a.owner, `✅ A conta da API oficial "${a.name}" foi RESTABELECIDA e está conectada novamente!`, 'reconn:' + accId);
    } catch (_) {}
  }
  // Conectada (por qualquer caminho) → libera novo aviso para uma queda futura
  if (status === 'connected') {
    try {
      const { data: a2 } = await supabase.from('accounts').select('owner').eq('id', accId).maybeSingle();
      if (a2) clearNoticeDisc(a2.owner, 'disc:' + accId);
    } catch (_) {}
  }
  _acctStatusCache[accId] = { status, ts: Date.now(), motivo };
  return status;
}

// 🧹 LIMPEZA AUTOMÁTICA: mídias de mensagens QR com mais de 6 MESES saem do
// Storage (senão o balde cresce para sempre e estoura o plano). Fotos de
// perfil (qr/avatars) são PRESERVADAS. Roda 10 min após subir e 1x por dia.
async function _limpaMidiasAntigas() {
  if (!supabase) return;
  const limite = Date.now() - 183 * 24 * 3600000; // ~6 meses
  try {
    const { data: pastas } = await supabase.storage.from('wa-media').list('qr', { limit: 1000 });
    for (const p of (pastas || [])) {
      if (!p || !p.name || p.name === 'avatars') continue; // avatares ficam
      let removidas = 0, offset = 0;
      let _voltas = 0;
      while (_voltas++ < 60) { // teto de segurança: nunca gira para sempre
        const { data: arqs } = await supabase.storage.from('wa-media').list('qr/' + p.name, { limit: 1000, offset });
        if (!arqs || !arqs.length) break;
        const velhos = arqs
          .filter(a => a && a.name && a.created_at && new Date(a.created_at).getTime() < limite)
          .map(a => 'qr/' + p.name + '/' + a.name);
        for (let i = 0; i < velhos.length; i += 100) {
          const lote = velhos.slice(i, i + 100);
          const { error: remErr } = await supabase.storage.from('wa-media').remove(lote);
          if (!remErr) removidas += lote.length;
        }
        if (arqs.length < 1000) break;
        offset += 1000 - Math.min(1000, velhos.length); // compensa os removidos na paginação
        if (offset < 0) offset = 0;
      }
      if (removidas) console.log(`🧹 Limpeza: ${removidas} mídia(s) com +6 meses removida(s) de qr/${p.name}`);
    }
  } catch (e) { console.error('Limpeza de mídias:', e.message); }
}
// (o agendamento fica com a _faxinaCompleta, mais abaixo: idade + teto de espaço)

// Vigia as contas da API oficial a cada 15 min — o aviso chega mesmo sem você
// abrir a tela de Contas (antes, o problema só era detectado ao abrir a tela).
setInterval(async () => {
  try {
    if (!supabase) return;
    const { data: accs } = await supabase.from('accounts').select('id')
      .not('phone_number_id', 'is', null)
      .is('evolution_instance', null); // SÓ contas da API oficial — QR tem vigia próprio
    for (const a of (accs || [])) {
      const c = _acctStatusCache[a.id];
      if (c) c.ts = 0; // força nova checagem, preservando o status anterior (detecta a virada)
      await cloudApiStatus(a.id);
    }
  } catch (_) {}
}, 15 * 60000);

// ── Adicionar conta manualmente ──
// Faz o MESMO ritual do fluxo do Facebook: número visível, WABA (modelos!),
// registro na Cloud API e inscrição da WABA no webhook — sem isso a conta
// ficava "capada": sem número na tela, sem modelos e sem receber mensagens
app.post("/accounts", async (req, res) => {
  const { name, phone_number_id, token } = req.body;
  if (!name || !phone_number_id || !token)
    return res.status(400).json({ error: "Informe name, phone_number_id e token" });
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });

  let phone_display = null;
  // WABA informada no formulário tem prioridade (tokens de usuário do sistema
  // não expõem a lista granular no debug_token — a descoberta automática falha)
  let waba_id = String(req.body.waba_id || '').trim() || null;
  // 1. Número visível (ex.: +55 15 98164-7190) — também valida o token
  try {
    const r = await axios.get(`https://graph.facebook.com/v23.0/${phone_number_id}`,
      { params: { access_token: token, fields: 'display_phone_number,verified_name' }, timeout: 10000 });
    phone_display = r.data?.display_phone_number || null;
  } catch (e) {
    return res.status(400).json({ error: 'O token não enxerga este número: ' + (e.response?.data?.error?.message || e.message) });
  }
  // 2. Descobre a WABA deste número (necessária para MODELOS e webhook)
  try {
    if (!waba_id && APP_ID && APP_SECRET) {
      const dbg = await axios.get('https://graph.facebook.com/v23.0/debug_token',
        { params: { input_token: token, access_token: `${APP_ID}|${APP_SECRET}` }, timeout: 10000 });
      const esc = (dbg.data.data?.granular_scopes || []).find(s => s.scope === 'whatsapp_business_management');
      for (const wid of (esc?.target_ids || [])) {
        try {
          const ph = await axios.get(`https://graph.facebook.com/v23.0/${wid}/phone_numbers`,
            { params: { access_token: token, fields: 'id' }, timeout: 10000 });
          if ((ph.data.data || []).some(p => String(p.id) === String(phone_number_id))) { waba_id = wid; break; }
        } catch (_) {}
      }
    }
  } catch (e) { console.log('⚠️ WABA não descoberta:', e.response?.data?.error?.message || e.message); }
  // 3. Registra o número na Cloud API (ativa se pendente; inofensivo se já ativo)
  try {
    await axios.post(`https://graph.facebook.com/v23.0/${phone_number_id}/register`,
      { messaging_product: 'whatsapp', pin: process.env.WHATSAPP_PIN || '123456' },
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } });
    console.log('✅ Número registrado na Cloud API (manual):', phone_display || phone_number_id);
  } catch (e) { console.log('⚠️ Registro (pode já estar ativo):', e.response?.data?.error?.message); }
  // 4. Inscreve a WABA no webhook do app (SEM isso as mensagens recebidas não chegam)
  if (waba_id) {
    try {
      await axios.post(`https://graph.facebook.com/v23.0/${waba_id}/subscribed_apps`, {}, { params: { access_token: token } });
      console.log('✅ WABA inscrita no webhook (manual):', waba_id);
    } catch (e) { console.log('⚠️ Webhook subscribe:', e.response?.data?.error?.message); }
  }

  // Conta JÁ existente (ex.: você reenviou o token) = PRESERVA o nome atual.
  // Para trocar o nome, use o ✏️ na lista de Contas.
  let _nomePreservado = null;
  try {
    const { data: _ex } = await supabase.from('accounts').select('name').eq('phone_number_id', phone_number_id).maybeSingle();
    _nomePreservado = _ex?.name || null;
  } catch (_) {}
  const { data, error } = await supabase
    .from("accounts")
    .upsert({ name: _nomePreservado || name, phone_number_id, phone_display, waba_id, token, owner: req.owner || null }, { onConflict: 'phone_number_id' })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  const _avisos = [];
  if (!waba_id) _avisos.push('WABA não localizada — os modelos podem não aparecer');
  if (_nomePreservado && _nomePreservado !== name) _avisos.push('O nome "' + _nomePreservado + '" foi mantido (use o ✏️ em Contas para trocar)');
  res.json({ success: true, data, aviso: _avisos.length ? _avisos.join('. ') : null });
});

// 🩺 DIAGNÓSTICO do número na Meta: diz EXATAMENTE o que falta para ele sair de
// "Pendente" (verificação do número, registro na Cloud API, aprovação do nome)
app.get('/accounts/:id/meta-diag', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { data: a } = await supabase.from('accounts').select('id, name, phone_number_id, token, waba_id, evolution_instance')
    .eq('id', req.params.id).eq('owner', req.owner || ' ').maybeSingle();
  if (!a) return res.status(404).json({ error: 'Conta não encontrada' });
  if (a.evolution_instance) return res.json({ tipo: 'qr', aviso: 'Conta QR Code — não passa pela aprovação da Meta.' });
  if (!a.phone_number_id || !a.token) return res.status(400).json({ error: 'Faltam Phone Number ID/Token nesta conta.' });
  try {
    const r = await axios.get(`https://graph.facebook.com/v23.0/${a.phone_number_id}`, {
      params: { access_token: a.token, fields: 'id,display_phone_number,verified_name,status,quality_rating,name_status,code_verification_status,platform_type' },
      timeout: 10000
    });
    const d = r.data || {};
    const st = String(d.status || '').toUpperCase();
    const cod = String(d.code_verification_status || '').toUpperCase();
    const nome = String(d.name_status || '').toUpperCase();
    const passos = [];
    if (cod && cod !== 'VERIFIED') passos.push('1) VERIFICAR O NÚMERO: no Gerenciador da Meta (WhatsApp > Números), clique no número e conclua a verificação por SMS ou ligação.');
    if (st === 'PENDING' || st === 'UNVERIFIED') passos.push('2) REGISTRAR na Cloud API: use o botão "⚡ Ativar na Meta" aqui no VETRA (registra o número com um PIN de 6 dígitos).');
    if (nome && !['APPROVED', 'AVAILABLE_WITHOUT_REVIEW'].includes(nome)) passos.push('3) NOME DE EXIBIÇÃO em análise/rejeitado (' + nome + '): a Meta leva até 48h. O número pode ficar "Pendente" até aprovar.');
    res.json({ tipo: 'api', numero: d.display_phone_number || null, nome_exibicao: d.verified_name || null,
      status: st || null, verificacao: cod || null, nome_status: nome || null, qualidade: d.quality_rating || null, passos });
  } catch (e) {
    res.status(400).json({ error: e.response?.data?.error?.message || e.message });
  }
});

// ⚡ ATIVAR: registra o número na Cloud API (é o que tira do "Pendente" e liga o envio).
// PIN: o de 6 dígitos da verificação em duas etapas do número (padrão do CRM se não enviar).
app.post('/accounts/:id/ativar', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { data: a } = await supabase.from('accounts').select('id, name, phone_number_id, token, waba_id, evolution_instance')
    .eq('id', req.params.id).eq('owner', req.owner || ' ').maybeSingle();
  if (!a) return res.status(404).json({ error: 'Conta não encontrada' });
  if (a.evolution_instance) return res.status(400).json({ error: 'Conta QR Code não precisa de ativação na Meta.' });
  if (!a.phone_number_id || !a.token) return res.status(400).json({ error: 'Faltam Phone Number ID/Token nesta conta.' });
  const pin = String(req.body?.pin || process.env.WHATSAPP_PIN || '123456').replace(/\D/g, '');
  if (pin.length !== 6) return res.status(400).json({ error: 'O PIN precisa ter 6 dígitos.' });
  try {
    await axios.post(`https://graph.facebook.com/v23.0/${a.phone_number_id}/register`,
      { messaging_product: 'whatsapp', pin },
      { headers: { Authorization: `Bearer ${a.token}`, 'Content-Type': 'application/json' }, timeout: 15000 });
    // Garante também a inscrição da WABA no webhook (sem isso não chegam mensagens)
    if (a.waba_id) { try { await axios.post(`https://graph.facebook.com/v23.0/${a.waba_id}/subscribed_apps`, {}, { params: { access_token: a.token } }); } catch (_) {} }
    delete _acctStatusCache[a.id]; // força reler o estado real na próxima consulta
    let st = null;
    try {
      const r = await axios.get(`https://graph.facebook.com/v23.0/${a.phone_number_id}`, { params: { access_token: a.token, fields: 'status' }, timeout: 8000 });
      st = String(r.data?.status || '').toUpperCase() || null;
    } catch (_) {}
    res.json({ success: true, status: st });
  } catch (e) {
    const err = e.response?.data?.error || {};
    const cod = err.code, sub = err.error_subcode;
    let dica = err.message || e.message;
    if (cod === 133005 || sub === 2388005) dica = 'PIN incorreto: este número já tem verificação em duas etapas com OUTRO PIN. Use o PIN correto ou peça a redefinição no Gerenciador da Meta (WhatsApp > Números > o número > Verificação em duas etapas).';
    else if (cod === 133010) dica = 'O número ainda NÃO foi verificado na Meta. Conclua a verificação por SMS/ligação no Gerenciador antes de ativar.';
    else if (cod === 133006) dica = 'O número precisa ser verificado novamente na Meta (SMS ou ligação) antes do registro.';
    else if (cod === 100) dica = 'A Meta recusou o registro: ' + dica + ' — confira se o token tem permissão sobre este número.';
    res.status(400).json({ error: dica, codigo: cod || null });
  }
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
  // Conta por QR: encerra a conexão ANTES de apagar — senão o WhatsApp continuava
  // conectado e as mensagens chegavam "sem dono" (invisíveis no CRM)
  try {
    const { data: _a } = await supabase.from('accounts').select('evolution_instance').eq('id', req.params.id).eq('owner', req.owner || ' ').maybeSingle();
    const _inst = _a && _a.evolution_instance;
    if (_inst) {
      const sock = _waSocks[_inst];
      if (sock) { try { await sock.logout(); } catch (_) {} try { sock.end(undefined); } catch (_) {} delete _waSocks[_inst]; }
      delete _waState[_inst]; delete _waPhone[_inst]; delete qrCache[_inst];
      try { await supabase.from('wa_sessions').delete().eq('instance', _inst); } catch (_) {}
    }
  } catch (e) { console.error('encerrar QR ao apagar conta:', e.message); }
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { error } = await supabase.from("accounts").delete().eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Enviar mensagem ──
// REGRA DE OURO: quando VOCÊ envia qualquer mensagem na conversa, o bot daquele
// lead é ENCERRADO na hora — humano assumiu, robô sai de cena. Nunca mais um bot
// se intromete numa conversa que você está tocando.
async function stopBotRunsForPhone(phone, owner) {
  try {
    if (!supabase || !phone) return;
    let q = supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() })
      .eq('contact_phone', phone).in('status',['running','waiting_reply','paused']);
    if (owner) q = q.eq('owner', owner);
    await q;
  } catch (_) {}
}

// 🚫 TRAVA ANTI-AUTOENVIO: nenhuma conta pode enviar mensagem para o PRÓPRIO
// número (acontecia quando um disparo em massa incluía o contato de teste com
// o número da própria conta). Compara com e sem o nono dígito.
function _sameBrPhone(a, b) {
  a = String(a || '').replace(/\D/g, ''); b = String(b || '').replace(/\D/g, '');
  if (!a || !b) return false;
  const vars = p => {
    const out = new Set([p]);
    if (/^55\d{10}$/.test(p)) out.add(p.slice(0, 4) + '9' + p.slice(4));
    if (/^55\d{11}$/.test(p) && p[4] === '9') out.add(p.slice(0, 4) + p.slice(5));
    return out;
  };
  const va = vars(a);
  for (const x of vars(b)) if (va.has(x)) return true;
  return false;
}
async function _isSelfSend(to, account_id) {
  try {
    if (!supabase || !account_id || !to) return false;
    const { data: a } = await supabase.from('accounts').select('phone_display, evolution_instance').eq('id', account_id).maybeSingle();
    if (!a) return false;
    let own = a.phone_display || '';
    if (a.evolution_instance && _waPhone[a.evolution_instance]) own = _waPhone[a.evolution_instance];
    return _sameBrPhone(to, own);
  } catch (_) { return false; }
}

app.post("/send", async (req, res) => {
  let { to, message, account_id, quoted_id, quoted_content, quoted_direction } = req.body;
  if (!to || !message) return res.status(400).json({ error: "Informe 'to' e 'message'" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito
  if (await _isSelfSend(to, account_id)) return res.status(400).json({ error: '🚫 Bloqueado: o destino é o PRÓPRIO número desta conta — envio para si mesmo não é permitido.' });
  stopBotRunsForPhone(to, req.owner); // você assumiu a conversa — bot deste lead para

  let phoneNumberId, token, evolutionInstance = null, accountType = 'cloudapi';

  // 1. Tenta buscar conta do banco de dados pelo account_id
  if (supabase && account_id) {
    const { data: account, error: accErr } = await supabase
      .from("accounts").select("phone_number_id, token, type, evolution_instance").eq("id", account_id).eq("owner", req.owner || ' ').single();
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
        await supabase.from('contacts').upsert({ phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId, last_message_preview: preview, last_message_direction: 'outbound', last_message_status: null, owner: req.owner || null }, { onConflict: 'owner,phone' });
        await supabase.from('messages').insert({ phone: to, content: message, type: 'text', direction: 'outbound', timestamp: new Date().toISOString(), account_id: safeAccountId, status: wamid ? 'sent' : 'pending', wamid, owner: req.owner || null, quoted_id: quoted_id || null, quoted_content: quoted_content || null, quoted_direction: quoted_direction || null });
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
          last_message_direction: 'outbound', last_message_status: null,
          owner: req.owner || null,
        },
        { onConflict: "owner,phone" }
      );
      const wamid = response.data?.messages?.[0]?.id || null;
      const { error: msgErr } = await supabase.from("messages").insert({
        phone: to, content: message, type: "text", direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        // Com o id do WhatsApp em mãos, a mensagem JÁ SAIU → nasce como "enviada"
        // (antes nascia "pendente" e o tique voltava para o relógio por instantes)
        status: wamid ? 'sent' : 'pending', wamid, owner: req.owner || null,
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
  if (!_exigeLogin(req, res)) return;
  const { to, wamid, emoji, account_id } = req.body;
  if (!to || !wamid) return res.status(400).json({ error: "Informe 'to' e 'wamid'" });

  // Conta QR (motor embutido): reage direto pelo WhatsApp pareado
  if (WA_EMBEDDED && supabase && account_id) {
    const { data: accQ } = await supabase.from('accounts')
      .select('type, evolution_instance').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
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
  const mCaption = String(req.body.caption || '').trim();
  if (!to || !fileBase64 || !fileName || !mimeType)
    return res.status(400).json({ error: "Informe to, fileBase64, fileName e mimeType" });
  to = await resolveExistingPhone(to, req.owner); // unifica com/sem nono dígito
  if (await _isSelfSend(to, account_id)) return res.status(400).json({ error: '🚫 Bloqueado: o destino é o PRÓPRIO número desta conta.' });
  stopBotRunsForPhone(to, req.owner); // você assumiu a conversa — bot deste lead para

  let phoneNumberId, token, accountType = 'cloudapi', evolutionInstance = null;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("phone_number_id, token, type, evolution_instance").eq("id", account_id).eq("owner", req.owner || ' ').single();
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
        req._wfOut = wf; // usado depois para gravar a onda no banco
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
        sent = await sock.sendMessage(jid, { image: fileBuf, mimetype: baseMime, ...(mCaption ? { caption: mCaption } : {}) });
        msgType = 'image'; content = mCaption || `[Imagem: ${fileName}]`;
      } else if (baseMime.startsWith('video/')) {
        let vMime = 'video/mp4';
        if (baseMime !== 'video/mp4') {
          try { fileBuf = await convertVideoToMp4(fileBuf); }
          catch (ve) { console.error('⚠️ Conversão de vídeo falhou, enviando original:', ve.message); vMime = baseMime; }
        }
        sent = await sock.sendMessage(jid, { video: fileBuf, mimetype: vMime, ...(mCaption ? { caption: mCaption } : {}) });
        msgType = 'video'; qrSentMime = vMime; content = mCaption || `[Vídeo: ${fileName}]`;
      } else {
        sent = await sock.sendMessage(jid, { document: fileBuf, mimetype: baseMime, fileName });
        msgType = 'document'; content = `[Documento: ${fileName}]`;
      }
      let mediaPathOut = null; // visível na resposta final (fora do bloco do supabase)
      if (supabase) {
        const wamid = sent?.key?.id || null;
        // Guarda a mídia enviada para poder reproduzi-la no CRM
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
            last_message_preview: content, last_message_direction: 'outbound', last_message_status: null, owner: req.owner || null },
          { onConflict: 'owner,phone' });
        await supabase.from('messages').insert({
          phone: to, content, type: msgType, direction: 'outbound',
          timestamp: new Date().toISOString(), account_id: account_id || null,
          status: 'sent', wamid, owner: req.owner || null,
          media_id: mediaPathOut, media_mime_type: mediaPathOut ? outMime : null });
        // Onda REAL da mensagem de voz (opcional — ignora se a coluna não existir)
        try {
          if (req._wfOut && req._wfOut.length && wamid)
            await supabase.from('messages').update({ waveform: JSON.stringify(Array.from(req._wfOut)) }).eq('wamid', wamid).eq('phone', to);
        } catch (_) {}
      }
      console.log(`📤 Mídia (${msgType}) enviada via WhatsApp QR: ${evolutionInstance}`);
      // media_id devolvido → o app mantém a prévia local no lugar (foto não pisca)
      return res.json({ success: true, via: 'qr', media_id: mediaPathOut || null });
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
    if (mCaption && (msgType === "image" || msgType === "video")) mediaObj.caption = mCaption;
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
        : ((mCaption && (msgType === "image" || msgType === "video")) ? mCaption : `[${label}: ${fileName}]`);
      await supabase.from("contacts").upsert(
        { phone: to, last_message_at: new Date().toISOString(), account_id: safeAccountId,
          last_message_preview: content, last_message_direction: 'outbound', last_message_status: null, owner: req.owner || null },
        { onConflict: "owner,phone" }
      );
      await supabase.from("messages").insert({
        phone: to, content,
        type: msgType, direction: "outbound",
        timestamp: new Date().toISOString(), account_id: safeAccountId,
        status: mediaWamid ? 'sent' : 'pending', wamid: mediaWamid, owner: req.owner || null,
        media_id: mediaId, media_mime_type: sendMime, // permite exibir a mídia no CRM
      });
      await applyPendingStatus(mediaWamid);
    }
    // 🗄️ Guarda uma cópia do que EU enviei por 6 meses (a Meta apaga em ~30 dias)
    try { if (mediaId && token) arquivaMidiaApi(mediaId, token, sendMime).catch(() => {}); } catch (_) {}
    // media_id devolvido → o app mantém a prévia local no lugar (foto não pisca)
    res.json({ success: true, media_id: (typeof mediaId !== 'undefined' && mediaId) || null });
  } catch (err) {
    console.error("❌ Erro ao enviar mídia:", err.response?.data || err.message);
    res.status(500).json({ error: "Falha ao enviar mídia", detail: err.response?.data });
  }
});

// ── Proxy de mídia recebida (imagem, áudio, vídeo, documento) ──
// Faz STREAMING direto da Meta repassando o Range — método correto para vídeo
// (sem baixar o arquivo inteiro na memória, evita travar a reprodução).
const mediaUrlCache = new Map(); // mediaId_token -> { url, ts }

// 🗄️ ARQUIVO PRÓPRIO DE 6 MESES: a Meta guarda os arquivos por ~30 dias. Ao
// receber/enviar mídia pela API oficial, guardamos UMA CÓPIA no nosso Storage
// (pasta api/) — assim as fotos e documentos continuam abrindo por 6 meses.
// A limpeza automática (a mesma das mídias do QR) apaga o que passa de 183 dias.
const _arquivando = new Set(); // evita baixar o mesmo arquivo duas vezes ao mesmo tempo
let _cofreUltimoOk = null, _cofreUltimoErro = null; // diagnóstico visível no /versao
async function arquivaMidiaApi(mediaId, token, mime) {
  if (!supabase || !mediaId || !token || String(mediaId).startsWith('qr/')) return;
  // 📦 Cofre cheio: para de copiar até a faxina abrir espaço (evita estourar a
  // cota do Supabase e derrubar o CRM inteiro). A Meta ainda serve o arquivo
  // pelos primeiros ~30 dias, então nada some na hora.
  if (_cofreCheio) { _cofreUltimoErro = 'cofre no limite (' + COFRE_TETO_MB + ' MB) — cópia adiada'; return; }
  const caminho = 'api/' + mediaId;
  if (_arquivando.has(caminho)) return;
  _arquivando.add(caminho);
  try {
    // Já guardado antes? Não baixa de novo (checagem DIRETA, sem busca por lista)
    try {
      const { data: ja } = await supabase.storage.from('wa-media')
        .createSignedUrl(caminho, 60); // barato: só confirma que o arquivo existe
      if (ja && ja.signedUrl) return;
    } catch (_) {}
    const metaRes = await axios.get(`https://graph.facebook.com/v23.0/${mediaId}`, {
      headers: { Authorization: `Bearer ${token}` }, timeout: 20000
    });
    const url = metaRes.data?.url;
    if (!url) return;
    // 📦 Arquivo grande demais não entra no cofre: um vídeo de 60 MB come 6% do
    // plano grátis sozinho. Acima do limite, fica só com a Meta (~30 dias).
    const _tam = Number(metaRes.data?.file_size || 0);
    if (_tam && _tam > COFRE_ARQ_MAX_MB * 1048576) {
      _cofreUltimoErro = mediaId + ': arquivo de ' + Math.round(_tam / 1048576) + ' MB — acima do limite de ' + COFRE_ARQ_MAX_MB + ' MB, não copiado';
      return;
    }
    const bin = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}`, 'User-Agent': 'WhatsApp/2.0' },
      responseType: 'arraybuffer', timeout: 60000, maxContentLength: COFRE_ARQ_MAX_MB * 1024 * 1024
    });
    const tipo = mime || metaRes.data?.mime_type || bin.headers['content-type'] || 'application/octet-stream';
    const { error } = await supabase.storage.from('wa-media')
      .upload(caminho, Buffer.from(bin.data), { contentType: tipo, upsert: true });
    if (error) { _cofreUltimoErro = mediaId + ': ' + error.message; console.error('🗄️ Arquivo 6 meses falhou:', mediaId, error.message); }
    else { _cofreUltimoOk = caminho + ' @ ' + new Date().toISOString(); console.log('🗄️ Mídia guardada por 6 meses:', caminho); }
  } catch (e) {
    const mm = e.response?.data?.error?.message || e.message || String(e.response?.status || '');
    _cofreUltimoErro = mediaId + ': ' + mm;
    console.error('🗄️ Arquivo 6 meses falhou:', mediaId, mm);
  } finally { _arquivando.delete(caminho); }
}

// 🧹 Faxina do arquivo próprio: apaga as cópias da API com mais de 6 meses
// (183 dias). Roda 5 min depois de subir e depois uma vez por dia.
async function _limpaCopiasApi() {
  if (!supabase) return;
  try {
    const limite = Date.now() - 183 * 24 * 3600 * 1000;
    let pag = 0, apagados = 0;
    while (pag < 40) { // teto de segurança (4000 arquivos por faxina)
      const { data: itens, error } = await supabase.storage.from('wa-media')
        .list('api', { limit: 100, offset: pag * 100 });
      if (error || !itens || !itens.length) break;
      const velhos = itens
        .filter(f => f.created_at && new Date(f.created_at).getTime() < limite)
        .map(f => 'api/' + f.name);
      if (velhos.length) {
        await supabase.storage.from('wa-media').remove(velhos);
        apagados += velhos.length;
      }
      if (itens.length < 100) break;
      pag++;
    }
    if (apagados) console.log(`🧹 Cópias com mais de 6 meses apagadas: ${apagados}`);
  } catch (e) { console.error('🧹 Faxina das cópias:', e.message); }
}
// ══════════════════════════════════════════════════════════════════════════
// 📦 CONTROLE DE ESPAÇO DO STORAGE
// O plano grátis do Supabase dá 1 GB. Quando estoura, o Supabase RESTRINGE o
// projeto inteiro — o CRM sai do ar, não só as fotos. Apagar só pela idade
// (183 dias) não bastava: o balde enchia muito antes de completar 6 meses.
// Agora existe um TETO: passou do teto, a faxina apaga do MAIS ANTIGO para o
// mais novo até voltar a caber. Fotos de perfil (qr/avatars) e fotos dos bots
// (bot/) nunca são apagadas — são pequenas e a tela depende delas.
// Dá para afinar pelo Railway: COFRE_TETO_MB e COFRE_ARQ_MAX_MB.
// ══════════════════════════════════════════════════════════════════════════
const COFRE_TETO_MB    = Number(process.env.COFRE_TETO_MB    || 700); // teto do balde inteiro
const COFRE_ARQ_MAX_MB = Number(process.env.COFRE_ARQ_MAX_MB || 12);  // nada maior que isso entra no cofre
const COFRE_ALVO_PCT   = 0.80;  // depois de podar, sobra folga até 80% do teto
let _espacoCache = null;        // última medição { mb, grupos, ts }
let _cofreCheio  = false;       // trava o arquivamento enquanto estiver no limite
let _ultimaPoda  = null;        // diagnóstico
let _ultimoErroStorage = null;  // último erro do Storage ao listar (para diagnóstico)

// Lista TODOS os arquivos de uma pasta (com tamanho e data). Pastas são ignoradas.
async function _listaArquivos(prefixo) {
  const saida = [];
  let offset = 0;
  while (offset < 20000) { // teto de segurança
    const { data, error } = await supabase.storage.from('wa-media')
      .list(prefixo, { limit: 100, offset });
    if (error) { _ultimoErroStorage = prefixo + ': ' + (error.message || JSON.stringify(error)); break; }
    if (!data || !data.length) break;
    for (const f of data) {
      if (!f || !f.name || !f.id) continue; // sem id = é subpasta
      saida.push({
        caminho: prefixo ? prefixo + '/' + f.name : f.name,
        bytes: Number(f.metadata && f.metadata.size ? f.metadata.size : 0),
        criado: f.created_at ? new Date(f.created_at).getTime() : 0,
        grupo: prefixo
      });
    }
    if (data.length < 100) break;
    offset += 100;
  }
  return saida;
}

// Mede o balde inteiro: quanto cada pasta ocupa e a lista completa de arquivos.
async function _mapaStorage() {
  if (!supabase) return null;
  const pastas = ['api', 'bot'];
  try {
    const { data: subs, error: eSubs } = await supabase.storage.from('wa-media').list('qr', { limit: 1000 });
    if (eSubs) _ultimoErroStorage = 'qr: ' + (eSubs.message || JSON.stringify(eSubs));
    for (const s of (subs || [])) if (s && s.name && !s.id) pastas.push('qr/' + s.name);
  } catch (e) { _ultimoErroStorage = 'qr: ' + e.message; }
  const grupos = {}, arquivos = [];
  for (const p of pastas) {
    const its = await _listaArquivos(p);
    let soma = 0;
    for (const a of its) { soma += a.bytes; arquivos.push(a); }
    grupos[p] = { arquivos: its.length, mb: +(soma / 1048576).toFixed(1) };
  }
  const bytes = arquivos.reduce((s, a) => s + a.bytes, 0);
  return { bytes, mb: +(bytes / 1048576).toFixed(1), grupos, arquivos };
}

// Quais arquivos do Storage as mensagens realmente usam (media_id). Devolve
// { set, ok } — ok=false quando a consulta falhou (aí NINGUÉM é tratado como órfão).
async function _midiaUsadaNoChat() {
  const usados = new Set();
  let ok = true;
  for (let de = 0; ; de += 1000) {
    const { data, error } = await supabase.from('messages').select('id, media_id')
      .not('media_id', 'is', null).order('id', { ascending: true }).range(de, de + 999);
    if (error) { ok = false; break; }
    if (!data || !data.length) break;
    for (const r of data) {
      const mid = String(r.media_id || '').trim();
      if (!mid) continue;
      // QR guarda o caminho completo (qr/...); a API oficial guarda o id cru e a cópia fica em api/<id>
      usados.add(mid.startsWith('qr/') || mid.startsWith('bot/') ? mid : 'api/' + mid);
      usados.add(mid);
    }
    if (data.length < 1000) break;
  }
  return { set: usados, ok };
}

// Poda por ESPAÇO: passou do teto, apaga até voltar a caber — PRIMEIRO os
// órfãos (que nenhuma mensagem usa), do mais antigo ao mais novo; só se ainda
// não couber é que mexe em arquivo usado no chat (e avisa no log).
async function _podaPorEspaco() {
  if (!supabase) return null;
  try {
    const mapa = await _mapaStorage();
    if (!mapa) return null;
    _espacoCache = { mb: mapa.mb, grupos: mapa.grupos, ts: Date.now() };
    const teto = COFRE_TETO_MB * 1048576;
    if (mapa.bytes <= teto) {
      _cofreCheio = false;
      _ultimaPoda = { quando: new Date().toISOString(), apagados: 0, mb: mapa.mb };
      return _ultimaPoda;
    }
    const alvo = teto * COFRE_ALVO_PCT;
    const usados = await _midiaUsadaNoChat();
    if (!usados.ok) { console.error('📦 Poda adiada: não consegui ler as mensagens (não apago às cegas)'); return { erro: 'mensagens indisponíveis' }; }
    const cand = mapa.arquivos.filter(a => a.grupo !== 'bot' && a.grupo !== 'qr/avatars'); // esses ficam sempre
    const orfaos = cand.filter(a => !usados.set.has(a.caminho)).sort((a, b) => a.criado - b.criado);
    const emUso  = cand.filter(a =>  usados.set.has(a.caminho)).sort((a, b) => a.criado - b.criado);
    const fila = orfaos.concat(emUso);
    let _avisouUso = false;
    let previsto = mapa.bytes;
    const lote = [];
    for (const a of fila) {
      if (previsto <= alvo) break;
      if (!_avisouUso && usados.set.has(a.caminho)) { _avisouUso = true; console.warn('📦 ATENÇÃO: órfãos não bastaram — poda vai alcançar arquivos usados no chat (mais antigos primeiro)'); }
      lote.push(a);
      previsto -= a.bytes;
    }
    let apagados = 0, liberado = 0;
    for (let i = 0; i < lote.length; i += 100) {
      const parte = lote.slice(i, i + 100);
      const { error } = await supabase.storage.from('wa-media').remove(parte.map(a => a.caminho));
      if (error) { console.error('📦 Poda falhou:', error.message); break; }
      apagados += parte.length;
      liberado += parte.reduce((s, a) => s + a.bytes, 0);
    }
    const restante = mapa.bytes - liberado;
    _cofreCheio = restante > teto;
    _espacoCache = { mb: +(restante / 1048576).toFixed(1), grupos: mapa.grupos, ts: Date.now() };
    _ultimaPoda = { quando: new Date().toISOString(), apagados, antes_mb: mapa.mb, depois_mb: _espacoCache.mb };
    console.log(`📦 Poda por espaço: ${apagados} arquivo(s) apagados — ${mapa.mb} MB → ${_espacoCache.mb} MB (teto ${COFRE_TETO_MB} MB)`);
    return _ultimaPoda;
  } catch (e) {
    console.error('📦 Poda por espaço:', e.message);
    return { erro: e.message };
  }
}

// Faxina completa: idade (183 dias) + teto de espaço. 5 min após subir e 1x/dia.
async function _faxinaCompleta() {
  await _limpaCopiasApi().catch(() => {});
  await _limpaMidiasAntigas().catch(() => {});
  return await _podaPorEspaco();
}
setTimeout(() => { _faxinaCompleta(); setInterval(_faxinaCompleta, 24 * 3600 * 1000); }, 5 * 60000);

// 🔑 Quem pode ver/forçar a faxina: quem está logada no CRM, quem manda o token
// de integração, ou — e isso é o socorro quando o Supabase restringe o projeto e
// o login para de funcionar — o token de administração do Railway:
//   ?admin=<ADMIN_TOKEN ou VERIFY_TOKEN das variáveis do Railway>
function _storageAuthOk(req, destrutivo) {
  if (req.owner && !destrutivo) return true; // ver números: qualquer conta logada
  if (req.owner && destrutivo && req.owner === OWNER_LEGADO) return true; // apagar: só a dona
  const adm = String(req.query.admin || '').trim();
  const espAdm = process.env.ADMIN_TOKEN || VERIFY_TOKEN;
  if (adm && espAdm && adm === espAdm) return true;
  const tok = String(req.query.token || '').trim()
    || String(req.headers['authorization'] || '').replace(/^Bearer\s+/i, '').trim();
  if (!tok) return false;
  // Token de integração: só LEITURA dos números (apagar exige a dona ou o token
  // de administração — antes qualquer cliente podia apagar arquivos de outro)
  if (destrutivo) return false;
  for (const k in _settings) if (k.startsWith('api_token::') && _settings[k] === tok) return true;
  return false;
}
const _storageAuthErro = { error: 'Sem permissão. Entre no CRM, ou use ?token=SEU_TOKEN (Configurações → Integração), ou ?admin=VERIFY_TOKEN (variável do Railway).' };

// 📊 Quanto o Storage está ocupando, pasta por pasta.
app.get('/storage-uso', async (req, res) => {
  if (!_storageAuthOk(req)) return res.status(401).json(_storageAuthErro);
  if (!supabase) return res.status(500).json({ error: 'Supabase indisponível' });
  try {
    const m = await _mapaStorage();
    _espacoCache = { mb: m.mb, grupos: m.grupos, ts: Date.now() };
    res.json({
      total_mb: m.mb,
      teto_mb: COFRE_TETO_MB,
      uso_pct: Math.round((m.bytes / (COFRE_TETO_MB * 1048576)) * 100),
      arquivo_max_mb: COFRE_ARQ_MAX_MB,
      cofre_cheio: _cofreCheio,
      por_pasta: m.grupos,
      ultima_poda: _ultimaPoda,
      erro_storage: _ultimoErroStorage
    });
  } catch (e) {
    res.status(500).json({ error: e.message, dica: /restrict|quota/i.test(e.message || '')
      ? 'O Supabase restringiu o projeto por cota. Libere espaço pelo painel do Supabase (Storage → wa-media → api).' : undefined });
  }
});

// 🧹 Faxina AGORA (idade + teto). Serve para destravar sem esperar o dia virar.
app.get('/storage-faxina', async (req, res) => {
  if (!_storageAuthOk(req, true)) return res.status(401).json(_storageAuthErro);
  if (!supabase) return res.status(500).json({ error: 'Supabase indisponível' });
  try {
    const r = await _faxinaCompleta();
    res.json({ ok: true, resultado: r, teto_mb: COFRE_TETO_MB });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// 🚨 SOCORRO: apaga uma fatia do cofre pela ORDEM DE CHEGADA (mais antigos
// primeiro), sem depender do teto. Use quando o projeto já está restrito:
//   /storage-emergencia?admin=SEU_TOKEN&quantos=2000&pasta=api
app.get('/storage-emergencia', async (req, res) => {
  if (!_storageAuthOk(req, true)) return res.status(401).json(_storageAuthErro);
  if (!supabase) return res.status(500).json({ error: 'Supabase indisponível' });
  const quantos = Math.min(Math.max(parseInt(req.query.quantos, 10) || 500, 1), 5000);
  const pasta = String(req.query.pasta || 'api').replace(/[^\w/-]/g, '').replace(/\/+$/, '') || 'api';
  if (pasta === 'bot' || pasta === 'qr/avatars') {
    return res.status(400).json({ error: 'Fotos de perfil e fotos dos bots não são apagadas por aqui.' });
  }
  try {
    const its = (await _listaArquivos(pasta)).sort((a, b) => a.criado - b.criado).slice(0, quantos);
    let apagados = 0, bytes = 0;
    for (let i = 0; i < its.length; i += 100) {
      const parte = its.slice(i, i + 100);
      const { error } = await supabase.storage.from('wa-media').remove(parte.map(a => a.caminho));
      if (error) return res.status(500).json({ error: error.message, apagados });
      apagados += parte.length;
      bytes += parte.reduce((s, a) => s + a.bytes, 0);
    }
    _cofreCheio = false;
    console.log(`🚨 Emergência: ${apagados} arquivo(s) de ${pasta} apagados (${Math.round(bytes / 1048576)} MB)`);
    res.json({ ok: true, pasta, apagados, liberado_mb: +(bytes / 1048576).toFixed(1) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// 🧹 ÓRFÃOS: arquivos do Storage que NENHUMA mensagem usa (mídia de grupo/status
// que era baixada e descartada). Apagar isso não muda nada no que aparece no CRM.
//   /storage-orfaos?admin=SEU_TOKEN            → só conta (não apaga)
//   /storage-orfaos?admin=SEU_TOKEN&apagar=1   → apaga
app.get('/storage-orfaos', async (req, res) => {
  if (!_storageAuthOk(req, String(req.query.apagar || '') === '1')) return res.status(401).json(_storageAuthErro);
  if (!supabase) return res.status(500).json({ error: 'Supabase indisponível' });
  try {
    // 1) tudo que está guardado nas pastas das contas QR (avatares fora)
    const mapa = await _mapaStorage();
    const qr = mapa.arquivos.filter(a => a.grupo.startsWith('qr/') && a.grupo !== 'qr/avatars');
    // 2) tudo que as mensagens realmente usam
    const u = await _midiaUsadaNoChat();
    const usados = u.set;
    const erroMsgs = u.ok ? null : 'falha ao ler mensagens';
    if (!u.ok && String(req.query.apagar || '') === '1') return res.status(409).json({ error: 'Não consegui ler as mensagens — por segurança, não apago nada.' });
    // Só considera órfão se a lista de mensagens veio (senão pararia de apagar tudo por engano)
    const orfaos = qr.filter(a => !usados.has(a.caminho));
    const mb = +(orfaos.reduce((s, a) => s + a.bytes, 0) / 1048576).toFixed(1);
    if (String(req.query.apagar || '') !== '1') {
      return res.json({ ok: true, modo: 'so_contagem', guardados_qr: qr.length, usados_no_chat: usados.size, orfaos: orfaos.length, orfaos_mb: mb,
        erro_storage: _ultimoErroStorage, erro_mensagens: erroMsgs, pastas: Object.keys(mapa.grupos) });
    }
    if (!usados.size && orfaos.length) {
      return res.status(409).json({ error: 'Nenhuma mensagem com mídia QR foi encontrada — por segurança, não apago nada assim. Confira o banco.' });
    }
    let apagados = 0;
    for (let i = 0; i < orfaos.length; i += 100) {
      const parte = orfaos.slice(i, i + 100);
      const { error } = await supabase.storage.from('wa-media').remove(parte.map(a => a.caminho));
      if (error) return res.status(500).json({ error: error.message, apagados, faltam: orfaos.length - apagados });
      apagados += parte.length;
    }
    _cofreCheio = false;
    console.log(`🧹 Órfãos apagados: ${apagados} (${mb} MB)`);
    res.json({ ok: true, modo: 'apagou', apagados, liberado_mb: mb, mantidos_no_chat: usados.size });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

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
  let _contaDaMidia = null;
  if (supabase && account_id) {
    const { data: account } = await supabase
      .from("accounts").select("token, owner, evolution_instance").eq("id", account_id).maybeSingle();
    _contaDaMidia = account || null;
    if (account?.token) token = account.token;
  }
  // 🔒 Arquivo do WhatsApp por QR: o caminho é "qr/<número>/arquivo" e só é
  // servido se pertencer ao MESMO número informado (antes bastava saber o
  // caminho para ver o arquivo de outra conta)
  if (String(mediaId).startsWith('qr/') && supabase) {
    const instDoArquivo = String(mediaId).split('/')[1] || '';
    // Quem é o dono do número que gerou o arquivo?
    let donoDoArquivo = null;
    try {
      const { data: dn } = await supabase.from('accounts').select('owner').eq('evolution_instance', instDoArquivo).maybeSingle();
      donoDoArquivo = dn ? (dn.owner || null) : null;
    } catch (_) {}
    // Só recusa quando dá para PROVAR que é de outra conta (imagem em <img> não
    // manda login, então nunca bloqueamos por falta de informação)
    const quemPede = req.owner || (_contaDaMidia && _contaDaMidia.owner) || null;
    if (donoDoArquivo && quemPede && String(donoDoArquivo).toLowerCase() !== String(quemPede).toLowerCase())
      return res.status(403).json({ error: 'Arquivo de outra conta' });
  }
  if (!token && !String(mediaId).startsWith('qr/')) return res.status(400).json({ error: "Token não encontrado" });

  // 🗄️ Cópia própria (6 meses): se o arquivo já está no NOSSO Storage, serve
  // de lá — funciona mesmo depois que a Meta apaga (30 dias) e é mais rápido.
  // A checagem agora é DIRETA (baixa a cópia): a busca por lista falhava às
  // vezes e o arquivo guardado era ignorado — parecia "sumido" antes da hora.
  let _servirDe = mediaId.startsWith('qr/') ? mediaId : null;
  let _blobCopia = null;
  if (!_servirDe && supabase) {
    try {
      const { data: b } = await supabase.storage.from('wa-media').download('api/' + mediaId);
      if (b) { _servirDe = 'api/' + mediaId; _blobCopia = b; }
    } catch (_) {}
  }

  // Mídia guardada no Supabase Storage (QR ou cópia da API) — com suporte a Range
  if (_servirDe) {
    const mediaId = _servirDe; // caminho dentro do bucket
    try {
      if (!supabase) return res.status(500).json({ error: 'Storage indisponível' });
      const { data: blob, error } = _blobCopia
        ? { data: _blobCopia, error: null }
        : await supabase.storage.from('wa-media').download(mediaId);
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
      // A mídia de um media_id NUNCA muda → cache forte (foto não pisca ao re-renderizar)
      res.setHeader('Cache-Control', 'public, max-age=604800, immutable');
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
    try { up = await fetchStream(false); }
    catch (e) {
      try { up = await fetchStream(true); } // URL pode ter expirado
      catch (e2) {
        // 🔁 PLANO B: o app às vezes manda a conta errada (ex.: abrindo pelos
        // detalhes do lead) — tenta o token de CADA conta cadastrada até achar
        // a dona do arquivo. Só roda quando o caminho normal já falhou.
        up = null;
        if (supabase) {
          try {
            let _qb = supabase.from("accounts").select("id, token").not("token", "is", null);
            // Só as contas do MESMO dono (antes tentava o token de TODOS os clientes)
            const _ownerBusca = (_contaDaMidia && _contaDaMidia.owner) || req.owner || null;
            if (_ownerBusca) _qb = _qb.eq('owner', _ownerBusca); else _qb = _qb.eq('id', account_id || '');
            const { data: accs } = await _qb;
            const vistos = new Set([token]);
            for (const a of (accs || [])) {
              if (!a.token || vistos.has(a.token)) continue;
              vistos.add(a.token);
              try {
                const ck = `${mediaId}_${a.token.substring(0, 20)}`;
                const url2 = await getMediaUrl(mediaId, a.token, ck, true);
                const h2 = { Authorization: `Bearer ${a.token}`, "User-Agent": "WhatsApp/2.0" };
                if (req.headers.range && download !== "1") h2.Range = req.headers.range;
                up = await axios.get(url2, { headers: h2, responseType: "stream", timeout: 30000, validateStatus: s => s === 200 || s === 206 });
                console.log(`🔁 Mídia ${mediaId}: baixada com o token da conta ${a.id} (a conta pedida não servia)`);
                break;
              } catch (_) {}
            }
          } catch (_) {}
        }
        if (!up) throw e2;
      }
    }

    // 🗄️ Veio da Meta = ainda não tínhamos cópia: guarda agora (vale por 6 meses)
    try { arquivaMidiaApi(mediaId, token, req.query.mime).catch(() => {}); } catch (_) {}

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
      // A mídia de um media_id NUNCA muda → cache forte (foto não pisca ao re-renderizar)
      res.setHeader("Cache-Control", "public, max-age=604800, immutable");
    }

    up.data.pipe(res);
    up.data.on("error", (e) => {
      console.error("❌ Stream de mídia interrompido:", e.message);
      try { res.destroy(); } catch (_) {}
    });
    // Se o navegador cancelar (fechou o vídeo, pulou trecho), corta o download da Meta
    res.on("close", () => { try { up.data.destroy(); } catch (_) {} });
  } catch (err) {
    const st = err.response?.status;
    const msgMeta = String(err.response?.data?.error?.message || err.message || '');
    console.error("❌ Erro ao baixar mídia:", mediaId, st || msgMeta);
    const naoAchou = st === 404 || /does not exist|Unsupported get request|cannot be loaded/i.test(msgMeta);
    if (!res.headersSent) res.status(naoAchou ? 410 : 500).json({
      error: naoAchou
        ? "O WhatsApp não entregou este arquivo (a Meta apaga arquivos antigos; se este é recente, use o QR Code ou me avise)."
        : "Falha ao baixar mídia"
    });
  }
});

// ── Lista todas as tags existentes (para sugestões e filtro) ──
app.get("/tags", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase.from("contacts").select("tags").eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  const set = new Set();
  (data || []).forEach(c => (c.tags || []).forEach(t => { if (t) set.add(t); }));
  _tagCatalog(req.owner).forEach(t => set.add(t)); // inclui tags criadas no gerenciador (catálogo DA CONTA)
  res.json(Array.from(set).sort((a, b) => a.localeCompare(b)));
});

// ── Busca por nome, telefone OU conteúdo das mensagens ──

// ═══════════════════════════════════════════════════════════════════════════
// 📝 TRANSCRIÇÃO DE ÁUDIO (Groq Whisper — mesma chave GROQ_API_KEY da IA do FAQ)
// O texto fica salvo em messages.transcript (rode atualizacao-transcricao.sql).
// ═══════════════════════════════════════════════════════════════════════════
// Bytes da mídia de uma mensagem: cópia no Storage (QR ou cofre da API) → Meta
async function _bytesDaMidia(msg) {
  const mid = String(msg.media_id || '');
  if (!mid) throw new Error('mensagem sem mídia');
  if (mid.startsWith('qr/')) {
    const { data, error } = await supabase.storage.from('wa-media').download(mid);
    if (error || !data) throw new Error('áudio não encontrado no Storage');
    return Buffer.from(await data.arrayBuffer());
  }
  try {
    const { data } = await supabase.storage.from('wa-media').download('api/' + mid);
    if (data) return Buffer.from(await data.arrayBuffer());
  } catch (_) {}
  let token = process.env.WHATSAPP_TOKEN;
  if (msg.account_id) {
    const { data: acc } = await supabase.from('accounts').select('token').eq('id', msg.account_id).maybeSingle();
    if (acc && acc.token) token = acc.token;
  }
  if (!token) throw new Error('token da conta não encontrado');
  const url = await getMediaUrl(mid, token, mid + '_' + (msg.account_id || ''), true);
  const r = await axios.get(url, { headers: { Authorization: 'Bearer ' + token, 'User-Agent': 'WhatsApp/2.0' },
    responseType: 'arraybuffer', timeout: 30000, maxContentLength: 25 * 1024 * 1024 });
  return Buffer.from(r.data);
}
// Converte para mp3 16 kHz mono (formato que o Whisper aceita com certeza)
function _paraMp3(buf) {
  return new Promise((resolve, reject) => {
    if (!_ffmpeg) return resolve(buf); // sem ffmpeg: tenta mandar como veio
    const os = require('os'), fs = require('fs'), path = require('path');
    const inFile = path.join(os.tmpdir(), 'tr_' + Date.now() + '_' + Math.random().toString(36).slice(2));
    const outFile = inFile + '.mp3';
    const cleanup = () => { try { fs.unlinkSync(inFile); } catch (_) {} try { fs.unlinkSync(outFile); } catch (_) {} };
    fs.writeFileSync(inFile, buf);
    _ffmpeg(inFile).noVideo().audioCodec('libmp3lame').audioBitrate('48k').audioChannels(1).audioFrequency(16000).format('mp3')
      .on('end', () => { try { const out = fs.readFileSync(outFile); cleanup(); resolve(out); } catch (e) { cleanup(); reject(e); } })
      .on('error', err => { cleanup(); reject(err); })
      .save(outFile);
  });
}
async function _transcreverBuffer(buf, ehMp3, mimeOrig) {
  if (!process.env.GROQ_API_KEY) throw new Error('Chave GROQ_API_KEY não configurada no Railway (é a mesma da IA do FAQ).');
  const FormData = require('form-data');
  const form = new FormData();
  const ext = ehMp3 ? 'mp3' : (String(mimeOrig || '').includes('ogg') || String(mimeOrig || '').includes('opus') ? 'ogg' : String(mimeOrig || '').includes('mp4') || String(mimeOrig || '').includes('m4a') ? 'm4a' : 'ogg');
  form.append('file', buf, { filename: 'audio.' + ext, contentType: ehMp3 ? 'audio/mpeg' : (mimeOrig || 'audio/ogg') });
  form.append('model', process.env.GROQ_WHISPER_MODEL || 'whisper-large-v3-turbo');
  form.append('language', 'pt');
  form.append('response_format', 'json');
  form.append('temperature', '0');
  const r = await axios.post('https://api.groq.com/openai/v1/audio/transcriptions', form, {
    headers: { ...form.getHeaders(), Authorization: 'Bearer ' + process.env.GROQ_API_KEY }, timeout: 90000,
    maxBodyLength: 30 * 1024 * 1024 });
  return String(r.data && r.data.text || '').trim();
}
const _transcrevendo = new Set();
app.post('/transcribe', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const id = req.body && req.body.message_id;
  if (!id) return res.status(400).json({ error: 'message_id obrigatório' });
  const { data: msg, error } = await supabase.from('messages').select('*').eq('id', id).eq('owner', req.owner).maybeSingle();
  if (error || !msg) return res.status(404).json({ error: 'Mensagem não encontrada' });
  if (msg.transcript) return res.json({ ok: true, transcript: msg.transcript, cached: true });
  const mime = String(msg.media_mime_type || '');
  if (msg.type !== 'audio' && !mime.startsWith('audio/')) return res.status(400).json({ error: 'Esta mensagem não é um áudio' });
  if (_transcrevendo.has(String(id))) return res.status(429).json({ error: 'Já estou transcrevendo este áudio…' });
  _transcrevendo.add(String(id));
  try {
    const bruto = await _bytesDaMidia(msg);
    let mp3 = bruto, ehMp3 = false;
    try { mp3 = await _paraMp3(bruto); ehMp3 = (mp3 !== bruto); } catch (e) { console.warn('📝 conversão mp3 falhou, mandando original:', e.message); }
    const texto = await _transcreverBuffer(mp3, ehMp3, mime);
    if (!texto) return res.json({ ok: true, transcript: '', vazio: true });
    const { error: upErr } = await supabase.from('messages').update({ transcript: texto }).eq('id', id);
    if (upErr) {
      // coluna ainda não existe? devolve o texto mesmo assim e avisa
      return res.json({ ok: true, transcript: texto, nao_salvo: true, aviso: 'Rode atualizacao-transcricao.sql no Supabase para o texto ficar salvo (' + upErr.message + ')' });
    }
    res.json({ ok: true, transcript: texto });
  } catch (e) {
    const m = e.response && e.response.data && e.response.data.error ? (e.response.data.error.message || JSON.stringify(e.response.data.error)) : e.message;
    console.error('📝 transcrição:', m);
    res.status(500).json({ error: 'Não consegui transcrever: ' + m });
  } finally { _transcrevendo.delete(String(id)); }
});

// ═══════════════════════════════════════════════════════════════════════════
// 🔎 BUSCA DENTRO DAS MENSAGENS: devolve as mensagens (não só os contatos)
// ═══════════════════════════════════════════════════════════════════════════
app.get('/search/messages', async (req, res) => {
  if (!supabase) return res.json([]);
  const raw = String(req.query.q || '').trim();
  if (raw.length < 2) return res.json([]);
  const term = raw.replace(/[,()%]/g, ' ').trim();
  const like = `%${term}%`;
  const { account_id } = req.query;
  const OW = req.owner || ' ';
  try {
    let q = supabase.from('messages').select('id, phone, content, transcript, direction, timestamp, type, account_id')
      .eq('owner', OW).or(`content.ilike.${like},transcript.ilike.${like}`)
      .order('timestamp', { ascending: false }).limit(60);
    if (account_id) q = q.eq('account_id', account_id);
    let { data, error } = await q;
    if (error && /transcript/i.test(error.message || '')) { // sem a coluna ainda
      let q2 = supabase.from('messages').select('id, phone, content, direction, timestamp, type, account_id')
        .eq('owner', OW).ilike('content', like).order('timestamp', { ascending: false }).limit(60);
      if (account_id) q2 = q2.eq('account_id', account_id);
      ({ data, error } = await q2);
    }
    if (error) return res.status(500).json({ error: error.message });
    const rows = data || [];
    const phones = [...new Set(rows.map(r => r.phone).filter(Boolean))];
    let nomes = {};
    if (phones.length) {
      const { data: cs } = await supabase.from('contacts').select('phone, name, account_id').eq('owner', OW).in('phone', phones);
      for (const c of (cs || [])) nomes[c.phone] = c;
    }
    res.json(rows.map(r => ({ ...r, name: (nomes[r.phone] || {}).name || null, contact_account_id: (nomes[r.phone] || {}).account_id || null })));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════
// ⬇ EXPORTAR LEADS (CSV para Excel) — respeita conta, etapa, etiqueta e período
// ═══════════════════════════════════════════════════════════════════════════
async function _exportarLeadsCsv(req, res) {
  if (!supabase) return res.status(500).send('Supabase não configurado');
  if (!req.owner) return res.status(401).send('Faça login no CRM');
  const src = Object.assign({}, req.query || {}, (req.body && typeof req.body === 'object') ? req.body : {});
  const { account_id, stage_id, tag, from, to } = src;
  const phones = Array.isArray(src.phones) ? src.phones : (src.phones ? String(src.phones).split(',') : null);
  const lista = phones ? phones.map(x => String(x).trim()).filter(Boolean) : null;
  const OW = req.owner;
  try {
    const COLS_FULL = 'phone, name, email, stage_id, tags, account_id, created_at, last_message_at, last_message_direction, notes, favorite';
    const COLS_MIN  = 'phone, name, stage_id, tags, account_id, created_at, last_message_at, last_message_direction';
    // Busca paginada (o PostgREST corta em 1000 linhas por padrão) e por lotes de telefones
    const buscar = async (cols) => {
      const lotes = lista ? [] : [null];
      if (lista) for (let k = 0; k < lista.length; k += 400) lotes.push(lista.slice(k, k + 400));
      const out = [];
      for (const lote of lotes) {
        for (let de = 0; de < 50000; de += 1000) {
          let q = supabase.from('contacts').select(cols).eq('owner', OW);
          if (account_id) q = q.eq('account_id', account_id);
          if (stage_id) q = q.eq('stage_id', stage_id);
          if (from) q = q.gte('created_at', from);
          if (to) q = q.lte('created_at', to + 'T23:59:59');
          if (lote) q = q.in('phone', lote);
          const { data, error } = await q.order('created_at', { ascending: false }).order('phone', { ascending: true }).range(de, de + 999);
          if (error) return { error };
          out.push(...(data || []));
          if (!data || data.length < 1000) break;
        }
      }
      return { data: out };
    };
    let { data, error } = await buscar(COLS_FULL);
    if (error && /email|notes|favorite/i.test(error.message || '')) ({ data, error } = await buscar(COLS_MIN));
    if (error) return res.status(500).send(error.message);
    let rows = data || [];
    if (tag) rows = rows.filter(r => (r.tags || []).includes(tag));
    const [{ data: stages }, { data: accs }] = await Promise.all([
      supabase.from('pipeline_stages').select('id, name'),
      supabase.from('accounts').select('id, name, phone_display')
    ]);
    const stN = {}; for (const s of (stages || [])) stN[s.id] = s.name;
    const acN = {}; for (const a of (accs || [])) acN[a.id] = a.name || a.phone_display || a.id;
    const fmtD = v => v ? new Date(v).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }) : '';
    // Aspas + proteção contra "injeção de fórmula" no Excel (célula começando com = + - @)
    const esc = v => { let t = String(v == null ? '' : v); if (/^[=+\-@]/.test(t)) t = "'" + t; t = t.replace(/"/g, '""'); return /[;"\n\r']/.test(t) ? '"' + t + '"' : t; };
    const cab = ['Nome', 'Telefone', 'E-mail', 'Etapa', 'Etiquetas', 'Conta WhatsApp', 'Cadastro', 'Última mensagem', 'Última direção', 'Favorito', 'Notas'];
    const linhas = rows.map(r => [
      r.name || '', r.phone || '', r.email || '', stN[r.stage_id] || '', (r.tags || []).join(', '), acN[r.account_id] || '',
      fmtD(r.created_at), fmtD(r.last_message_at), r.last_message_direction === 'inbound' ? 'Lead' : (r.last_message_direction === 'outbound' ? 'Você' : ''),
      r.favorite ? 'Sim' : '', (r.notes || '').replace(/\r?\n/g, ' ')
    ].map(esc).join(';'));
    const csv = '﻿' + cab.join(';') + '\r\n' + linhas.join('\r\n');
    const nome = 'leads_' + new Date().toISOString().slice(0, 10) + '.csv';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="' + nome + '"');
    res.send(csv);
  } catch (e) { res.status(500).send(e.message); }
}
app.get('/contacts/export', _exportarLeadsCsv);
app.post('/contacts/export', _exportarLeadsCsv);

// ═══════════════════════════════════════════════════════════════════════════
// 🔗 UNIFICAR CHATS DUPLICADOS (variantes do nono dígito) — SÓ dentro da MESMA
// conta (owner). Nunca cruza e-mails diferentes.
//   GET  /contacts/duplicates            → só lista (não mexe em nada)
//   POST /contacts/unify-duplicates      → unifica
// ═══════════════════════════════════════════════════════════════════════════
function _chaveNonoDigito(phone) {
  const p = String(phone || '').replace(/\D/g, '');
  if (/^55\d{2}9\d{8}$/.test(p)) return p.slice(0, 4) + p.slice(5); // tira o 9 → chave canônica
  if (/^55\d{2}[6-9]\d{7}$/.test(p)) return p;
  return null; // não é celular BR → não participa
}
async function _gruposDuplicados(owner) {
  const OW = owner || ' ';
  const data = [];
  for (let de = 0; de < 100000; de += 1000) {
    let r = await supabase.from('contacts')
      .select('phone, name, account_id, stage_id, tags, last_message_at, created_at, notes, email, favorite, avatar, unread_count')
      .eq('owner', OW).order('phone', { ascending: true }).range(de, de + 999);
    if (r.error && /notes|email|favorite|avatar|unread/i.test(r.error.message || '')) {
      r = await supabase.from('contacts').select('phone, name, account_id, stage_id, tags, last_message_at, created_at')
        .eq('owner', OW).order('phone', { ascending: true }).range(de, de + 999);
    }
    if (r.error) throw new Error(r.error.message);
    data.push(...(r.data || []));
    if (!r.data || r.data.length < 1000) break;
  }
  const grupos = {};
  for (const c of data) {
    const k = _chaveNonoDigito(c.phone);
    if (!k) continue;
    (grupos[k] = grupos[k] || []).push(c);
  }
  const dup = Object.values(grupos).filter(g => g.length > 1);
  // Quem fica: o que tem a conversa mais recente (empate → o com 9, formato atual)
  return dup.map(g => {
    const ord = [...g].sort((a, b) => {
      const ta = a.last_message_at ? new Date(a.last_message_at).getTime() : 0;
      const tb = b.last_message_at ? new Date(b.last_message_at).getTime() : 0;
      if (tb !== ta) return tb - ta;
      return String(b.phone).length - String(a.phone).length;
    });
    return { fica: ord[0], somem: ord.slice(1) };
  });
}
app.get('/contacts/duplicates', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  try {
    const g = await _gruposDuplicados(req.owner);
    res.json({ grupos: g.length, contatos_a_remover: g.reduce((s, x) => s + x.somem.length, 0),
      exemplos: g.slice(0, 20).map(x => ({ fica: { phone: x.fica.phone, name: x.fica.name }, somem: x.somem.map(y => ({ phone: y.phone, name: y.name })) })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/contacts/unify-duplicates', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const OW = req.owner;
  try {
    const grupos = await _gruposDuplicados(OW);
    let unificados = 0, msgsMovidas = 0, erros = [];
    for (const g of grupos) {
      const fica = g.fica;
      for (const dup of g.somem) {
        try {
          // 1) mensagens, tarefas e disparos passam para o telefone que fica
          const { count } = await supabase.from('messages').update({ phone: fica.phone }, { count: 'exact' }).eq('phone', dup.phone).eq('owner', OW);
          msgsMovidas += (count || 0);
          await supabase.from('tasks').update({ phone: fica.phone }).eq('phone', dup.phone).eq('owner', OW);
          try { await supabase.from('bot_runs').update({ contact_phone: fica.phone }).eq('contact_phone', dup.phone); } catch (_) {}
          // 2) completa o que faltava no que fica (nome, etiquetas, notas, e-mail, etapa)
          const patch = {};
          if ((!fica.name || /^\+?\d+$/.test(fica.name)) && dup.name && !/^\+?\d+$/.test(dup.name)) patch.name = dup.name;
          if (!fica.stage_id && dup.stage_id) patch.stage_id = dup.stage_id;
          if (!fica.email && dup.email) patch.email = dup.email;
          if (!fica.avatar && dup.avatar) patch.avatar = dup.avatar;
          if (dup.favorite && !fica.favorite) patch.favorite = true;
          const tags = [...new Set([...(fica.tags || []), ...(dup.tags || [])])];
          if (tags.length !== (fica.tags || []).length) patch.tags = tags;
          if (dup.notes && String(dup.notes).trim()) patch.notes = (fica.notes ? fica.notes + '\n\n' : '') + dup.notes;
          if ((dup.unread_count || 0) > 0) patch.unread_count = (fica.unread_count || 0) + dup.unread_count;
          if (Object.keys(patch).length) {
            const { error: pe } = await supabase.from('contacts').update(patch).eq('phone', fica.phone).eq('owner', OW);
            if (pe && /email|avatar|notes|favorite|unread/i.test(pe.message || '')) { // colunas opcionais ausentes
              delete patch.email; delete patch.avatar; delete patch.notes; delete patch.favorite; delete patch.unread_count;
              if (Object.keys(patch).length) await supabase.from('contacts').update(patch).eq('phone', fica.phone).eq('owner', OW);
            }
            Object.assign(fica, patch);
          }
          // 3) apaga o duplicado (só desta conta)
          const { error: de } = await supabase.from('contacts').delete().eq('phone', dup.phone).eq('owner', OW);
          if (de) throw new Error(de.message);
          unificados++;
        } catch (e) { erros.push(dup.phone + ': ' + e.message); }
      }
    }
    console.log(`🔗 Unificação (${OW}): ${unificados} contato(s) unificados, ${msgsMovidas} mensagens movidas`);
    res.json({ ok: true, unificados, mensagens_movidas: msgsMovidas, erros });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

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
  // Nome NÃO é obrigatório: sem texto, a tarefa nasce como "Tarefa"
  const titleFinal = String(title || '').trim() || 'Tarefa';
  const { data, error } = await supabase.from("tasks")
    .insert({ phone: phone || null, account_id: account_id || null, title: titleFinal, due_at: due_at || null, notes: notes || null, owner: req.owner || null, created_at: new Date().toISOString() })
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
  const cleanPhone = String(phone).replace(/\D/g, '');
  if (cleanPhone.length < 8) return res.status(400).json({ error: "Número de celular inválido" });
  // UNIFICAÇÃO: se o número JÁ existe (com OU sem o nono dígito), reaproveita o
  // registro existente — o nome vira o último informado e o chat continua UM só
  let phoneFinal = cleanPhone;
  try { phoneFinal = (await resolveExistingPhone(cleanPhone, req.owner)) || cleanPhone; } catch (_) {}
  const { data, error } = await supabase.from("contacts")
    .upsert({ phone: phoneFinal, name, account_id: account_id || null, owner: req.owner || null, last_message_at: new Date().toISOString() }, { onConflict: "owner,phone" })
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

// 🔐 As rotas do n8n agora EXIGEM o token de integração (Configurações →
// Integração no CRM): envie como ?token=SEU_TOKEN na URL ou no header
// Authorization: Bearer SEU_TOKEN — sem ele, a porta fica trancada.
function _n8nAuthOk(req, owner) {
  const tok = String(req.headers['authorization'] || '').replace(/^Bearer\s+/i, '').trim() || String(req.query.token || '').trim();
  const esperado = _settings['api_token::' + (owner || ' ')];
  if (!esperado || !tok) return false;
  try { const c = require('crypto'); const a = Buffer.from(String(tok)), b = Buffer.from(String(esperado)); return a.length === b.length && c.timingSafeEqual(a, b); } catch (_) { return false; }
}
const _n8nAuthErro = { error: 'Token de integração ausente ou inválido. Gere/copie o token no CRM (Configurações → Integração) e envie como ?token=SEU_TOKEN na URL ou no header Authorization: Bearer SEU_TOKEN.' };

// ── Importar lead via n8n / planilha (mapeia ID da etapa → stage_id) ──
// Aceita 1 lead OU um array de leads. Campos flexíveis:
//   name | title | "Lead Titulo"   →  nome
//   phone | celular | "Celular"     →  telefone
//   id | "ID" | stage_external_id   →  ID da etapa (external_id de pipeline_stages)
//   account_id (opcional)           →  vincula a uma conta WhatsApp
// Núcleo da importação (usado pelo n8n E pela planilha do Google).
//   opts.soNovos = true → quem já existe no CRM não é tocado (não volta de etapa)
async function _importarLeads(items, owner, opts) {
  opts = opts || {};
  const onProg = typeof opts.onProgress === 'function' ? opts.onProgress : null;
  let _idx = 0;
  const stageCache = {};
  let imported = 0, atualizados = 0, pulados = 0;
  const errors = [];
  let existentes = null;
  if (opts.soNovos) {
    const fones = (items || []).map(it => String(it.phone || it.celular || it["Celular"] || "").replace(/\D/g, "")).filter(p => p.length >= 8);
    existentes = new Set();
    for (let k = 0; k < fones.length; k += 500) {
      const { data } = await supabase.from('contacts').select('phone').eq('owner', owner).in('phone', fones.slice(k, k + 500));
      for (const c of (data || [])) existentes.add(c.phone);
    }
  }
  for (const it of (items || [])) {
    // remove um "=" no início (marcador de expressão do n8n que às vezes vaza como texto)
    const name  = (String(it.name || it.title || it["Lead Titulo"] || "").replace(/^=+\s*/, "").trim()) || "Lead";
    const phone = String(it.phone || it.celular || it["Celular"] || "").replace(/\D/g, "");
    const extId = String(it.id || it["ID"] || it.stage_external_id || "").replace(/^=+\s*/, "").trim();
    const account_id = it.account_id || null;
    _idx++;
    if (onProg) { try { onProg({ feitos: _idx, importados: imported, pulados, erros: errors.length }); } catch (_) {} }
    if (phone.length < 8) { errors.push({ phone, error: "telefone inválido" }); continue; }
    if (existentes && existentes.has(phone)) { pulados++; continue; }

    let stage_id = null;
    if (extId) {
      if (stageCache[extId] === undefined) {
        const { data: st } = await supabase.from("pipeline_stages").select("id").eq("external_id", extId).eq("owner", owner).maybeSingle();
        stageCache[extId] = st ? st.id : null;
      }
      stage_id = stageCache[extId];
    }

    const row = { phone, name, owner };
    if (account_id) row.account_id = account_id;
    if (stage_id) row.stage_id = stage_id;
    // Não define last_message_* → o lead aparece só no Pipeline até iniciar conversa
    const { error: e } = await supabase.from("contacts").upsert(row, { onConflict: "owner,phone" });
    if (e) errors.push({ phone, error: e.message }); else imported++;
  }
  return { imported, atualizados, pulados, errors };
}

app.post("/import/lead", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const items = Array.isArray(req.body) ? req.body
              : (Array.isArray(req.body.leads) ? req.body.leads : [req.body]);
  const n8nOwner = (String(req.query.owner||'').trim()) || (!Array.isArray(req.body) && req.body.owner) || 'elianecezaroliveira@gmail.com';
  if (!_n8nAuthOk(req, n8nOwner)) return res.status(401).json(_n8nAuthErro);
  const r = await _importarLeads(items, n8nOwner);
  console.log(`📥 n8n importou ${r.imported} lead(s)` + (r.errors.length ? `, ${r.errors.length} erro(s)` : ""));
  res.json({ success: true, imported: r.imported, errors: r.errors });
});


// ═══════════════════════════════════════════════════════════════════════════
// ⏳ GOTEJAMENTO (substitui o 2º fluxo do n8n): move leads de uma etapa para
// outra UM POR VEZ, com intervalo aleatório entre eles (ex.: 3,5 a 5 min), para
// que os bots da etapa de destino disparem espaçados. Regras por conta em
// settings drip_rules::owner = [{ id, nome, de, para, min_seg, max_seg, ativo,
// hora_ini, hora_fim, next_at, last }]
// ═══════════════════════════════════════════════════════════════════════════
function _dripRegras(owner) { try { const a = JSON.parse(_cfg('drip_rules', owner) || '[]'); return Array.isArray(a) ? a : []; } catch (_) { return []; } }
async function _dripSalva(owner, regras) {
  const k = 'drip_rules::' + (owner || ' ');
  const value = JSON.stringify(regras);
  await supabase.from('settings').upsert({ key: k, value, updated_at: new Date().toISOString() });
  _settings[k] = value;
}
// O robô só grava os campos DELE (next_at/last/movidos) — se você pausou/editou a
// regra enquanto ele movia um lead, a sua alteração vence
async function _dripSalvaProgresso(owner, regrasDoTick) {
  const atuais = _dripRegras(owner);
  for (const r of regrasDoTick) {
    const a = atuais.find(x => x.id === r.id);
    if (!a) continue;
    if (r.manual === false && a.manual) a.manual = false;
    // Só desliga o Automático quando FOI O ROBÔ que decidiu (etapa apagada) — antes,
    // clicar em "Automático: ligado" durante um tick era desfeito sem aviso
    if (r._desligarAgendado) { a.agendado = false; a.ativo = false; }
    // 🛑 Você mexeu na regra enquanto o robô movia (Parar/zerar/editar): a SUA
    // alteração vence no que é CONFIGURAÇÃO (contador, ciclo).
    if (Number(a.reset_seq || 0) !== Number(r.reset_seq || 0)) {
      // Mas o que ACONTECEU de verdade tem de ficar registrado: se um lead foi
      // movido agora, o horário dele é gravado e o próximo fica o MAIS TARDE
      // entre os dois cálculos. (Sem isso, dois leads podiam sair com 5s.)
      if (r.last && r.last.phone) {
        a.last = r.last;
        const t1 = a.next_at ? new Date(a.next_at).getTime() : 0;
        const t2 = r.next_at ? new Date(r.next_at).getTime() : 0;
        a.next_at = new Date(Math.max(t1, t2) || Date.now()).toISOString();
      }
      continue;
    }
    a.next_at = r.next_at; a.last = r.last; a.movidos = r.movidos; a.ciclo_fechado = !!r.ciclo_fechado;
  }
  await _dripSalva(owner, atuais);
}
function _dripDentroDaJanela(r) {
  // ⏹ "Parar" com o automático ligado: fica parada até o horário marcado (fim da
  // janela de hoje) — depois o automático retoma sozinho na próxima janela
  if (r.parado_ate && Date.now() < new Date(r.parado_ate).getTime()) return false;
  const agora = new Date(Date.now() - 3 * 3600000); // horário de Brasília
  // dias da semana (0=dom … 6=sáb); lista vazia = todos os dias
  if (Array.isArray(r.dias) && r.dias.length && !r.dias.includes(agora.getUTCDay())) return false;
  if (!r.hora_ini && !r.hora_fim) return true;
  const hm = agora.getUTCHours() * 60 + agora.getUTCMinutes();
  const toMin = t => { const m = /^(\d{1,2}):(\d{2})$/.exec(String(t || '')); return m ? (+m[1]) * 60 + (+m[2]) : null; };
  const a = toMin(r.hora_ini), b = toMin(r.hora_fim);
  if (a == null && b == null) return true;
  if (a != null && b != null) return a <= b ? (hm >= a && hm < b) : (hm >= a || hm < b);
  // só "começa às": a partir daquela hora segue até esvaziar a fila (varando a madrugada);
  // se a fila estiver vazia às 00:00 e chegar lead novo, espera a hora de começar
  if (a != null) {
    if (hm >= a) return true;
    // Antes da hora de começar: só continua se AINDA está no meio de uma fila que
    // varou a madrugada. A folga acompanha o intervalo da regra (o fixo de 30 min
    // cortava filas com intervalo maior, ex.: 35–50 min).
    const folga = Math.max(30 * 60000, _dripMinMax(r).maxS * 2000 + 120000);
    return !!(r.last && r.last.quando && (Date.now() - new Date(r.last.quando).getTime()) < folga && !r.last.vazio);
  }
  return hm < b;
}
// 📱 Quantos NÚMEROS o bot da etapa de destino usa: o intervalo é DIVIDIDO por
// eles, para que CADA número mantenha o ritmo que você configurou.
// Ex.: 35–50 min com 2 números → um lead a cada 17,5–25 min (cada número volta a
// enviar só depois de 35–50 min).
function _dripDiv(r) { const n = parseInt(r && r.numeros, 10); return (n >= 1 && n <= 20) ? n : 1; }
function _dripMinMax(r) {
  const div = _dripDiv(r);
  const minS = Math.max(1, Math.round((Math.max(1, Number(r.min_seg) || 210)) / div));
  const maxS = Math.max(minS, Math.round((Math.max(1, Number(r.max_seg) || 300)) / div));
  return { minS, maxS };
}
// Etapas existentes do dono (cache de 60s): o gotejamento NUNCA move um lead para
// uma etapa apagada (o lead sumiria do quadro)
const _dripStagesCache = {};
async function _dripEtapasDoDono(owner) {
  const c = _dripStagesCache[owner];
  if (c && Date.now() - c.ts < 60000) return c.set;
  const { data, error } = await supabase.from('pipeline_stages').select('id').eq('owner', owner);
  // Falha na consulta: NÃO guarda lista vazia (isso desligava a proteção de etapa
  // apagada por 60s). Mantém a lista anterior; se não houver, devolve null.
  if (error || !Array.isArray(data)) { console.error('⏳ não consegui ler as etapas do gotejamento:', (error && error.message) || 'resposta vazia'); return c ? c.set : null; }
  const set = new Set(data.map(x => String(x.id)));
  _dripStagesCache[owner] = { set, ts: Date.now() };
  return set;
}
const _dripLock = new Set();
async function _dripTick() {
  if (!supabase) return;
  for (const k in _settings) {
    if (!k.startsWith('drip_rules::')) continue;
    const owner = k.slice('drip_rules::'.length);
    if (!owner.trim() || _dripLock.has(owner)) continue;
    _dripLock.add(owner);
    try {
      const regras = _dripRegras(owner);
      let mudou = false;
      const etapas = await _dripEtapasDoDono(owner);
      for (const r of regras) {
        if (!r.de || !r.para || r.de === r.para) continue;
        // Etapa apagada (origem ou destino): a regra PARA sozinha e avisa
        if (etapas && etapas.size && (!etapas.has(String(r.de)) || !etapas.has(String(r.para)))) {
          if (r.manual || r.agendado) {
            r.manual = false; r.agendado = false; r.ativo = false; r._desligarAgendado = true; mudou = true;
            r.last = { quando: new Date().toISOString(), erro: 'Etapa da regra foi apagada — gotejamento desligado' };
            try { addNotice(owner, `⏳ O gotejamento "${r.nome || r.id}" foi DESLIGADO: uma das etapas dele foi apagada.`, 'drip-etapa:' + r.id); } catch (_) {}
          }
          continue;
        }
        // Roda se: ▶ iniciado à mão (ignora dia/horário, até esvaziar ou pausar)
        //      ou: 🕐 automático ligado E dentro dos dias/horários
        const rodaManual = !!r.manual;
        const rodaAuto = !!r.agendado && _dripDentroDaJanela(r);
        if (!rodaManual && !rodaAuto) continue;
        const nx = r.next_at ? new Date(r.next_at).getTime() : 0;
        if (nx > Date.now()) continue;
        // 🛡️ Garantia absoluta do intervalo mínimo: mesmo que o agendamento se perca
        // (edição/pausa no mesmo instante, redeploy), nunca move antes de min_seg do último
        const _minGuard = _dripMinMax(r).minS * 1000;
        if (r.last && r.last.quando && !r.last.vazio && (Date.now() - new Date(r.last.quando).getTime()) < _minGuard) continue;
        // 🔒 PALAVRA FINAL É DO BANCO. Relê a regra agora mesmo e só move se ELA
        // ainda estiver ligada. Isso resolve dois casos reais:
        //  • você desligou o Automático e a memória deste servidor ainda estava velha;
        //  • dois servidores no ar durante um deploy (um já agendou o próximo).
        try {
          const { data: fresco } = await supabase.from('settings').select('value').eq('key', 'drip_rules::' + owner).maybeSingle();
          const rf = fresco && fresco.value ? (JSON.parse(fresco.value) || []).find(x => x.id === r.id) : null;
          if (!rf) { r.manual = false; r.agendado = false; continue; } // regra apagada
          // Espelha o estado real do banco na cópia da memória
          r.manual = !!rf.manual; r.agendado = !!rf.agendado; r.parado_ate = rf.parado_ate || null;
          r.de = rf.de || r.de; r.para = rf.para || r.para;
          r.min_seg = rf.min_seg || r.min_seg; r.max_seg = rf.max_seg || r.max_seg; r.numeros = rf.numeros || r.numeros;
          r.dias = Array.isArray(rf.dias) ? rf.dias : r.dias; r.hora_ini = rf.hora_ini || ''; r.hora_fim = rf.hora_fim || '';
          r.movidos = rf.movidos || 0; r.last = rf.last || r.last; r.reset_seq = rf.reset_seq || 0;
          const ligadaAgora = !!rf.manual || (!!rf.agendado && _dripDentroDaJanela(r));
          if (!ligadaAgora) { mudou = true; continue; } // DESLIGADA no banco: não move nada
          if (rf.next_at && new Date(rf.next_at).getTime() > Date.now()) { r.next_at = rf.next_at; continue; }
        } catch (e) { console.error('⏳ leitura de segurança do gotejamento:', e.message); continue; }
        // 1) agenda o PRÓXIMO e grava JÁ (antes de mover) — se cair no meio, não repete
        const { minS, maxS } = _dripMinMax(r); // já dividido pela quantidade de números
        const espera = Math.floor(Math.random() * (maxS - minS + 1)) + minS;
        r.next_at = new Date(Date.now() + espera * 1000).toISOString();
        await _dripSalvaProgresso(owner, regras);
        // 2) pega UM lead da etapa de origem
        const { data: leads } = await supabase.from('contacts').select('phone, name')
          .eq('owner', owner).eq('stage_id', r.de).order('last_message_at', { ascending: false, nullsFirst: false }).limit(1);
        const lead = leads && leads[0];
        mudou = true;
        if (!lead) {
          // Fila acabou: o ciclo fecha. O total movido vira histórico e a regra
          // volta ao estado PARADA — sem "progresso pendente" (era isso que fazia
          // aparecer Retomar/Parar quando entravam leads novos pela planilha).
          r.last = { quando: new Date().toISOString(), vazio: true, total_ciclo: r.movidos || 0 };
          r.ciclo_fechado = true;
          if (r.manual) r.manual = false;
          continue;
        }
        // 3) move (só se ainda estiver na etapa de origem — evita mover quem você acabou de arrastar à mão)
        const { data: mv, error } = await supabase.from('contacts').update({ stage_id: r.para })
          .eq('phone', lead.phone).eq('owner', owner).eq('stage_id', r.de).select('phone');
        if (error) { r.last = { quando: new Date().toISOString(), erro: error.message }; continue; }
        // O lead saiu da etapa entre a busca e o movimento (você arrastou à mão, ou
        // outra regra levou). NÃO é "fila vazia": marca como pulado para não derrubar
        // a trava de intervalo nem encerrar uma fila que varou a madrugada.
        if (!mv || !mv.length) { r.last = { quando: new Date().toISOString(), pulado: true }; continue; }
        try { await fireStageBots(lead.phone, r.para, owner); } catch (e) { console.error('drip fireStageBots:', e.message); }
        r.movidos = (r.movidos || 0) + 1; r.ciclo_fechado = false;
        r.last = { quando: new Date().toISOString(), phone: lead.phone, name: lead.name || '', proximo_em_seg: espera, motivo: rodaManual ? 'manual' : 'automatico' };
        console.log(`⏳ Gotejamento "${r.nome || r.id}" [${rodaManual ? 'MANUAL' : 'AUTOMÁTICO'}] (${owner}): ${lead.phone} movido; próximo em ${espera}s`);
      }
      if (mudou) await _dripSalvaProgresso(owner, regras);
    } catch (e) { console.error('⏳ drip:', e.message); }
    finally { _dripLock.delete(owner); }
  }
}
// Confere a cada 2 s → o intervalo real fica entre o mínimo e o máximo (+ até 2 s)
setTimeout(() => { _dripTick(); setInterval(_dripTick, 2000); }, 60000);

app.get('/drip', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const regras = _dripRegras(req.owner);
  // quantos leads ainda esperam em cada etapa de origem
  const contagens = {};
  for (const r of regras) {
    if (!r.de || contagens[r.de] !== undefined) continue;
    const { count } = await supabase.from('contacts').select('phone', { count: 'exact', head: true }).eq('owner', req.owner).eq('stage_id', r.de);
    contagens[r.de] = count || 0;
  }
  // diz para a tela se cada regra está dentro da janela agora (dia/hora)
  const agora = new Date(Date.now() - 3 * 3600000);
  const hhmm = String(agora.getUTCHours()).padStart(2, '0') + ':' + String(agora.getUTCMinutes()).padStart(2, '0');
  const janela = {};
  for (const r of regras) janela[r.id] = _dripDentroDaJanela(r);
  res.json({ regras, contagens, janela, agora_brasilia: hhmm, dia_semana: agora.getUTCDay() });
});
app.put('/drip', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  // Modo 1 regra: mexe SÓ nela e mantém as outras exatamente como estão no banco.
  // (Antes a tela mandava a lista inteira; se ela estivesse desatualizada, um
  // ligar/desligar podia RESSUSCITAR o estado antigo de outra regra.)
  const umaSo = req.body && req.body.regra && req.body.regra.id ? req.body.regra : null;
  const guardadas = _dripRegras(req.owner);
  // Remover UMA regra pelo id (sem reenviar a lista inteira, que podia estar velha)
  const removerId = req.body && req.body.remover ? String(req.body.remover) : null;
  // Criar UMA regra nova sem reenviar a lista (uma lista velha podia apagar as outras)
  const nova = req.body && req.body.nova ? req.body.nova : null;
  if (removerId) {
    const restantes = guardadas.filter(x => String(x.id) !== removerId);
    await _dripSalva(req.owner, restantes);
    return res.json({ ok: true, regras: restantes });
  }
  const novas = umaSo
    ? guardadas.map(x => (String(x.id) === String(umaSo.id) ? Object.assign({}, x, umaSo) : x))
    : (nova ? guardadas.concat([Object.assign({}, nova, { id: undefined })])
            : (Array.isArray(req.body && req.body.regras) ? req.body.regras : null));
  // Regra já removida (em outra aba/aparelho): NÃO ressuscita — só avisa
  if (umaSo && !guardadas.some(x => String(x.id) === String(umaSo.id)))
    return res.status(404).json({ error: 'Esta regra foi removida. Atualize a tela.' });
  if (!novas) return res.status(400).json({ error: 'regras[] obrigatório' });
  if (novas.length > 50) return res.status(400).json({ error: 'Máximo de 50 regras.' });
  const antigas = guardadas;
  // 🔒 As etapas precisam ser DESTA conta (nada de mover leads para etapa de outro dono)
  let idsValidos = null;
  try {
    const { data: sts } = await supabase.from('pipeline_stages').select('id').eq('owner', req.owner);
    if (Array.isArray(sts)) idsValidos = new Set(sts.map(x => String(x.id)));
  } catch (_) {}
  const _hhmm = v => (/^([01]?\d|2[0-3]):[0-5]\d$/.test(String(v || '')) ? String(v) : '');
  const limpas = novas.map(r => {
    const a = antigas.find(x => x.id === r.id) || {};
    const minN = Math.max(1, parseInt(r.min_seg, 10) || 210), maxN = Math.max(minN, Math.max(1, parseInt(r.max_seg, 10) || 300));
    const numN = (() => { const n = parseInt(r.numeros, 10); return (n >= 1 && n <= 20) ? n : ((a.numeros >= 1 && a.numeros <= 20) ? a.numeros : 1); })();
    // Mudou o intervalo (ou a quantidade de números) → o PRÓXIMO já obedece o novo
    let nextAt = a.next_at || null;
    if (a.id && (Number(a.min_seg) !== minN || Number(a.max_seg) !== maxN || Number(a.numeros || 1) !== numN)) {
      // Base = último movimento real; se não houver, conta a partir de AGORA.
      // (Antes ficava null e o próximo lead saía no tick seguinte, sem intervalo.)
      const base = (a.last && a.last.quando && !a.last.vazio) ? new Date(a.last.quando).getTime() : Date.now();
      const mm = _dripMinMax({ min_seg: minN, max_seg: maxN, numeros: numN });
      const alvo = base + (Math.floor(Math.random() * (mm.maxS - mm.minS + 1)) + mm.minS) * 1000;
      // Se o alvo já passou, conta um intervalo mínimo INTEIRO a partir de agora
      nextAt = new Date(Math.max(alvo, Date.now() + mm.minS * 1000)).toISOString();
    }
    return {
      id: String(r.id || ('drip_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7))),
      nome: String(r.nome || '').slice(0, 60),
      de: String(r.de || ''), para: String(r.para || ''),
      // reset_seq muda a cada alteração SUA que o robô não pode desfazer
      reset_seq: Number(a.reset_seq || 0) + ((r.zerar_movidos || Number(a.min_seg) !== minN || Number(a.max_seg) !== maxN || Number(a.numeros || 1) !== numN || String(a.de || '') !== String(r.de || '') || String(a.para || '') !== String(r.para || '')) ? 1 : 0),
      // ciclo_fechado: a fila acabou e o ciclo terminou (some o "progresso pendente")
      ciclo_fechado: r.zerar_movidos ? false : (r.ciclo_fechado !== undefined ? !!r.ciclo_fechado : !!a.ciclo_fechado),
      min_seg: minN, max_seg: maxN, numeros: numN,
      manual: !!r.manual,
      agendado: (r.agendado !== undefined) ? !!r.agendado : (a.agendado !== undefined ? !!a.agendado : !!a.ativo),
      ativo: !!r.manual || ((r.agendado !== undefined) ? !!r.agendado : (a.agendado !== undefined ? !!a.agendado : !!a.ativo)),
      hora_ini: _hhmm(r.hora_ini), hora_fim: _hhmm(r.hora_fim),
      dias: Array.isArray(r.dias) ? r.dias.map(d => parseInt(d, 10)).filter(d => d >= 0 && d <= 6) : [],
      next_at: nextAt, // mantém o próximo horário (pausar/ligar não encurta o intervalo); intervalo novo → recalculado
      parado_ate: (r.parado_ate && !isNaN(Date.parse(r.parado_ate)) && Date.parse(r.parado_ate) > Date.now()) ? new Date(r.parado_ate).toISOString() : null,
      movidos: r.zerar_movidos ? 0 : (a.movidos || 0), last: r.zerar_movidos ? null : (a.last || null)
    };
  });
  const recusadas = [];
  const limpas2 = limpas.filter(r => {
    if (!r.de || !r.para || r.de === r.para) { recusadas.push('origem e destino inválidos'); return false; }
    // Etapa que não existe mais: só recusa a regra que está sendo criada/alterada
    // agora. As demais continuam salvas (desligadas) para poderem ser recuperadas.
    const tocada = umaSo ? String(r.id) === String(umaSo.id) : true;
    const eraConhecida = antigas.some(x => String(x.id) === String(r.id));
    if (idsValidos && tocada && !(idsValidos.has(r.de) && idsValidos.has(r.para))) {
      if (!eraConhecida) { recusadas.push('a etapa escolhida não existe mais'); return false; }
      return true;
    }
    return true;
  });
  if (recusadas.length && limpas2.length < limpas.length && !umaSo && !removerId)
    return res.status(400).json({ error: 'Não consegui salvar: ' + recusadas[0] + '.' });
  await _dripSalva(req.owner, limpas2);
  res.json({ ok: true, regras: limpas2 });
});

// ═══════════════════════════════════════════════════════════════════════════
// 📊 PLANILHA DO GOOGLE → PIPELINE (substitui o fluxo do n8n)
// Segurança: o servidor entra no Google com uma CONTA DE SERVIÇO (e-mail robô)
// cuja chave fica SÓ na variável GOOGLE_SA_JSON do Railway. Você compartilha a
// planilha com esse e-mail como leitor — o robô não enxerga mais nada do Drive.
// Configuração por conta em settings sheets_sync::owner:
//   { spreadsheet_id, sheet_name, auto, atualizar_etapa, last }
// ═══════════════════════════════════════════════════════════════════════════
function _googleSA() {
  try {
    const raw = process.env.GOOGLE_SA_JSON;
    if (!raw) return null;
    const j = JSON.parse(raw);
    if (!j.client_email || !j.private_key) return null;
    j.private_key = String(j.private_key).replace(/\\n/g, '\n');
    return j;
  } catch (_) { return null; }
}
let _gTok = null; // { token, exp }
async function _googleAccessToken() {
  const sa = _googleSA();
  if (!sa) throw new Error('GOOGLE_SA_JSON não configurada no Railway');
  if (_gTok && _gTok.exp > Date.now() + 60000) return _gTok.token;
  const crypto = require('crypto');
  const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const header = b64({ alg: 'RS256', typ: 'JWT' });
  const claim = b64({ iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets.readonly',
    aud: 'https://oauth2.googleapis.com/token', iat: now, exp: now + 3600 });
  const sig = crypto.sign('RSA-SHA256', Buffer.from(header + '.' + claim), sa.private_key).toString('base64url');
  const jwt = header + '.' + claim + '.' + sig;
  const r = await axios.post('https://oauth2.googleapis.com/token',
    new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt }).toString(),
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
  _gTok = { token: r.data.access_token, exp: Date.now() + (Number(r.data.expires_in || 3600) * 1000) };
  return _gTok.token;
}
function _sheetsIdDeUrl(v) {
  const t = String(v || '').trim();
  const m = /\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/.exec(t);
  return m ? m[1] : t.replace(/[^a-zA-Z0-9_-]/g, '');
}
async function _lerPlanilha(spreadsheetId, sheetName) {
  const tok = await _googleAccessToken();
  const range = encodeURIComponent("'" + String(sheetName || 'Página1').replace(/'/g, "''") + "'!A1:Z5000");
  let r;
  try {
    r = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${range}`,
      { headers: { Authorization: 'Bearer ' + tok }, params: { valueRenderOption: 'FORMATTED_VALUE' }, timeout: 30000 });
  } catch (e) {
    const st = e.response && e.response.status;
    const msg = e.response && e.response.data && e.response.data.error && e.response.data.error.message || e.message;
    if (st === 403 && /has not been used|is disabled|SERVICE_DISABLED|accessNotConfigured/i.test(msg || '')) throw new Error('A Google Sheets API está DESLIGADA no projeto do robô. Ative em https://console.cloud.google.com/apis/library/sheets.googleapis.com e tente de novo em 1 minuto. Detalhe: ' + msg);
    if (st === 403) throw new Error('Sem acesso à planilha — compartilhe-a (como leitor) com o e-mail do robô: ' + (_googleSA() || {}).client_email + '. Detalhe do Google: ' + msg);
    if (st === 404) throw new Error('Planilha não encontrada — confira o link');
    if (st === 400) throw new Error('Aba não encontrada ("' + sheetName + '") — confira o nome da aba. Detalhe: ' + msg);
    throw new Error(msg);
  }
  const vals = (r.data && r.data.values) || [];
  if (!vals.length) return [];
  const cab = vals[0].map(h => String(h || '').trim());
  const linhas = [];
  for (let i = 1; i < vals.length; i++) {
    const row = vals[i]; if (!row || !row.length) continue;
    const o = {};
    cab.forEach((h, k) => { if (h) o[h] = row[k] == null ? '' : String(row[k]); });
    // aceita variações do cabeçalho (com/sem espaço, maiúsculas)
    const pick = (...ns) => { for (const n of ns) { const k = Object.keys(o).find(x => x.replace(/\s+/g, '').toLowerCase() === n.replace(/\s+/g, '').toLowerCase()); if (k) return o[k]; } return ''; };
    o.name = pick('Lead Titulo', 'Lead Título', 'Nome', 'name', 'title');
    o.phone = pick('Celular', 'Telefone', 'phone', 'WhatsApp');
    o.id = pick('ID', 'id', 'Etapa ID', 'stage_external_id');
    if (!o.phone && !o.name) continue;
    linhas.push(o);
  }
  return linhas;
}
async function _sheetsCfg(owner) { try { return JSON.parse(_cfg('sheets_sync', owner) || '{}') || {}; } catch (_) { return {}; } }
async function _sheetsSalvaCfg(owner, cfg) {
  const k = 'sheets_sync::' + (owner || ' ');
  const value = JSON.stringify(cfg);
  await supabase.from('settings').upsert({ key: k, value, updated_at: new Date().toISOString() });
  _settings[k] = value;
}
const _sheetsRodando = new Set();
const _sheetsProg = {}; // owner -> { fase, total, feitos, importados, pulados, erros, done, resultado }
async function _sheetsSincronizar(owner, motivo) {
  if (_sheetsRodando.has(owner)) return { ok: false, erro: 'já está sincronizando' };
  _sheetsRodando.add(owner);
  const cfg = await _sheetsCfg(owner);
  const ini = Date.now();
  _sheetsProg[owner] = { fase: 'lendo', total: 0, feitos: 0, importados: 0, pulados: 0, erros: 0, done: false, ini };
  try {
    if (!cfg.spreadsheet_id) throw new Error('Nenhuma planilha configurada');
    const linhas = await _lerPlanilha(cfg.spreadsheet_id, cfg.sheet_name || 'DISPARO');
    Object.assign(_sheetsProg[owner], { fase: 'importando', total: linhas.length });
    const r = await _importarLeads(linhas, owner, { soNovos: !cfg.atualizar_etapa,
      onProgress: p => Object.assign(_sheetsProg[owner], p) });
    cfg.last = { quando: new Date().toISOString(), motivo: motivo || 'manual', linhas: linhas.length, importados: r.imported, pulados: r.pulados, erros: r.errors.length, ms: Date.now() - ini };
    await _sheetsSalvaCfg(owner, cfg);
    console.log(`📊 Planilha (${owner}, ${motivo}): ${linhas.length} linha(s), ${r.imported} importado(s), ${r.pulados} já existiam`);
    const resultado = { ok: true, ...cfg.last, detalhes_erros: r.errors.slice(0, 10) };
    Object.assign(_sheetsProg[owner], { fase: 'concluido', feitos: linhas.length, importados: r.imported, pulados: r.pulados, erros: r.errors.length, done: true, resultado });
    return resultado;
  } catch (e) {
    cfg.last = { quando: new Date().toISOString(), motivo: motivo || 'manual', erro: e.message };
    try { await _sheetsSalvaCfg(owner, cfg); } catch (_) {}
    console.error('📊 Planilha falhou:', owner, e.message);
    const resultado = { ok: false, erro: e.message };
    Object.assign(_sheetsProg[owner], { fase: 'erro', done: true, resultado });
    return resultado;
  } finally { _sheetsRodando.delete(owner); }
}
app.get('/sheets/status', async (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const sa = _googleSA();
  const cfg = await _sheetsCfg(req.owner);
  res.json({ robo_configurado: !!sa, robo_email: sa ? sa.client_email : null,
    spreadsheet_id: cfg.spreadsheet_id || '', sheet_name: cfg.sheet_name || 'DISPARO', auto: !!cfg.auto, atualizar_etapa: !!cfg.atualizar_etapa,
    drip_rule_id: cfg.drip_rule_id || '', regras_drip: _dripRegras(req.owner).map(r => ({ id: r.id, nome: r.nome, ativo: !!r.ativo })), last: cfg.last || null });
});
app.post('/sheets/config', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const b = req.body || {};
  const cfg = await _sheetsCfg(req.owner);
  if (b.spreadsheet !== undefined) cfg.spreadsheet_id = _sheetsIdDeUrl(b.spreadsheet);
  if (b.sheet_name !== undefined) cfg.sheet_name = String(b.sheet_name || 'DISPARO').trim() || 'DISPARO';
  if (b.auto !== undefined) cfg.auto = !!b.auto;
  if (b.atualizar_etapa !== undefined) cfg.atualizar_etapa = !!b.atualizar_etapa;
  if (b.drip_rule_id !== undefined) cfg.drip_rule_id = String(b.drip_rule_id || '');
  await _sheetsSalvaCfg(req.owner, cfg);
  res.json({ ok: true, cfg });
});
// 👀 PRÉVIA: mostra o que a planilha vai gerar ANTES de importar (nada é gravado)
app.get('/sheets/preview', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  try {
    const cfg = await _sheetsCfg(req.owner);
    if (!cfg.spreadsheet_id) return res.status(400).json({ error: 'Nenhuma planilha configurada — cole o link e salve.' });
    const linhas = await _lerPlanilha(cfg.spreadsheet_id, cfg.sheet_name || 'DISPARO');
    const { data: sts } = await supabase.from('pipeline_stages').select('id, name, external_id').eq('owner', req.owner);
    const stPorExt = {}; for (const st of (sts || [])) if (st.external_id) stPorExt[String(st.external_id)] = st;
    const fones = linhas.map(l => String(l.phone || '').replace(/\D/g, '')).filter(p => p.length >= 8);
    const existentes = new Map();
    for (let k = 0; k < fones.length; k += 500) {
      const { data } = await supabase.from('contacts').select('phone, name, stage_id').eq('owner', req.owner).in('phone', fones.slice(k, k + 500));
      for (const c of (data || [])) existentes.set(c.phone, c);
    }
    const stNome = {}; for (const st of (sts || [])) stNome[st.id] = st.name;
    let novos = 0, jaExistem = 0, invalidos = 0, semEtapa = 0;
    const vistos = new Set();
    const itens = linhas.map((l, i) => {
      const phone = String(l.phone || '').replace(/\D/g, '');
      const extId = String(l.id || '').replace(/^=+\s*/, '').trim();
      const st = stPorExt[extId];
      let situacao;
      if (phone.length < 8) { situacao = 'invalido'; invalidos++; }
      else if (vistos.has(phone)) { situacao = 'repetido'; }
      else if (existentes.has(phone)) { situacao = 'existe'; jaExistem++; }
      else { situacao = 'novo'; novos++; }
      vistos.add(phone);
      if (!st && phone.length >= 8) semEtapa++;
      const ex = existentes.get(phone);
      return { linha: i + 2, name: l.name || '', phone, id: extId, etapa: st ? st.name : (extId ? '⚠️ ID não encontrado' : '(sem etapa)'),
        situacao, etapa_atual: ex ? (stNome[ex.stage_id] || '') : '' };
    });
    res.json({ total: linhas.length, novos, ja_existem: jaExistem, invalidos, sem_etapa: semEtapa,
      atualizar_etapa: !!cfg.atualizar_etapa, itens: itens.slice(0, 500), truncado: itens.length > 500 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/sheets/sync', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  if (String(req.query.bg || '') === '1') {
    // Em segundo plano: responde já e o app acompanha o progresso em /sheets/sync/status
    if (_sheetsRodando.has(req.owner)) return res.json({ ok: true, iniciado: false, ja_rodando: true });
    _sheetsSincronizar(req.owner, 'manual').catch(() => {});
    return res.json({ ok: true, iniciado: true });
  }
  const r = await _sheetsSincronizar(req.owner, 'manual');
  res.status(r.ok ? 200 : 500).json(r);
});
app.get('/sheets/sync/status', (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  res.json(_sheetsProg[req.owner] || { fase: 'parado', done: true });
});
// ⏰ Sincronização automática: a cada hora, para as contas que ligaram "auto"
setInterval(async () => {
  try {
    if (!supabase || !_googleSA()) return;
    for (const k in _settings) {
      if (!k.startsWith('sheets_sync::')) continue;
      const owner = k.slice('sheets_sync::'.length).trim();
      if (!owner) continue;
      let cfg = {}; try { cfg = JSON.parse(_settings[k] || '{}'); } catch (_) {}
      if (cfg && cfg.auto && cfg.spreadsheet_id) await _sheetsSincronizar(owner, 'automática');
    }
  } catch (e) { console.error('📊 auto-sync:', e.message); }
}, 60 * 60 * 1000);

// ── Alterar a ETAPA de um lead já existente (via n8n) ──
// Aceita 1 lead OU array. Identifica o lead pelo telefone e a etapa por:
//   stage | etapa | "Etapa" (nome, ex.: "S3")  |  id | "ID" (external_id)  |  stage_id (UUID)
app.post("/update/lead", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const items = Array.isArray(req.body) ? req.body
              : (Array.isArray(req.body.leads) ? req.body.leads : [req.body]);
  const clean = v => String(v || "").replace(/^=+\s*/, "").trim();
  const n8nOwner = (String(req.query.owner||'').trim()) || (!Array.isArray(req.body) && req.body.owner) || 'elianecezaroliveira@gmail.com';
  if (!_n8nAuthOk(req, n8nOwner)) return res.status(401).json(_n8nAuthErro);
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
  if (!_n8nAuthOk(req, n8nOwner)) return res.status(401).json(_n8nAuthErro);

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

// ── 📷 FOTOS DOS BOTS ──
// Guarda a imagem no cofre e devolve um link PÚBLICO (a Meta e o WhatsApp
// precisam conseguir baixar a foto sozinhos na hora do disparo).
app.post('/bot-media', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  try {
    const { data, mime, filename } = req.body || {};
    if (!data) return res.status(400).json({ error: 'Arquivo não recebido' });
    const buf = Buffer.from(String(data).replace(/^data:[^,]+,/, ''), 'base64');
    if (buf.length > 12 * 1024 * 1024) return res.status(400).json({ error: 'Imagem muito grande (máx. 12 MB)' });
    let tipo = String(mime || 'image/jpeg').split(';')[0].toLowerCase();
    // Só foto ou vídeo: qualquer outro tipo vira imagem (o link é público — nada
    // de hospedar página/arquivo estranho no endereço do CRM)
    if (!/^(image\/(jpeg|jpg|png|webp|gif)|video\/(mp4|3gpp|quicktime))$/.test(tipo)) tipo = 'image/jpeg';
    const ext = (tipo.split('/')[1] || 'jpg').replace(/[^a-z0-9]/gi, '') || 'jpg';
    const nome = `bot/${Date.now()}_${String(filename || 'foto').replace(/[^\w.-]/g, '').slice(-40) || 'foto'}.${ext}`;
    const { error } = await supabase.storage.from('wa-media').upload(nome, buf, { contentType: tipo, upsert: true });
    if (error) return res.status(500).json({ error: error.message });
    const base = process.env.BACKEND_URL || `https://${req.headers.host}`;
    res.json({ success: true, url: `${base}/bot-media/${encodeURIComponent(nome.slice(4))}` });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Entrega PÚBLICA da foto do bot (sem login — quem baixa é o próprio WhatsApp)
app.get('/bot-media/:arquivo', async (req, res) => {
  if (!supabase) return res.status(500).send('Storage indisponível');
  try {
    const caminho = 'bot/' + decodeURIComponent(req.params.arquivo).replace(/^bot\//, '').replace(/\.\./g, '').replace(/^\/+/, '');
    const { data: blob, error } = await supabase.storage.from('wa-media').download(caminho);
    if (error || !blob) return res.status(404).send('Arquivo não encontrado');
    const buf = Buffer.from(await blob.arrayBuffer());
    res.setHeader('Content-Type', blob.type || 'image/jpeg');
    res.setHeader('Content-Length', buf.length);
    res.setHeader('Cache-Control', 'public, max-age=604800, immutable');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.status(200).end(buf);
  } catch (e) { res.status(500).send('Falha ao carregar'); }
});

// ── 📝 NOTA INTERNA: fica no chat só para o dono (NUNCA vai para o lead) ──
app.post("/notes", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phone, content, account_id } = req.body || {};
  const txt = String(content || '').trim();
  if (!phone || !txt) return res.status(400).json({ error: "Telefone e texto são obrigatórios" });
  const { data, error } = await supabase.from("messages").insert({
    phone: String(phone), content: txt, type: 'note', direction: 'outbound',
    timestamp: new Date().toISOString(), account_id: account_id || null,
    owner: req.owner || null, status: null,
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, data });
});

// ── Mensagens de um contato ──
// Busca pelas DUAS variantes do número (com e sem o nono dígito): mensagens
// enviadas por canais diferentes (QR × API) podem ter sido gravadas na outra
// variante — o chat mostra TUDO num lugar só, como deve ser
app.get("/messages/:phone", async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase
    .from("messages").select("*").in("phone", phoneVariants(req.params.phone)).eq("owner", req.owner || ' ')
    .order("timestamp", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
  // CURA RETROATIVA (só a partir de RECIBOS REAIS): "leu uma = leu as anteriores".
  // Antes ela partia do status da PRÉVIA do contato — se a prévia estivesse com um
  // "lida" herdado, pintava de azul mensagens que ninguém leu (caso do bot).
  // Agora a referência é a última mensagem enviada que TEM recibo de leitura/entrega
  // e, no fim, a prévia é corrigida para o status real da última mensagem enviada.
  (async () => {
    try {
      const OW = req.owner || ' ', ph = req.params.phone;
      const base = () => supabase.from('messages').select('status, timestamp').eq('phone', ph).eq('owner', OW).eq('direction', 'outbound');
      // REPARO do estrago antigo: mensagem marcada "lida" SEM horário de leitura e
      // mais nova que a última leitura REAL (com horário) só pode ter vindo da
      // herança errada → volta para "entregue". (Se a conversa não tem nenhuma
      // leitura com horário, não mexe — pode ser histórico anterior à coluna read_at.)
      try {
        const { data: ultReal, error: eR } = await supabase.from('messages').select('timestamp').eq('phone', ph).eq('owner', OW)
          .eq('direction', 'outbound').not('read_at', 'is', null).order('timestamp', { ascending: false }).limit(1).maybeSingle();
        if (!eR && ultReal) await supabase.from('messages').update({ status: 'delivered' }).eq('phone', ph).eq('owner', OW)
          .eq('direction', 'outbound').eq('status', 'read').is('read_at', null).gt('timestamp', ultReal.timestamp);
      } catch (_) {}
      const { data: ultR } = await base().eq('status', 'read').order('timestamp', { ascending: false }).limit(1).maybeSingle();
      if (ultR) await supabase.from('messages').update({ status: 'read' }).eq('phone', ph).eq('owner', OW).eq('direction', 'outbound')
        .lt('timestamp', ultR.timestamp).or('status.is.null,status.in.(pending,sent,delivered)');
      const { data: ultD } = await base().eq('status', 'delivered').order('timestamp', { ascending: false }).limit(1).maybeSingle();
      if (ultD) await supabase.from('messages').update({ status: 'delivered' }).eq('phone', ph).eq('owner', OW).eq('direction', 'outbound')
        .lt('timestamp', ultD.timestamp).or('status.is.null,status.in.(pending,sent)');
      // Prévia = status REAL da última mensagem enviada (nunca "lida" por herança)
      const { data: c } = await supabase.from('contacts').select('last_message_status, last_message_at, last_message_direction').eq('phone', ph).eq('owner', OW).maybeSingle();
      if (c && c.last_message_direction === 'outbound') {
        const { data: ult } = await base().order('timestamp', { ascending: false }).limit(1).maybeSingle();
        if (ult) {
          const real = (!ult.status || ult.status === 'pending') ? null : ult.status;
          if ((c.last_message_status || null) !== real) await supabase.from('contacts').update({ last_message_status: real }).eq('phone', ph).eq('owner', OW);
        }
      }
    } catch (_) {}
  })();
});

// ⭐/📌 Favoritar e fixar MENSAGEM (como no WhatsApp)
app.put('/messages/:id/star', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { error } = await supabase.from('messages').update({ starred: !!req.body.starred })
    .eq('id', req.params.id).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});
app.put('/messages/:id/pin', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { error } = await supabase.from('messages').update({ pinned: !!req.body.pinned })
    .eq('id', req.params.id).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ── Listar templates ──
app.get("/templates", async (req, res) => {
  const { account_id } = req.query;
  if (!supabase || !account_id) return res.status(400).json({ error: "account_id obrigatório" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("token, waba_id").eq("id", account_id).eq("owner", req.owner || ' ').single();
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
    .from("accounts").select("token, waba_id").eq("id", account_id).eq("owner", req.owner || ' ').single();
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
    .from("accounts").select("token, waba_id").eq("id", account_id).eq("owner", req.owner || ' ').single();
  if (accErr || !account) return res.status(404).json({ error: "Conta não encontrada" });
  if (!account.waba_id) return res.status(400).json({ error: "WABA ID não encontrado para esta conta" });
  const name = _decSeguro(req.params.template_id);
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
  if (await _isSelfSend(to, account_id)) return res.status(400).json({ error: '🚫 Bloqueado: o destino é o PRÓPRIO número desta conta.' });
  stopBotRunsForPhone(to, req.owner); // você assumiu a conversa — bot deste lead para
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { data: account, error: accErr } = await supabase
    .from("accounts").select("phone_number_id, token").eq("id", account_id).eq("owner", req.owner || ' ').single();
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
        last_message_preview: preview, last_message_direction: 'outbound', last_message_status: null, owner: req.owner || null },
      { onConflict: "owner,phone" }
    );
    const tplWamid = response.data?.messages?.[0]?.id || null;
    await supabase.from("messages").insert({
      phone: to, content: shownText, type: "template",
      direction: "outbound", timestamp: new Date().toISOString(), account_id: safeAccountId,
      status: tplWamid ? 'sent' : 'pending', wamid: tplWamid, owner: req.owner || null,
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

// Criar estágio — TODA etapa nasce com um ID EXTERNO próprio (pronto para o n8n)
app.post("/pipeline/stages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  if (!name) return res.status(400).json({ error: "Nome obrigatório" });
  const external_id = String(req.body.external_id || '').trim() || String(Math.floor(10000000 + Math.random() * 90000000));
  const { data, error } = await supabase
    .from("pipeline_stages").insert({ name, position: position || 0, owner: req.owner || null, external_id }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Etapas ANTIGAS sem ID externo ganham um automaticamente (uma vez, ao subir).
// IMPORTANTE: etapas que JÁ TÊM ID (as usadas no n8n da Eliane) NÃO são tocadas.
setTimeout(async () => {
  try {
    if (!supabase) return;
    // Ajuste pontual (roda UMA única vez): SIAPE3 do vendetta = 104721840 (ID da planilha dele)
    try {
      const K = 'fix_siape3_vendetta';
      const { data: feito } = await supabase.from('settings').select('value').eq('key', K).maybeSingle();
      if (!feito) {
        await supabase.from('pipeline_stages').update({ external_id: '104721840' })
          .eq('owner', 'vendetta.freedon@gmail.com')
          .or('name.ilike.SIAPE3,name.ilike.SIAPE 3');
        await supabase.from('settings').upsert({ key: K, value: 'ok', updated_at: new Date().toISOString() });
        console.log('🆔 SIAPE3 (vendetta) → 104721840');
      }
    } catch (_) {}
    const { data: st } = await supabase.from('pipeline_stages').select('id').is('external_id', null);
    for (const s of (st || [])) {
      await supabase.from('pipeline_stages')
        .update({ external_id: String(Math.floor(10000000 + Math.random() * 90000000)) })
        .eq('id', s.id).is('external_id', null);
    }
    if (st && st.length) console.log(`🆔 ${st.length} etapa(s) ganharam ID externo automático`);
  } catch (_) {}
}, 20000);

// Renomear / reordenar / trocar ID externo do estágio
app.put("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { name, position } = req.body;
  const updates = {};
  if (name !== undefined) updates.name = name;
  if (position !== undefined) updates.position = position;
  if (req.body.external_id !== undefined) updates.external_id = String(req.body.external_id).trim() || null;
  const { error } = await supabase
    .from("pipeline_stages").update(updates).eq("id", req.params.id).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Excluir estágio (move leads para sem-status)
// 🗑️ Excluir coluna → vai para a LIXEIRA (settings stage_trash::owner) com os leads
// que estavam nela, para poder DESFAZER (mesmo id → bots, gotejamento e IDs externos
// continuam funcionando ao restaurar).
async function _stageTrash(owner) { try { const a = JSON.parse(_cfg('stage_trash', owner) || '[]'); return Array.isArray(a) ? a : []; } catch (_) { return []; } }
async function _stageTrashSalva(owner, lista) {
  const k = 'stage_trash::' + (owner || ' ');
  const value = JSON.stringify(lista.slice(0, 15));
  await supabase.from('settings').upsert({ key: k, value, updated_at: new Date().toISOString() });
  _settings[k] = value;
}
app.delete("/pipeline/stages/:id", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const OW = req.owner || ' ';
  const { data: st } = await supabase.from('pipeline_stages').select('*').eq('id', req.params.id).eq('owner', OW).maybeSingle();
  const { data: leads } = await supabase.from('contacts').select('phone').eq('stage_id', req.params.id).eq('owner', OW);
  await supabase.from("contacts").update({ stage_id: null }).eq("stage_id", req.params.id).eq("owner", OW);
  const { error } = await supabase.from("pipeline_stages").delete().eq("id", req.params.id).eq("owner", OW);
  if (error) return res.status(500).json({ error: error.message });
  try {
    if (st) {
      const lixo = await _stageTrash(req.owner);
      lixo.unshift({ stage: st, phones: (leads || []).map(l => l.phone), deleted_at: new Date().toISOString() });
      await _stageTrashSalva(req.owner, lixo);
    }
  } catch (e) { console.error('lixeira de coluna:', e.message); }
  // ⏳ Gotejamentos que usavam esta etapa são DESLIGADOS (senão continuariam
  // mandando leads para uma etapa que não existe mais)
  try {
    const regras = _dripRegras(req.owner);
    let mexeu = false;
    for (const r of regras) {
      if (String(r.de) !== String(req.params.id) && String(r.para) !== String(req.params.id)) continue;
      if (r.manual || r.agendado) { r.manual = false; r.agendado = false; r.ativo = false; mexeu = true; }
    }
    if (mexeu) { await _dripSalva(req.owner, regras); addNotice(req.owner, `⏳ Gotejamento(s) que usavam a etapa "${st?.name || ''}" foram desligados.`, 'drip-del:' + req.params.id); }
    delete _dripStagesCache[req.owner];
  } catch (e) { console.error('drip pós-exclusão de etapa:', e.message); }
  res.json({ success: true, leads_movidos: (leads || []).length });
});
// Lixeira de colunas
app.get('/pipeline/stages/trash', async (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const lixo = await _stageTrash(req.owner);
  res.json(lixo.map(x => ({ id: x.stage.id, name: x.stage.name, external_id: x.stage.external_id, leads: (x.phones || []).length, deleted_at: x.deleted_at })));
});
// Restaurar coluna da lixeira (mesmo id) e devolver os leads
app.post('/pipeline/stages/restore', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const id = String(req.body && req.body.id || '');
  const lixo = await _stageTrash(req.owner);
  const item = lixo.find(x => x.stage && x.stage.id === id);
  if (!item) return res.status(404).json({ error: 'Coluna não está na lixeira' });
  const st = Object.assign({}, item.stage); delete st.created_at;
  const { error } = await supabase.from('pipeline_stages').insert(st);
  if (error && !/duplicate|unique/i.test(error.message)) return res.status(500).json({ error: error.message });
  let devolvidos = 0;
  const fones = item.phones || [];
  for (let k = 0; k < fones.length; k += 400) {
    const { data } = await supabase.from('contacts').update({ stage_id: id }).eq('owner', req.owner).in('phone', fones.slice(k, k + 400)).is('stage_id', null).select('phone');
    devolvidos += (data || []).length;
  }
  await _stageTrashSalva(req.owner, lixo.filter(x => x !== item));
  try { delete _dripStagesCache[req.owner]; } catch (_) {} // a etapa voltou: o gotejamento a enxerga na hora
  console.log(`♻️ Coluna restaurada: ${st.name} (${devolvidos} leads de volta)`);
  res.json({ ok: true, stage: st, leads_devolvidos: devolvidos });
});
// 🩹 Recuperar coluna apagada ANTES da lixeira existir: acha ids de etapa que bots,
// gotejamento ou regras ainda referenciam mas que não existem mais, e recria com o
// MESMO id (assim tudo volta a apontar certo). Os leads precisam ser movidos à mão.
app.get('/pipeline/stages/orphans', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const OW = req.owner;
  const { data: sts } = await supabase.from('pipeline_stages').select('id').eq('owner', OW);
  const existe = new Set((sts || []).map(x => x.id));
  const refs = {}; // id -> pistas
  const add = (id, pista) => { if (!id || existe.has(id)) return; (refs[id] = refs[id] || new Set()).add(pista); };
  const { data: bots } = await supabase.from('bots').select('id, name, trigger_stage_id').eq('owner', OW);
  for (const b of (bots || [])) add(b.trigger_stage_id, 'bot "' + b.name + '"');
  for (const r of _dripRegras(OW)) { add(r.de, 'gotejamento "' + (r.nome || '') + '" (origem)'); add(r.para, 'gotejamento "' + (r.nome || '') + '" (destino)'); }
  try { const sa = JSON.parse(_settings['stage_actions::' + OW] || '{}'); for (const k in sa) add(k, 'automação da etapa'); for (const k in sa) for (const a of (sa[k] || [])) if (a && a.type === 'move_stage') add(a.stage_id, 'automação "mover para"'); } catch (_) {}
  try { const { data: nodes } = await supabase.from('bot_nodes').select('config, type').eq('type', 'move_stage'); for (const n of (nodes || [])) { const c = typeof n.config === 'string' ? JSON.parse(n.config) : (n.config || {}); add(c.stage_id, 'passo "Mudar status" de um bot'); } } catch (_) {}
  res.json(Object.keys(refs).map(id => ({ id, pistas: [...refs[id]] })));
});
app.post('/pipeline/stages/recover', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const { id, name, external_id } = req.body || {};
  if (!id || !name) return res.status(400).json({ error: 'id e name obrigatórios' });
  const { data: max } = await supabase.from('pipeline_stages').select('position').eq('owner', req.owner).order('position', { ascending: false }).limit(1).maybeSingle();
  const row = { id: String(id), name: String(name).trim(), owner: req.owner, position: ((max && max.position) || 0) + 1, external_id: String(external_id || '').trim() || String(Math.floor(10000000 + Math.random() * 90000000)) };
  const { data, error } = await supabase.from('pipeline_stages').insert(row).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true, stage: data });
});

// Listar contatos (com stage_id, unread_count e prévia)
app.get("/contacts", async (req, res) => {
  if (!supabase) return res.json([]);
  const { account_id, with_messages } = req.query;
  // Tenta incluir created_at; se a coluna ainda não existir no banco, cai para a
  // seleção sem ela (não quebra o carregamento dos leads/conversas)
  _subscribeRecentPresence(req.owner); // presença dos recentes (não bloqueia a resposta)
  const COLS_BASE = "phone, name, account_id, stage_id, tags, unread_count, first_unread_at, last_message_at, last_message_preview, last_message_direction, favorite, avatar";
  const build = (cols) => {
    let q = supabase.from("contacts").select(cols).eq("owner", req.owner || ' ').order("last_message_at", { ascending: false });
    if (account_id) q = q.eq("account_id", account_id);
    if (with_messages) q = q.not("last_message_preview", "is", null);
    return q;
  };
  let { data, error } = await build(COLS_BASE + ", created_at, last_message_status, pinned, muted");
  if (error) { ({ data, error } = await build(COLS_BASE + ", created_at, last_message_status, pinned")); } // fallback sem muted
  if (error) { ({ data, error } = await build(COLS_BASE + ", created_at, last_message_status")); } // fallback sem pinned
  if (error) { ({ data, error } = await build(COLS_BASE + ", created_at")); } // fallback sem last_message_status
  if (error) { ({ data, error } = await build(COLS_BASE)); } // fallback sem created_at
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
    const { data } = await supabase.from('accounts').select('type, evolution_instance').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
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
    const { error: eEd } = await supabase.from('messages').update({ content: text, edited: true }).eq('wamid', wamid).eq('phone', to);
    if (eEd) await supabase.from('messages').update({ content: text }).eq('wamid', wamid).eq('phone', to);
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
      const { data } = await supabase.from('accounts').select('evolution_instance, phone_number_id, token').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
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
        // A API oficial só aceita mostrar "digitando…" junto com a leitura de uma
        // mensagem RECEBIDA. Depois que você responde, a Meta costuma recusar até
        // o lead escrever de novo — por isso guardamos o motivo no log.
        let recusa = null;
        await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`, {
          messaging_product: 'whatsapp', status: 'read', message_id: lastIn.wamid,
          typing_indicator: { type: 'text' }
        }, { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } })
          .catch(e => { recusa = e.response?.data?.error?.message || e.message; console.log('⌨️ Meta recusou o "digitando…":', recusa); });
        return res.json({ success: !recusa, via: 'cloud', motivo: recusa || undefined });
      }
    }
    res.json({ success: false });
  } catch (_) { res.json({ success: false }); }
});

// "digitando…" para o lead (usado pelo Cronômetro do bot antes de uma mensagem).
// Devolve 'qr' | 'cloud' | null — o ritmo de renovação é diferente em cada motor:
// QR renova à vontade (8s); API oficial mostra ~25s por pedido e repetir cedo
// demais com a mesma referência CANCELA o indicador (por isso 22s lá).
async function botTypingPulse(phone, accountId) {
  try {
    if (!accountId || !supabase) return null;
    const { data: acct } = await supabase.from('accounts').select('evolution_instance, phone_number_id, token').eq('id', accountId).maybeSingle();
    if (!acct) return null;
    if (acct.evolution_instance && _waSocks[acct.evolution_instance] && _waState[acct.evolution_instance] === 'open') {
      const sock = _waSocks[acct.evolution_instance];
      const jid = await waResolveJid(sock, phone);
      await sock.sendPresenceUpdate('composing', jid);
      return 'qr';
    }
    if (acct.phone_number_id && acct.token) {
      const { data: lastIn } = await supabase.from('messages').select('wamid')
        .eq('phone', phone).eq('direction', 'inbound').eq('account_id', accountId)
        .not('wamid', 'is', null).order('timestamp', { ascending: false }).limit(1).maybeSingle();
      if (lastIn?.wamid) {
        await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`, {
          messaging_product: 'whatsapp', status: 'read', message_id: lastIn.wamid,
          typing_indicator: { type: 'text' }
        }, { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } }).catch(() => {});
        return 'cloud';
      }
    }
  } catch (_) {}
  return null;
}

// Grava a mensagem enviada + atualiza a prévia da conversa (recursos especiais)
async function saveOutboundSpecial(req, to, account_id, type, content, wamid) {
  if (!supabase) return;
  const preview = content.length > 80 ? content.substring(0, 80) + '…' : content;
  await supabase.from('contacts').upsert({ phone: to, last_message_at: new Date().toISOString(), account_id: account_id || null, last_message_preview: preview, last_message_direction: 'outbound', last_message_status: null, owner: req.owner || null }, { onConflict: 'owner,phone' });
  await supabase.from('messages').insert({ phone: to, content, type, direction: 'outbound', timestamp: new Date().toISOString(), account_id: account_id || null, status: wamid ? 'sent' : 'pending', wamid: wamid || null, owner: req.owner || null });
}

// 📍 Enviar localização (QR e API oficial)
app.post('/send-location', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  try {
    let { to, account_id, lat, lng, name } = req.body || {};
    lat = parseFloat(lat); lng = parseFloat(lng);
    if (!to || isNaN(lat) || isNaN(lng)) return res.status(400).json({ error: 'Informe to, lat e lng' });
    to = await resolveExistingPhone(to, req.owner);
    stopBotRunsForPhone(to, req.owner);
    const { data: acct } = await supabase.from('accounts').select('*').eq('id', account_id || '').eq('owner', req.owner || ' ').maybeSingle();
    if (!acct) return res.status(400).json({ error: 'Conta não encontrada' });
    const content = `📍 ${name || 'Localização'}\nhttps://maps.google.com/?q=${lat},${lng}`;
    let wamid = null;
    if (acct.evolution_instance) {
      const r = await waSendRaw(acct.evolution_instance, to, { location: { degreesLatitude: lat, degreesLongitude: lng, name: name || undefined } });
      wamid = r?.key?.id || null;
    } else if (acct.phone_number_id && acct.token) {
      const r = await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
        { messaging_product: 'whatsapp', to, type: 'location', location: { latitude: lat, longitude: lng, name: name || undefined } },
        { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } });
      wamid = r.data?.messages?.[0]?.id || null;
    } else return res.status(400).json({ error: 'Conta sem credenciais' });
    await saveOutboundSpecial(req, to, account_id, 'location', content, wamid);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.response?.data?.error?.message || e.message }); }
});

// 👤 Enviar cartão de contato (QR e API oficial)
app.post('/send-contact', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  try {
    let { to, account_id, cname, cphone } = req.body || {};
    if (!to || !cname || !cphone) return res.status(400).json({ error: 'Informe to, cname e cphone' });
    to = await resolveExistingPhone(to, req.owner);
    stopBotRunsForPhone(to, req.owner);
    const digits = String(cphone).replace(/\D/g, '');
    const { data: acct } = await supabase.from('accounts').select('*').eq('id', account_id || '').eq('owner', req.owner || ' ').maybeSingle();
    if (!acct) return res.status(400).json({ error: 'Conta não encontrada' });
    const content = `👤 ${cname}\n+${digits}`;
    let wamid = null;
    if (acct.evolution_instance) {
      const vcard = `BEGIN:VCARD\nVERSION:3.0\nFN:${cname}\nTEL;type=CELL;waid=${digits}:+${digits}\nEND:VCARD`;
      const r = await waSendRaw(acct.evolution_instance, to, { contacts: { displayName: cname, contacts: [{ displayName: cname, vcard }] } });
      wamid = r?.key?.id || null;
    } else if (acct.phone_number_id && acct.token) {
      const r = await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
        { messaging_product: 'whatsapp', to, type: 'contacts', contacts: [{ name: { formatted_name: cname, first_name: cname }, phones: [{ phone: '+' + digits, wa_id: digits, type: 'CELL' }] }] },
        { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } });
      wamid = r.data?.messages?.[0]?.id || null;
    } else return res.status(400).json({ error: 'Conta sem credenciais' });
    await saveOutboundSpecial(req, to, account_id, 'contact', content, wamid);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.response?.data?.error?.message || e.message }); }
});

// 📊 Enviar enquete (SÓ números QR — a API oficial não tem enquete)
app.post('/send-poll', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  try {
    let { to, account_id, question, options } = req.body || {};
    if (!to || !question || !Array.isArray(options) || options.length < 2) return res.status(400).json({ error: 'Informe to, question e pelo menos 2 options' });
    to = await resolveExistingPhone(to, req.owner);
    stopBotRunsForPhone(to, req.owner);
    const { data: acct } = await supabase.from('accounts').select('*').eq('id', account_id || '').eq('owner', req.owner || ' ').maybeSingle();
    if (!acct?.evolution_instance) return res.status(400).json({ error: 'Enquetes só funcionam em números QR Code' });
    const r = await waSendRaw(acct.evolution_instance, to, { poll: { name: question, values: options.slice(0, 12), selectableCount: 1 } });
    try {
      const sockP = _waSocks[acct.evolution_instance];
      if (r?.key?.id) _waPolls[r.key.id] = {
        name: question,
        options: options.slice(0, 12),
        encKey: r?.message?.messageContextInfo?.messageSecret || null,
        creatorJid: (_baileys.jidNormalizedUser && sockP?.user?.id) ? _baileys.jidNormalizedUser(sockP.user.id) : (sockP?.user?.id || null)
      };
    } catch (_) {}
    const content = `📊 ${question}\n` + options.slice(0, 12).map(o => '▫️ ' + o).join('\n');
    await saveOutboundSpecial(req, to, account_id, 'poll', content, r?.key?.id || null);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// 🚫 Apagar mensagem PARA TODOS (SÓ QR — a API oficial não permite)
app.post('/message-revoke', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  try {
    const { phone, account_id, wamid } = req.body || {};
    if (!phone || !wamid) return res.status(400).json({ error: 'Informe phone e wamid' });
    const { data: acct } = await supabase.from('accounts').select('evolution_instance').eq('id', account_id || '').eq('owner', req.owner || ' ').maybeSingle();
    const inst = acct?.evolution_instance;
    if (!inst || !_waSocks[inst] || _waState[inst] !== 'open') return res.status(400).json({ error: 'Apagar para todos só funciona em números QR conectados' });
    const sock = _waSocks[inst];
    const jid = await waResolveJid(sock, phone);
    await sock.sendMessage(jid, { delete: { remoteJid: jid, fromMe: true, id: wamid } });
    await supabase.from('messages').update({ content: '🚫 Mensagem apagada', type: 'text' }).eq('wamid', wamid).eq('phone', phone);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Inscreve a presença dos contatos recentes (para o "digitando…" na LISTA)
const _presSubTs = {};
async function _subscribeRecentPresence(owner) {
  try {
    if (!supabase) return;
    const { data: accs } = await supabase.from('accounts').select('id, evolution_instance').eq('owner', owner || ' ').not('evolution_instance', 'is', null);
    for (const a of (accs || [])) {
      const inst = a.evolution_instance;
      const sock = _waSocks[inst];
      if (!sock || _waState[inst] !== 'open') continue;
      const { data: cts } = await supabase.from('contacts').select('phone').eq('owner', owner || ' ').eq('account_id', a.id).order('last_message_at', { ascending: false }).limit(30);
      for (const c of (cts || [])) {
        const k = inst + '|' + c.phone;
        if (_presSubTs[k] && Date.now() - _presSubTs[k] < 300000) continue;
        _presSubTs[k] = Date.now();
        try { const jid = await waResolveJid(sock, c.phone); await sock.presenceSubscribe(jid); } catch (_) {}
      }
    }
  } catch (_) {}
}

// "digitando…"/"gravando áudio…" AGORA, para a lista de conversas (SÓ QR)
// Devolve o telefone COM e SEM o nono dígito (o WhatsApp ora usa um, ora outro)
function _brPhoneVariants(ph) {
  const out = new Set([ph]);
  if (/^55\d{10}$/.test(ph)) out.add(ph.slice(0, 4) + '9' + ph.slice(4));
  if (/^55\d{11}$/.test(ph) && ph[4] === '9') out.add(ph.slice(0, 4) + ph.slice(5));
  return Array.from(out);
}
// Instâncias QR de cada dono (cache de 60s) — o "digitando…" só mostra os SEUS
const _instDoDono = {};
async function _minhasInstancias(owner) {
  const c = _instDoDono[owner];
  if (c && Date.now() - c.ts < 60000) return c.set;
  const { data } = await supabase.from('accounts').select('evolution_instance').eq('owner', owner).not('evolution_instance', 'is', null);
  const set = new Set((data || []).map(x => String(x.evolution_instance)));
  _instDoDono[owner] = { set, ts: Date.now() };
  return set;
}
app.get('/typing-list', async (req, res) => {
  if (!req.owner || !supabase) return res.json({});
  let minhas = new Set();
  try { minhas = await _minhasInstancias(req.owner); } catch (_) { return res.json({}); }
  const out = {}; const now = Date.now();
  for (const [k, p] of Object.entries(_waPresence)) {
    if (!minhas.has(String(k.split('|')[0] || ''))) continue; // número de outra conta: não é da sua conta
    if ((p.state === 'composing' || p.state === 'recording') && now - p.at < 12000) {
      const ph = (k.split('|')[1] || '').split('@')[0].split(':')[0];
      if (ph) _brPhoneVariants(ph).forEach(v => { out[v] = p.state; });
    }
  }
  res.json(out);
});

// 👤 PERFIL do número de WhatsApp (foto, recado, descrição…) — QR e API oficial
app.get('/wa-profile', async (req, res) => {
  try {
    const { account_id } = req.query;
    if (!supabase || !account_id) return res.json({});
    const { data: acct } = await supabase.from('accounts').select('*').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
    if (!acct) return res.json({});
    if (acct.evolution_instance) return res.json({ type: 'qr' }); // QR não expõe leitura fácil
    if (acct.phone_number_id && acct.token) {
      const r = await axios.get(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/whatsapp_business_profile`, {
        params: { fields: 'about,address,description,email,profile_picture_url,websites' },
        headers: { Authorization: `Bearer ${acct.token}` }, timeout: 10000
      });
      const d = (r.data?.data || [])[0] || {};
      return res.json({ type: 'api', about: d.about || '', address: d.address || '', description: d.description || '', email: d.email || '', photo: d.profile_picture_url || null, website: (d.websites || [])[0] || '' });
    }
    res.json({});
  } catch (e) { res.json({ error: e.response?.data?.error?.message || e.message }); }
});

app.post('/wa-profile', async (req, res) => {
  try {
    const { account_id, name, about, description, email, address, website, photoBase64 } = req.body || {};
    if (!supabase || !account_id) return res.status(400).json({ error: 'account_id obrigatório' });
    const { data: acct } = await supabase.from('accounts').select('*').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
    if (!acct) return res.status(404).json({ error: 'Conta não encontrada' });

    // ── Número QR (Baileys) ──
    if (acct.evolution_instance) {
      const sock = _waSocks[acct.evolution_instance];
      if (!sock || _waState[acct.evolution_instance] !== 'open') return res.status(400).json({ error: 'WhatsApp QR desconectado' });
      const jid = sock.user?.id;
      if (name) await sock.updateProfileName(name).catch(e => { throw new Error('nome: ' + e.message); });
      if (about) await sock.updateProfileStatus(about).catch(e => { throw new Error('recado: ' + e.message); });
      if (photoBase64 && jid) await sock.updateProfilePicture(jid, Buffer.from(photoBase64, 'base64')).catch(e => { throw new Error('foto: ' + e.message); });
      return res.json({ success: true, via: 'qr' });
    }

    // ── API oficial (Meta) ──
    if (!acct.phone_number_id || !acct.token) return res.status(400).json({ error: 'Conta sem credenciais' });
    const hdr = { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' };
    const body = { messaging_product: 'whatsapp' };
    if (about !== undefined && about !== null) body.about = String(about);
    if (description !== undefined && description !== null) body.description = String(description);
    if (email !== undefined && email !== null) body.email = String(email);
    if (address !== undefined && address !== null) body.address = String(address);
    if (website) body.websites = [String(website)];

    // Foto: upload retomável no app da Meta → handle → aplica no perfil
    if (photoBase64) {
      if (!APP_ID) return res.status(400).json({ error: 'APP_ID não configurado no servidor (necessário para a foto)' });
      const buf = Buffer.from(photoBase64, 'base64');
      const up = await axios.post(`https://graph.facebook.com/v23.0/${APP_ID}/uploads`, null, {
        params: { file_length: buf.length, file_type: 'image/jpeg', access_token: acct.token }, timeout: 15000
      });
      const sessId = up.data?.id; // formato "upload:XXX"
      const fin = await axios.post(`https://graph.facebook.com/v23.0/${sessId}`, buf, {
        headers: { Authorization: `OAuth ${acct.token}`, file_offset: '0', 'Content-Type': 'application/octet-stream' }, timeout: 30000,
        maxContentLength: Infinity, maxBodyLength: Infinity
      });
      if (fin.data?.h) body.profile_picture_handle = fin.data.h;
    }

    await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/whatsapp_business_profile`, body, { headers: hdr, timeout: 15000 });
    res.json({ success: true, via: 'api' });
  } catch (e) {
    res.status(500).json({ error: e.response?.data?.error?.message || e.message });
  }
});

// 🔗 Prévia de link (título/descrição/imagem do site) — com cache em memória
const _linkPrevCache = new Map();
app.get('/link-preview', async (req, res) => {
  try {
    const url = String(req.query.url || '');
    if (!/^https?:\/\//i.test(url)) return res.json({});
    // 🔒 Nunca busca endereços internos (SSRF)
    try {
      const h = new URL(url).hostname.toLowerCase();
      if (/^(localhost|127\.|10\.|0\.|169\.254\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|\[?::1\]?$|\[?fc|\[?fd|\[?fe80)/.test(h) || !h.includes('.')) return res.json({});
    } catch (_) { return res.json({}); }
    const hit = _linkPrevCache.get(url);
    if (hit && Date.now() - hit.ts < 6 * 3600000) return res.json(hit.data);
    const r = await axios.get(url, { timeout: 6000, maxContentLength: 512 * 1024, maxRedirects: 0, validateStatus: c => c >= 200 && c < 300, // sem seguir redirecionamento: um link podia desviar para um endereço interno do servidor
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; VETRA-CRM/1.0)' }, responseType: 'text',
      validateStatus: st => st >= 200 && st < 400 });
    const html = String(r.data || '').slice(0, 300000);
    const pick = (re) => { const m = html.match(re); return m ? m[1].trim() : null; };
    const meta = (pr) => pick(new RegExp(`<meta[^>]+(?:property|name)=["']${pr}["'][^>]+content=["']([^"']+)["']`, 'i'))
                    || pick(new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']${pr}["']`, 'i'));
    let img = meta('og:image') || meta('twitter:image');
    if (img && img.startsWith('/')) { try { const u = new URL(url); img = u.origin + img; } catch (_) {} }
    const data = {
      title: meta('og:title') || pick(/<title[^>]*>([^<]+)<\/title>/i) || null,
      desc: (meta('og:description') || meta('description') || '').slice(0, 160) || null,
      image: img || null,
      site: (() => { try { return new URL(url).hostname.replace(/^www\./, ''); } catch (_) { return null; } })()
    };
    _linkPrevCache.set(url, { ts: Date.now(), data });
    if (_linkPrevCache.size > 500) _linkPrevCache.delete(_linkPrevCache.keys().next().value);
    res.json(data);
  } catch (_) { res.json({}); }
});

// 🟢 Online / visto por último do lead (SÓ QR)
app.get('/presence', async (req, res) => {
  try {
    const { phone, account_id } = req.query;
    if (!phone || !account_id || !supabase) return res.json({});
    const { data: acct } = await supabase.from('accounts').select('evolution_instance').eq('id', account_id).eq('owner', req.owner || ' ').maybeSingle();
    const inst = acct?.evolution_instance;
    if (!inst || !_waSocks[inst] || _waState[inst] !== 'open') return res.json({});
    const sock = _waSocks[inst];
    const jid = await waResolveJid(sock, phone);
    try { await sock.presenceSubscribe(jid); } catch (_) {}
    // Assina TAMBÉM as variantes com/sem o nono dígito (o WhatsApp alterna entre elas)
    const _limpo = String(phone).replace(/\D/g, '');
    for (const v of _brPhoneVariants(_limpo)) {
      try { if (v + '@s.whatsapp.net' !== jid) await sock.presenceSubscribe(v + '@s.whatsapp.net'); } catch (_) {}
    }
    let pr = _waPresence[inst + '|' + jid];
    if (!pr) {
      // A atualização pode chegar com o número na OUTRA variante → procura por telefone
      const vars = _brPhoneVariants(_limpo);
      for (const [k, p] of Object.entries(_waPresence)) {
        if (!k.startsWith(inst + '|')) continue;
        // tira o sufixo do aparelho (":0") antes de comparar o telefone
        const ph = (k.split('|')[1] || '').split('@')[0].split(':')[0];
        if (vars.includes(ph)) { pr = p; break; }
      }
    }
    if (!pr) return res.json({});
    res.json({ state: pr.state, lastSeen: pr.lastSeen, at: pr.at });
  } catch (_) { res.json({}); }
});

// 🔇 Silenciar/reativar notificações da conversa
app.put('/contacts/:phone/mute', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { error } = await supabase.from('contacts').update({ muted: !!req.body.muted })
    .eq('phone', _decSeguro(req.params.phone)).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// 🔵 Marcar conversa como NÃO lida (volta o badge)
app.put('/contacts/:phone/unread', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { error } = await supabase.from('contacts').update({ unread_count: 1, first_unread_at: new Date().toISOString() })
    .eq('phone', _decSeguro(req.params.phone)).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Conversa silenciada? (consulta protegida — funciona mesmo sem a coluna no banco)
async function _isContactMuted(phone, owner) {
  try {
    const { data } = await supabase.from('contacts').select('muted').eq('phone', phone).eq('owner', owner || ' ').maybeSingle();
    return !!(data && data.muted);
  } catch (_) { return false; }
}

// 📌 Fixar/desafixar conversa no topo
app.put('/contacts/:phone/pin', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const { error } = await supabase.from('contacts').update({ pinned: !!req.body.pinned })
    .eq('phone', _decSeguro(req.params.phone)).eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Apaga SÓ as mensagens da conversa — o lead continua no CRM e no Pipeline
// (etapa, etiquetas, anotações e tarefas são preservados)
app.delete("/contacts/:phone/messages", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const phone = _decSeguro(req.params.phone);
  await supabase.from("messages").delete().eq("phone", phone).eq("owner", req.owner || ' ');
  const { error } = await supabase.from("contacts")
    .update({ last_message_preview: null, last_message_direction: null, unread_count: 0, first_unread_at: null })
    .eq("phone", phone).eq("owner", req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// 🗑️ Excluir leads → LIXEIRA (settings lead_trash::owner): guarda o cadastro dos leads
// por 30 dias e NÃO apaga as mensagens (ficam guardadas pelo telefone) — assim
// "Restaurar" devolve o lead inteiro, com conversa, etapa, etiquetas e notas.
async function _leadTrash(owner) { try { const a = JSON.parse(_cfg('lead_trash', owner) || '[]'); return Array.isArray(a) ? a : []; } catch (_) { return []; } }
async function _leadTrashSalva(owner, lista) {
  const corte = Date.now() - 30 * 86400000;
  lista = lista.filter(x => x && x.deleted_at && new Date(x.deleted_at).getTime() > corte).slice(0, 500);
  const k = 'lead_trash::' + (owner || ' ');
  const value = JSON.stringify(lista);
  await supabase.from('settings').upsert({ key: k, value, updated_at: new Date().toISOString() });
  _settings[k] = value;
}
app.delete("/contacts/bulk-delete", async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  const { phones } = req.body;
  if (!Array.isArray(phones) || !phones.length) return res.status(400).json({ error: "phones obrigatório" });
  const OW = req.owner || ' ';
  const { data: rows } = await supabase.from('contacts').select('*').in('phone', phones).eq('owner', OW);
  const { error } = await supabase.from("contacts").delete().in("phone", phones).eq("owner", OW);
  if (error) return res.status(500).json({ error: error.message });
  try {
    const lixo = await _leadTrash(req.owner);
    const agora = new Date().toISOString();
    for (const r of (rows || [])) lixo.unshift({ contact: r, deleted_at: agora });
    await _leadTrashSalva(req.owner, lixo);
  } catch (e) { console.error('lixeira de leads:', e.message); }
  console.log(`🗑️ ${phones.length} lead(s) excluídos (na lixeira por 30 dias)`);
  res.json({ success: true, na_lixeira: (rows || []).length });
});
app.get('/contacts/trash', async (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const lixo = await _leadTrash(req.owner);
  res.json(lixo.map(x => ({ phone: x.contact.phone, name: x.contact.name, stage_id: x.contact.stage_id, deleted_at: x.deleted_at })));
});
app.post('/contacts/restore', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: "Supabase não configurado" });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  const fones = Array.isArray(req.body && req.body.phones) ? req.body.phones.map(String) : [];
  if (!fones.length) return res.status(400).json({ error: 'phones obrigatório' });
  const lixo = await _leadTrash(req.owner);
  let restaurados = 0, erros = [];
  for (const ph of fones) {
    const item = lixo.find(x => x.contact && x.contact.phone === ph);
    if (!item) { erros.push(ph + ': não está na lixeira'); continue; }
    const c = Object.assign({}, item.contact); delete c.created_at;
    const { error } = await supabase.from('contacts').upsert(c, { onConflict: 'owner,phone' });
    if (error) { erros.push(ph + ': ' + error.message); continue; }
    restaurados++;
  }
  await _leadTrashSalva(req.owner, lixo.filter(x => !fones.includes(x.contact && x.contact.phone) || erros.some(e => e.startsWith(x.contact.phone + ':'))));
  res.json({ ok: true, restaurados, erros });
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
    await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', last_message_status: null }).eq('phone', phone).eq('owner', owner || ' ');
  } catch(e) { console.error('recordBotFail:', e.message); }
}

// 📱 Descobre o número (conta) que o bot deve usar quando nada foi configurado:
// 1) o número da ÚLTIMA mensagem trocada com o lead, 2) o número do cadastro do
// lead, 3) o primeiro número do dono. Evita o disparo travar com "sem número".
async function _acctPadraoDoLead(phone, owner) {
  if (!supabase) return null;
  const OW = owner || ' ';
  try {
    const { data: ult } = await supabase.from('messages').select('account_id')
      .eq('phone', phone).eq('owner', OW).not('account_id', 'is', null)
      .order('timestamp', { ascending: false }).limit(1).maybeSingle();
    if (ult?.account_id) return ult.account_id;
  } catch (_) {}
  try {
    const { data: ct } = await supabase.from('contacts').select('account_id')
      .eq('phone', phone).eq('owner', OW).maybeSingle();
    if (ct?.account_id) return ct.account_id;
  } catch (_) {}
  try {
    const { data: acc } = await supabase.from('accounts').select('id')
      .eq('owner', OW).order('created_at', { ascending: true }).limit(1).maybeSingle();
    if (acc?.id) return acc.id;
  } catch (_) {}
  return null;
}

// 📷 FOTO DO BOT: envia a imagem (link público) com o texto como legenda
async function sendBotFoto(phone, acct, usedAcctId, imgUrl, legenda, owner) {
  const ts = new Date().toISOString();
  const prev = legenda ? (legenda.length > 80 ? legenda.slice(0, 80) + '…' : legenda) : '[Imagem]';
  const salva = async (wamid) => {
    if (!supabase) return;
    await supabase.from('messages').insert({
      phone, content: legenda || '[Imagem]', type: 'image', direction: 'outbound',
      timestamp: ts, account_id: usedAcctId, status: 'pending', wamid: wamid || null,
      owner: owner || null,
    });
    await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', last_message_status: null, unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
  };
  if (acct.evolution_instance) { // QR Code
    const r = await waSendRaw(acct.evolution_instance, phone, { image: { url: imgUrl }, caption: legenda || undefined });
    const wamid = r?.key?.id || null;
    await salva(wamid);
    return wamid || true;
  }
  const r = await axios.post(`https://graph.facebook.com/v23.0/${acct.phone_number_id}/messages`,
    { messaging_product: 'whatsapp', to: phone, type: 'image', image: { link: imgUrl, caption: legenda || undefined } },
    { headers: { Authorization: `Bearer ${acct.token}`, 'Content-Type': 'application/json' } });
  const wamid = r.data?.messages?.[0]?.id || null;
  await salva(wamid);
  await applyPendingStatus(wamid);
  return wamid;
}

async function sendBotMsg(phone, accountId, text, owner, nodeAccountId, imgUrl) {
  let acct, usedAcctId;
  if (nodeAccountId) {
    // Nó com número CONFIGURADO: obedece exatamente — sem troca automática
    acct = await botGetAcctStrict(nodeAccountId);
    usedAcctId = nodeAccountId;
    if (!acct) {
      await _recordBotFail(phone, text, 'O número configurado neste passo do bot não existe mais. Edite o nó "Enviar mensagem" e escolha outro número.', usedAcctId, owner, 'text');
      return null;
    }
  } else {
    // SEM número configurado no nó = NÃO ENVIA (bloqueio total, por segurança)
    await _recordBotFail(phone, text, '⛔ Envio BLOQUEADO: este passo do bot não tem número configurado. Abra o bot, clique no nó "Enviar mensagem" e escolha o número em "📱 Enviar pelo número".', accountId || null, owner, 'text');
    return null;
  }
  // 🚫 TRAVA ANTI-AUTOENVIO: bot nunca envia para o número da própria conta
  if (await _isSelfSend(phone, usedAcctId)) {
    await _recordBotFail(phone, text, '🚫 Bloqueado: o destino é o PRÓPRIO número desta conta (contato de teste no meio do disparo?).', usedAcctId, owner, 'text');
    return null;
  }
  const phoneNumberId = acct.phone_number_id, token = acct.token;
  // 📷 Passo com FOTO: manda a imagem (o texto vira legenda)
  if (imgUrl) {
    try { return await sendBotFoto(phone, acct, usedAcctId, imgUrl, text, owner); }
    catch (e) {
      console.error('❌ Bot foto:', e.response?.data || e.message);
      await _recordBotFail(phone, text || '[Imagem]', 'Falha ao enviar a foto do bot: ' + (metaErrorText(e.response?.data?.error) || e.message || ''), usedAcctId, owner, 'text');
      return null;
    }
  }
  // Conta QR Code: envia pelo PRÓPRIO número QR (igual ao envio manual)
  if (acct.evolution_instance) {
    try {
      const r = await waSendText(acct.evolution_instance, phone, text);
      const wamid = r?.key?.id || null;
      if (supabase) {
        const ts = new Date().toISOString();
        await supabase.from('messages').insert({ phone, content: text, type: 'text', direction: 'outbound', timestamp: ts, account_id: usedAcctId, status: 'pending', wamid, owner: owner || null });
        const prev = text.length > 80 ? text.substring(0, 80) + '…' : text;
        await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', last_message_status: null, unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
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
      // last_message_status: null é OBRIGATÓRIO — sem isso a prévia herdava o "lida"
      // da mensagem anterior e a cura retroativa pintava a mensagem do bot de azul
      await supabase.from('contacts').update({ last_message_at:ts, last_message_preview:prev, last_message_direction:'outbound', last_message_status:null, unread_count:0, first_unread_at:null }).eq('phone',phone).eq('owner',owner||' ');
    }
    return wamid;
  } catch(e) {
    console.error('❌ Bot sendMsg:', e.response?.data||e.message);
    await _recordBotFail(phone, text, metaErrorText(e.response?.data?.error) || (e.message || 'Falha no envio'), usedAcctId, owner, 'text');
    return null;
  }
}

// ESTRITO: devolve EXATAMENTE a conta pedida (ou null) — sem nenhum fallback.
// Usado quando o nó do bot tem um número configurado: ou envia por ele, ou falha.
async function botGetAcctStrict(accountId) {
  if (!supabase || !accountId) return null;
  const { data } = await supabase.from('accounts').select('id,phone_number_id,token,waba_id,type,evolution_instance').eq('id', accountId).maybeSingle();
  return data || null;
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
  let acct, usedAcctId;
  if (cfg.account_id) {
    // Nó com número CONFIGURADO: obedece exatamente — ou envia por ele, ou FALHA.
    // Nunca troca de número sozinho.
    acct = await botGetAcctStrict(cfg.account_id);
    usedAcctId = cfg.account_id;
    if (!acct) {
      await _recordBotFail(phone, `[Modelo: ${cfg.template_name}]`, 'O número configurado neste passo do bot não existe mais. Edite o nó "Enviar mensagem" e escolha outro número.', usedAcctId, owner, 'template');
      return null;
    }
    if (!acct.phone_number_id || !acct.token) {
      await _recordBotFail(phone, `[Modelo: ${cfg.template_name}]`, 'O número configurado neste passo é de QR Code — modelos só saem pela API oficial. Edite o nó e escolha um número da API.', usedAcctId, owner, 'template');
      return null;
    }
  } else {
    // SEM número configurado no nó = NÃO ENVIA (bloqueio total, por segurança).
    // Edite o nó "Enviar mensagem" e escolha o número.
    await _recordBotFail(phone, `[Modelo: ${cfg.template_name}]`, '⛔ Envio BLOQUEADO: este passo do bot não tem número configurado. Abra o bot, clique no nó "Enviar mensagem" e escolha o número em "📱 Enviar pelo número".', accountId || null, owner, 'template');
    return null;
  }
  // 🚫 TRAVA ANTI-AUTOENVIO: bot nunca envia modelo para o número da própria conta
  if (await _isSelfSend(phone, usedAcctId)) {
    await _recordBotFail(phone, `[Modelo: ${cfg.template_name}]`, '🚫 Bloqueado: o destino é o PRÓPRIO número desta conta (contato de teste no meio do disparo?).', usedAcctId, owner, 'template');
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
      await supabase.from('contacts').update({ last_message_at: ts, last_message_preview: prev, last_message_direction: 'outbound', last_message_status: null, unread_count: 0, first_unread_at: null }).eq('phone', phone).eq('owner', owner || ' ');
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

// ⚡ CACHE CURTO do desenho do bot (nós e setas) — cada passo fazia 2-4 consultas
// ao banco só para reler o fluxo; agora relê no máximo a cada 20s (e zera ao salvar)
const _botGraphCache = { nodes: new Map(), edges: new Map() };
const _BOT_GRAPH_TTL = 20000;
function _botGraphLimpa() { _botGraphCache.nodes.clear(); _botGraphCache.edges.clear(); }
async function _nodeById(id) {
  if (!supabase || !id) return null;
  const c = _botGraphCache.nodes.get(id);
  if (c && Date.now() - c.ts < _BOT_GRAPH_TTL) return c.v;
  const { data } = await supabase.from('bot_nodes').select('*').eq('id', id).maybeSingle();
  if (!data) return null; // nó ausente NÃO entra no cache (salvar o bot apaga e recria os nós)
  _botGraphCache.nodes.set(id, { v: data, ts: Date.now() });
  return data;
}
async function _edgesFrom(fromNodeId) {
  if (!supabase || !fromNodeId) return [];
  const c = _botGraphCache.edges.get(fromNodeId);
  if (c && Date.now() - c.ts < _BOT_GRAPH_TTL) return c.v;
  const { data } = await supabase.from('bot_edges').select('*').eq('from_node_id', fromNodeId);
  if (!data || !data.length) return []; // sem setas: não guarda (pode ser o instante do salvamento)
  _botGraphCache.edges.set(fromNodeId, { v: data, ts: Date.now() });
  return data;
}
async function getNextNodeId(fromNodeId, edgeLabel) {
  if (!supabase) return null;
  const edges = await _edgesFrom(fromNodeId);
  if (!edges?.length) return null;
  if (edgeLabel) {
    const m = edges.find(e => e.label && e.label.toLowerCase() === String(edgeLabel).toLowerCase());
    if (m) return m.to_node_id;
  }
  const def = edges.find(e => !e.label || e.label === '' || e.label === 'default');
  if (def) return def.to_node_id;
  // Pediu uma saída ESPECÍFICA que não existe (ex.: sem resposta, número do rodízio,
  // fora do horário) e não há saída padrão → PARA. Antes ele pegava a primeira seta
  // qualquer: quem não respondeu seguia pelo caminho do "sim".
  if (edgeLabel) return null;
  return edges[0]?.to_node_id || null;
}

async function stopRun(runId, status='completed') {
  if (supabase) await supabase.from('bot_runs').update({ status, updated_at:new Date().toISOString() }).eq('id', runId);
}

async function processNode(run, depth=0) {
  if (!supabase) return;
  if (depth > 30) { // fluxo em círculo: encerra (antes ficava "rodando" para sempre)
    console.error('🤖 Bot com muitos passos seguidos — execução encerrada:', run && run.id);
    try { await stopRun(run.id, 'stopped'); } catch (_) {}
    return;
  }
  const { id:runId, contact_phone:phone, account_id:acctId, current_node_id:nodeId, owner:botOwner } = run;
  const OW = botOwner || ' '; // sentinela p/ escopo por dono
  const node = await _nodeById(nodeId);
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
    // Número deste passo: o configurado NO NÓ ou o HERDADO da execução
    // (definido por um passo anterior com número ou pelo Round Robin).
    // Só se pergunta uma vez — os passos seguintes herdam automaticamente.
    // Sem número em lugar nenhum? NÃO bloqueia mais: escolhe sozinho o número
    // certo para este lead (o da última conversa dele, o do cadastro ou, em
    // último caso, o primeiro número do dono) — antes o disparo parava com erro.
    const nodeAcct = cfg.account_id || run.account_id || await _acctPadraoDoLead(phone, botOwner);
    if (cfg.account_id && run.account_id !== cfg.account_id) {
      try { await supabase.from('bot_runs').update({ account_id: cfg.account_id, updated_at: new Date().toISOString() }).eq('id', runId); } catch (_) {}
      run.account_id = cfg.account_id; // os próximos passos herdam este número
    }
    if (cfg.mode === 'template' && cfg.template_name) {
      sendOk = await sendBotTemplate(phone, acctId, { ...cfg, account_id: nodeAcct }, name, notes, botOwner);
    } else {
      const text = applyVars(cfg.text || '', name, phone, notes);
      // Passo pode ter FOTO (com o texto de legenda) — sem texto e sem foto = nada a fazer
      sendOk = (text || cfg.image_url) ? await sendBotMsg(phone, acctId, text, botOwner, nodeAcct, cfg.image_url || null) : true;
    }
    // resolve as arestas deste nó (sucesso = sem rótulo / falha = __failed__)
    const medges = await _edgesFrom(nodeId);
    const okNxt   = medges?.find(e=>!e.label||e.label===''||e.label==='default')?.to_node_id || null;
    const failNxt = medges?.find(e=>(e.label||'').toLowerCase()==='__failed__')?.to_node_id || null;
    const hasButtons = cfg.mode === 'template' && Array.isArray(cfg.buttons) && cfg.buttons.length > 0;
    if (!sendOk && failNxt) {
      await supabase.from('bot_runs').update({ current_node_id:failNxt, updated_at:new Date().toISOString() }).eq('id',runId);
      await processNode({...run,current_node_id:failNxt}, depth+1);
    } else if (!sendOk) {
      await stopRun(runId,'failed');
    } else if (hasButtons && (medges && medges.length)) {
      // Modelo com botões: aguarda o lead clicar num botão (ramifica conforme o botão).
      // Sem NENHUM caminho ligado, não há o que esperar → encerra (não fica "rodando").
      const _hrs = (cfg.timeout_hours && cfg.timeout_hours > 0) ? cfg.timeout_hours : 48;
      let pauseUntil = new Date(Date.now() + _hrs * 3600000).toISOString();
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
    await supabase.from('tasks').insert({ phone, account_id:acctId||null, title, due_at:due, owner:botOwner||null, created_at:new Date().toISOString() });
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
    // Sem prazo configurado, usa 48h de segurança: execução parada para sempre
    // travava o lead fora dos próximos disparos em massa desse bot.
    const _hrs = (cfg.timeout_hours && cfg.timeout_hours > 0) ? cfg.timeout_hours : 48;
    let pauseUntil = new Date(Date.now() + _hrs * 3600000).toISOString();
    await supabase.from('bot_runs').update({ status:'waiting_reply', pause_until:pauseUntil, updated_at:new Date().toISOString() }).eq('id',runId);

  } else if (node.type === 'pause') {
    const ms = ((cfg.days||0)*24+(cfg.hours||0))*3600000 + (cfg.minutes||0)*60000 + (cfg.seconds||0)*1000;
    const waitMs = Math.max(ms, 1000);
    const pauseUntil = new Date(Date.now()+waitMs).toISOString();
    await supabase.from('bot_runs').update({ status:'paused', pause_until:pauseUntil, updated_at:new Date().toISOString() }).eq('id',runId);
    // Espera CURTA (até 2 min): cronômetro EXATO na memória — retoma na hora certa.
    // O ciclo de 30s fica só para esperas longas e como segurança pós-reinício.
    const _prof = depth; // mantém a contagem de passos (senão um fluxo em círculo nunca para)
    if (waitMs <= 120000) {
      // "digitando…" para o lead enquanto o cronômetro roda, se o PRÓXIMO passo é uma mensagem
      let typingTimer = null;
      try {
        const nxtPeek = await getNextNodeId(nodeId, null);
        if (nxtPeek) {
          const nxNode = await _nodeById(nxtPeek);
          if (nxNode && nxNode.type === 'message') {
            const typeAcct = (nxNode.config && nxNode.config.account_id) || run.account_id || null;
            if (typeAcct) {
              botTypingPulse(phone, typeAcct).then(via => {
                if (!via) return;
                const ritmo = via === 'qr' ? 8000 : 22000; // API: renovar cedo demais cancela o indicador
                typingTimer = setInterval(() => botTypingPulse(phone, typeAcct), ritmo);
                setTimeout(() => { if (typingTimer) { clearInterval(typingTimer); typingTimer = null; } }, waitMs + 2000);
              }).catch(()=>{});
            }
          }
        }
      } catch (_) {}
      setTimeout(async () => {
        try {
          if (typingTimer) { clearInterval(typingTimer); typingTimer = null; }
          // Atômico: só retoma se AINDA estiver pausada (evita corrida com o ciclo de 30s)
          const { data: took } = await supabase.from('bot_runs')
            .update({ status:'running', pause_until:null, updated_at:new Date().toISOString() })
            .eq('id', runId).eq('status', 'paused').select('id');
          if (!took || !took.length) return;
          const nxt = await getNextNodeId(nodeId, null);
          if (nxt) {
            await supabase.from('bot_runs').update({ current_node_id:nxt, updated_at:new Date().toISOString() }).eq('id',runId);
            await processNode({ ...run, current_node_id:nxt, status:'running' }, _prof + 1);
          } else {
            await stopRun(runId,'completed');
          }
        } catch (e) { console.error('Retomada de pausa curta:', e.message); }
      }, waitMs);
    }

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
  // VALIDADE: execução parada há mais de 48h é FANTASMA — encerra em silêncio e
  // NUNCA dispara nada na conversa (a resposta segue para o atendimento normal).
  const ageMs = Date.now() - new Date(run.updated_at || run.created_at || 0).getTime();
  if (ageMs > 48*3600000) { await stopRun(run.id, 'stopped'); return false; }
  const edges = await _edgesFrom(run.current_node_id);
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

// ── AÇÕES DE ETAPA (Automação do funil): executadas quando o lead ENTRA na etapa.
// Tipos: task (criar tarefa), complete_task (concluir), tags (editar), move_stage
// (mudar etapa, com proteção contra loop), create_lead (criar lead fixo).
// NENHUMA delas envia mensagem ao cliente.
async function runStageActions(phone, stageId, owner, depth = 0) {
  if (!supabase || depth > 3) return; // proteção contra correntes infinitas de "mudar etapa"
  try {
    const { data } = await supabase.from('settings').select('value').eq('key', 'stage_actions::' + (owner || ' ')).maybeSingle();
    if (!data || !data.value) return;
    let cfg; try { cfg = typeof data.value === 'string' ? JSON.parse(data.value) : data.value; } catch (_) { return; }
    const actions = (cfg && cfg[stageId]) || [];
    const OW = owner || ' ';
    for (const a of actions) {
      try {
        if (a.type === 'task' && a.title) {
          const { data: ct } = await supabase.from('contacts').select('name').eq('phone', phone).eq('owner', OW).maybeSingle();
          const title = applyVars(a.title, ct?.name || phone, phone);
          const due = a.due_hours ? new Date(Date.now() + Number(a.due_hours) * 3600000).toISOString() : null;
          await supabase.from('tasks').insert({ phone, title, due_at: due, owner: owner || null, created_at: new Date().toISOString() });
        } else if (a.type === 'complete_task') {
          let q = supabase.from('tasks').update({ done: true }).eq('phone', phone).eq('done', false).eq('owner', OW);
          if (a.title_filter) q = q.ilike('title', '%' + a.title_filter + '%');
          await q;
        } else if (a.type === 'tags') {
          const { data: ct } = await supabase.from('contacts').select('tags').eq('phone', phone).eq('owner', OW).maybeSingle();
          let tags = Array.isArray(ct?.tags) ? ct.tags.slice() : [];
          (a.add || []).forEach(t => { if (t && !tags.includes(t)) tags.push(t); });
          if (a.remove && a.remove.length) tags = tags.filter(t => !a.remove.includes(t));
          await supabase.from('contacts').update({ tags }).eq('phone', phone).eq('owner', OW);
        } else if (a.type === 'move_stage' && a.stage_id && a.stage_id !== stageId) {
          await supabase.from('contacts').update({ stage_id: a.stage_id }).eq('phone', phone).eq('owner', OW);
          await fireStageBots(phone, a.stage_id, owner, depth + 1); // encadeia com limite
        } else if (a.type === 'create_lead' && a.phone) {
          const np = String(a.phone).replace(/\D/g, '');
          if (np) await supabase.from('contacts').upsert(
            { phone: np, name: a.name || np, stage_id: a.lead_stage_id || null, owner: owner || null },
            { onConflict: 'owner,phone' });
        }
      } catch (e) { console.error('Ação de etapa falhou:', a.type, e.message); }
    }
  } catch (e) { console.error('runStageActions:', e.message); }
}

// Configuração das ações de etapa (Automação do funil)
app.get('/stage-actions', async (req, res) => {
  if (!supabase) return res.json({ value: {} });
  const { data } = await supabase.from('settings').select('value').eq('key', 'stage_actions::' + (req.owner || ' ')).maybeSingle();
  let v = {};
  try { v = data?.value ? (typeof data.value === 'string' ? JSON.parse(data.value) : data.value) : {}; } catch (_) {}
  res.json({ value: v });
});
app.put('/stage-actions', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const v = JSON.stringify(req.body?.value || {});
  const { error } = await supabase.from('settings').upsert({ key: 'stage_actions::' + (req.owner || ' '), value: v, updated_at: new Date().toISOString() });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Dispara todos os bots com gatilho "entrou na etapa" para um lead (do dono certo)
async function fireStageBots(phone, stageId, owner, depth = 0) {
  if (!supabase || !stageId || !phone) return;
  // Ações de etapa (tarefas, tags, mover, criar lead) — antes dos bots
  await runStageActions(phone, stageId, owner, depth);
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

async function startBot(botId, phone, accountId, owner, seedAccount, emSegundoPlano) {
  if (!supabase) return null;
  let ownerEmail = owner;
  if (!ownerEmail) { const { data:b } = await supabase.from('bots').select('owner').eq('id',botId).maybeSingle(); ownerEmail = b?.owner || null; }
  await supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() }).eq('contact_phone',phone).eq('bot_id',botId).in('status',['running','waiting_reply','paused']);
  const { data:startNodes } = await supabase.from('bot_nodes').select('id').eq('bot_id',botId).eq('type','start').limit(1);
  const startNode = startNodes && startNodes[0];
  if (!startNode) { console.error('❌ Bot sem nó start:', botId); return null; }
  const { data:run, error } = await supabase.from('bot_runs').insert({
    // account_id começa VAZIO de propósito: o número usado nos envios vem SÓ dos
    // nós configurados ou do Round Robin — nunca de uma conta implícita do lead.
    // EXCEÇÃO (seedAccount): disparo MANUAL de dentro do chat — usa o número da
    // própria conversa (o "Enviar de" que está na tela), escolha explícita da usuária.
    bot_id:botId, contact_phone:phone, account_id:(seedAccount && accountId) ? accountId : null,
    current_node_id:startNode.id, status:'running', owner:ownerEmail||null,
    created_at:new Date().toISOString(), updated_at:new Date().toISOString()
  }).select().single();
  if (error) { console.error('❌ Bot run insert:', error.message); return null; }
  // Disparo manual do chat: devolve já (a tela mostra "bot ativo" na hora) e o
  // primeiro passo roda em seguida — sem o botão ficar travado esperando o envio
  if (emSegundoPlano) { processNode(run).catch(e => console.error('processNode bg:', e.message)); return run; }
  await processNode(run);
  return run;
}

// FAXINA na subida do servidor: execuções de bot paradas há mais de 48h são
// fantasmas de disparos antigos — encerra TODAS em silêncio, nada acorda depois.
(async () => {
  try {
    if (!supabase) return;
    const cutoff = new Date(Date.now() - 48*3600000).toISOString();
    await supabase.from('bot_runs').update({ status:'stopped', updated_at:new Date().toISOString() })
      .in('status',['running','waiting_reply','paused']).lt('updated_at', cutoff);
    console.log('🧹 Faxina de bots fantasmas concluída');
  } catch (_) {}
})();

// Timer: retoma runs pausadas/expiradas do bot.
// 30s (era 5s) — economiza CPU/banda no Railway; as esperas dos bots são de
// minutos/horas, então até 30s de folga não muda nada na prática.
let _retomaOcupado = false;
setInterval(async () => {
  if (!supabase || _retomaOcupado) return; // um ciclo por vez (senão o mesmo passo rodava 2x)
  _retomaOcupado = true;
  try {
  const now = new Date().toISOString();
  const { data:paused } = await supabase.from('bot_runs').select('*').in('status',['paused','waiting_reply']).lte('pause_until',now).not('pause_until','is',null);
  for (const run of paused||[]) {
    // 🔒 CLAIM: só continua quem conseguir "pegar" a execução (evita o mesmo passo
    // sair duas vezes quando o cronômetro curto e este ciclo se cruzam)
    const { data: pego } = await supabase.from('bot_runs')
      .update({ pause_until: null, updated_at: new Date().toISOString() })
      .eq('id', run.id).eq('status', run.status).not('pause_until', 'is', null).select('id');
    if (!pego || !pego.length) continue;
    const _devolve = async () => { try { await supabase.from('bot_runs').update({ pause_until: run.pause_until }).eq('id', run.id).eq('status', run.status); } catch (_) {} };
    try {
    // EXPIRADA: se a hora de retomar passou há mais de 15 min (servidor reiniciou,
    // execução esquecida), NÃO envia nada "do nada" — encerra em silêncio.
    if (Date.now() - new Date(run.pause_until).getTime() > 15*60000) { await stopRun(run.id,'stopped'); continue; }
    // Se o nó atual é "Horário comercial", re-avalia o próprio nó (não avança)
    const curNode = await _nodeById(run.current_node_id);
    if (curNode?.type === 'business_hours') {
      await supabase.from('bot_runs').update({ status:'running', pause_until:null, updated_at:now }).eq('id',run.id);
      await processNode({...run, status:'running'});
      continue;
    }
    // Só as saídas de "sem resposta". Sem elas desenhadas, a execução ENCERRA —
    // antes ela seguia pela primeira seta qualquer (quem não respondeu ia pelo "Sim")
    const nxt = await getNextNodeId(run.current_node_id, '__timeout__') || await getNextNodeId(run.current_node_id, '__other__');
    if (nxt) { await supabase.from('bot_runs').update({ current_node_id:nxt, status:'running', pause_until:null, updated_at:now }).eq('id',run.id); await processNode({...run,current_node_id:nxt,status:'running'}); }
    else { await stopRun(run.id,'completed'); }
    } catch (e) { console.error('⏰ retomada de uma execução:', e.message); await _devolve(); }
  }
  } catch (e) { console.error('⏰ retomada de bots:', e.message); }
  finally { _retomaOcupado = false; }
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
  if ((_cfg('faq_enabled', owner) || 'off') !== 'on') return false; // interruptor DA CONTA

  // Filtro por conta de WhatsApp: se 'faq_accounts' foi configurado (lista JSON de IDs),
  // a IA só responde nas contas dessa lista. Se nunca foi configurado, vale para TODAS.
  const accSetting = _cfg('faq_accounts', owner);
  if (accSetting !== undefined && accSetting !== null && accSetting !== '') {
    try {
      const list = JSON.parse(accSetting);
      if (Array.isArray(list) && !list.map(String).includes(String(accountId))) return false;
    } catch (_) {}
  }

  // Modo IA (Groq): entende o contexto da conversa. Se falhar, cai no texto grátis.
  let m = null;
  if (_faqAiOn(owner)) {
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
  const delaySec = parseInt(_cfg('faq_delay_seconds', owner), 10);
  const delayMs = Math.max(0, (Number.isFinite(delaySec) ? delaySec : 25) * 1000);
  setTimeout(async () => {
    try {
      const wamid = await sendBotMsg(phone, acct, m.faq.answer, owner, acct || await _acctPadraoDoLead(phone, owner));
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
const _faqAiOn = (owner) => _cfg('faq_mode', owner) === 'ai' && !!process.env.GROQ_API_KEY;

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

  const model = _cfg('faq_ai_model', owner) || GROQ_DEFAULT_MODEL;
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
    mode: _cfg('faq_mode', req.owner) || 'text',
    keyConfigured: !!process.env.GROQ_API_KEY,
    model: _cfg('faq_ai_model', req.owner) || GROQ_DEFAULT_MODEL
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

function matchWrongPerson(text, owner) {
  const raw = _cfg('wrongperson_triggers', owner) || WRONGPERSON_DEFAULT_TRIGGERS;
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
  if ((_cfg('wrongperson_enabled', owner) || 'off') !== 'on') return false;

  // mesmo filtro de contas do FAQ
  const accSetting = _cfg('faq_accounts', owner);
  if (accSetting !== undefined && accSetting !== null && accSetting !== '') {
    try {
      const list = JSON.parse(accSetting);
      if (Array.isArray(list) && !list.map(String).includes(String(accountId))) return false;
    } catch (_) {}
  }

  if (!matchWrongPerson(text, owner)) return false;

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

  const answer = _cfg('wrongperson_answer', owner) || WRONGPERSON_DEFAULT_ANSWER;
  const tag = _cfg('wrongperson_tag', owner) || WRONGPERSON_DEFAULT_TAG;
  const delaySec = parseInt(_cfg('faq_delay_seconds', owner), 10);
  const delayMs = Math.max(0, (Number.isFinite(delaySec) ? delaySec : 25) * 1000);

  setTimeout(async () => {
    try {
      const wamid = await sendBotMsg(phone, acct, answer, owner, acct || await _acctPadraoDoLead(phone, owner));
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
function _tagCatalog(owner) {
  try { const a = JSON.parse(_cfg('tag_catalog', owner) || '[]'); return Array.isArray(a) ? a : []; }
  catch (_) { return []; }
}
async function _saveTagCatalog(arr, owner) {
  const uniq = Array.from(new Set(arr.filter(Boolean)));
  const k = 'tag_catalog::' + (owner || ' '); // catálogo POR CONTA
  await supabase.from('settings').upsert({ key: k, value: JSON.stringify(uniq), updated_at: new Date().toISOString() });
  _settings[k] = JSON.stringify(uniq);
}

// Lista tags com contagem de leads (inclui as do catálogo com contagem 0)
app.get('/tags/manage', async (req, res) => {
  if (!supabase) return res.json([]);
  const { data, error } = await supabase.from('contacts').select('tags').eq('owner', req.owner || ' ');
  if (error) return res.status(500).json({ error: error.message });
  const counts = {};
  (data || []).forEach(c => (c.tags || []).forEach(t => { if (t) counts[t] = (counts[t] || 0) + 1; }));
  _tagCatalog(req.owner).forEach(t => { if (!(t in counts)) counts[t] = 0; });
  const out = Object.keys(counts).sort((a, b) => a.localeCompare(b)).map(name => ({ name, count: counts[name] }));
  res.json(out);
});

// Cria/cadastra uma tag no catálogo
app.post('/tags/manage', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'sem banco' });
  const name = ((req.body && req.body.name) || '').toString().trim();
  if (!name) return res.status(400).json({ error: 'nome obrigatório' });
  await _saveTagCatalog([..._tagCatalog(req.owner), name], req.owner);
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
  await _saveTagCatalog(_tagCatalog(req.owner).filter(t => t !== name), req.owner);
  res.json({ ok: true });
});

// ── CRUD de Bots ──
app.get('/bots', async (req,res) => {
  if (!supabase) return res.json([]);
  const { data,error } = await supabase.from('bots').select('*').eq('owner', req.owner || ' ').order('created_at',{ascending:false});
  if (error) return res.status(500).json({error:error.message});
  const bots = data || [];
  // 📈 Estatísticas leves por bot (execuções, última, em andamento) para os cartões
  try {
    if (String(req.query.stats || '') === '1' && bots.length) {
      const ids = bots.map(b => b.id);
      const { data: runs } = await supabase.from('bot_runs').select('bot_id, status, created_at').in('bot_id', ids).order('created_at', { ascending: false }).limit(5000);
      const st = {};
      for (const r of (runs || [])) {
        const o = st[r.bot_id] = st[r.bot_id] || { execucoes: 0, andamento: 0, ultima: null };
        o.execucoes++;
        if (r.status === 'waiting_reply' || r.status === 'running') o.andamento++;
        if (!o.ultima) o.ultima = r.created_at;
      }
      for (const b of bots) b._stats = st[b.id] || { execucoes: 0, andamento: 0, ultima: null };
    }
  } catch (_) {}
  res.json(bots);
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
  _botGraphLimpa();
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
    _botGraphLimpa();
    await supabase.from('bot_edges').delete().eq('bot_id',botId);
    await supabase.from('bot_nodes').delete().eq('bot_id',botId);
    if (nodes?.length) { const { error:ne } = await supabase.from('bot_nodes').insert(nodes.map(n=>({ id:n.id, bot_id:botId, type:n.type, label:n.label||'', config:n.config||{}, pos_x:Math.round(n.pos_x||0), pos_y:Math.round(n.pos_y||0), owner:req.owner||null }))); if (ne) throw ne; }
    if (edges?.length) { const { error:ee } = await supabase.from('bot_edges').insert(edges.map(e=>({ id:e.id, bot_id:botId, from_node_id:e.from_node_id, to_node_id:e.to_node_id, label:e.label||'', owner:req.owner||null }))); if (ee) throw ee; }
    _botGraphLimpa(); // limpa de novo DEPOIS de gravar (o fluxo novo entra em vigor na hora)
    res.json({success:true});
  } catch(err) { _botGraphLimpa(); res.status(500).json({error:err.message}); }
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
  const run = await startBot(req.params.id, phone, account_id, req.owner, true, true); // manual no chat: herda o número da conversa; responde na hora
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
  // 🏁 Bot que já chegou ao FIM (o passo atual não leva a lugar nenhum) não é
  // mais "em andamento": encerra e some da tela — não faz sentido "parar" o que
  // já acabou.
  const vivos = [];
  for (const r of (data || [])) {
    try {
      if (!r.current_node_id) { await stopRun(r.id, 'completed'); continue; }
      const { data: saidas } = await supabase.from('bot_edges').select('id').eq('from_node_id', r.current_node_id).limit(1);
      const { data: nd } = await supabase.from('bot_nodes').select('type').eq('id', r.current_node_id).maybeSingle();
      // Encerrado quando: não há para onde ir, é o nó Fim, o passo sumiu do bot,
      // ou a espera está parada há mais de 7 dias (ninguém vai responder mais)
      const parado7d = ['waiting_reply', 'paused'].includes(r.status) &&
        (Date.now() - new Date(r.updated_at || r.created_at || 0).getTime()) > 7 * 24 * 3600 * 1000;
      const acabou = (!saidas || !saidas.length) || !nd || (nd && nd.type === 'end') || parado7d;
      if (acabou) { await stopRun(r.id, 'completed'); continue; }
    } catch (_) {}
    vivos.push(r);
  }
  res.json(vivos);
});

// ═══════════════════════════════════════
// CONFIGURAÇÕES: INTEGRAÇÃO (token) + FATURAMENTO
// ═══════════════════════════════════════
// ⚡ Respostas rápidas COMPARTILHADAS: mesma lista em todos os aparelhos e números da conta
app.get('/quick-replies', async (req, res) => {
  if (!supabase) return res.json([]);
  const { data } = await supabase.from('settings').select('value').eq('key', 'quick_replies::' + (req.owner || ' ')).maybeSingle();
  try { res.json(data?.value ? JSON.parse(data.value) : []); } catch (_) { res.json([]); }
});
app.put('/quick-replies', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const arr = Array.isArray(req.body) ? req.body : [];
  await supabase.from('settings').upsert({ key: 'quick_replies::' + (req.owner || ' '), value: JSON.stringify(arr), updated_at: new Date().toISOString() });
  res.json({ success: true });
});

app.get('/integration/token', async (req, res) => {
  if (!supabase) return res.json({ token: null });
  const { data } = await supabase.from('settings').select('value').eq('key', 'api_token::' + (req.owner || ' ')).maybeSingle();
  res.json({ token: data?.value || null });
});
app.post('/integration/token', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const token = 'vetra_' + require('crypto').randomBytes(24).toString('hex');
  const key = 'api_token::' + (req.owner || ' ');
  const { error } = await supabase.from('settings').upsert({ key, value: token, updated_at: new Date().toISOString() });
  if (error) return res.status(500).json({ error: error.message });
  _settings[key] = token; // vale imediatamente, sem esperar o recarregamento
  res.json({ token });
});
// Faturamento (exibição): plano e validade ficam em settings (chave billing::<dono>)
app.get('/billing', async (req, res) => {
  if (!supabase) return res.json({ value: null });
  const { data } = await supabase.from('settings').select('value').eq('key', 'billing::' + (req.owner || ' ')).maybeSingle();
  let v = null;
  try { v = data?.value ? (typeof data.value === 'string' ? JSON.parse(data.value) : data.value) : null; } catch (_) {}
  res.json({ value: v });
});

// ═══════════════════════════════════════
// LEMBRETE DE TAREFAS EM ABERTO
// Push a cada 20 min se houver tarefa aberta — com liga/desliga e janela
// de dias/horários POR DIA da semana (horário de Brasília, UTC-3)
// ═══════════════════════════════════════
function _remKey(owner) { return 'task_reminder::' + (owner || ' '); }

app.get('/task-reminder', async (req, res) => {
  if (!supabase) return res.json({ value: null });
  const { data } = await supabase.from('settings').select('value').eq('key', _remKey(req.owner)).maybeSingle();
  let v = null;
  try { v = data?.value ? (typeof data.value === 'string' ? JSON.parse(data.value) : data.value) : null; } catch (_) {}
  res.json({ value: v });
});

app.put('/task-reminder', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  const v = JSON.stringify(req.body?.value || {});
  const { error } = await supabase.from('settings').upsert({ key: _remKey(req.owner), value: v, updated_at: new Date().toISOString() });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Verificação a cada 20 minutos (leve: 1 consulta de settings + 1 contagem por dono ativo)
setInterval(async () => {
  if (!supabase || !webpush) return;
  try {
    const { data: rows } = await supabase.from('settings').select('key, value').like('key', 'task_reminder::%');
    for (const row of rows || []) {
      let cfg;
      try { cfg = typeof row.value === 'string' ? JSON.parse(row.value) : row.value; } catch (_) { continue; }
      if (!cfg || !cfg.enabled) continue;
      // Janela do DIA atual (Brasília): 0=Dom … 6=Sáb
      const brt = new Date(Date.now() - 3 * 3600000);
      const d = cfg.days?.[String(brt.getUTCDay())];
      if (!d || !d.on) continue;
      const hm = String(brt.getUTCHours()).padStart(2, '0') + ':' + String(brt.getUTCMinutes()).padStart(2, '0');
      if (hm < (d.start || '09:00') || hm >= (d.end || '18:00')) continue;
      const owner = row.key.slice('task_reminder::'.length);
      const ownerVal = owner === ' ' ? null : owner;
      let tq = supabase.from('tasks').select('id', { count: 'exact', head: true }).eq('done', false);
      tq = ownerVal ? tq.eq('owner', ownerVal) : tq.is('owner', null);
      const { count } = await tq;
      if (!count) continue;
      await sendPushToOwner(ownerVal, {
        title: '✅ Tarefas em aberto',
        body: `Você tem ${count} tarefa${count > 1 ? 's' : ''} em aberto no VETRA`,
        tag: 'task-reminder'
      });
    }
  } catch (e) { console.error('Lembrete de tarefas:', e.message); }
}, 20 * 60 * 1000);

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
// Aviso claro se a configuração antiga de "todos na mesma conta" ainda estiver no banco
setTimeout(() => {
  if (_settings['owner_default']) console.warn('⚠️ A configuração antiga "owner_default" existe no banco mas está IGNORADA — cada e-mail agora é uma conta separada. Use a Equipe (Configurações) para compartilhar de propósito.');
}, 30000);

// 🔒 SEPARAÇÃO POR CONTA: estas chaves eram GLOBAIS (a configuração de uma conta
// valia para a outra). Agora cada dona tem a sua (chave::email). Os valores
// antigos (sem ::) pertencem à dona ORIGINAL e valem como herança SÓ para ela —
// nada muda para quem já configurou; as outras contas começam do zero.
const OWNER_LEGADO = 'elianecezaroliveira@gmail.com';
const CHAVES_POR_CONTA = new Set([
  'tag_catalog', 'n8n_webhook_url',
  'faq_enabled', 'faq_mode', 'faq_accounts', 'faq_delay_seconds', 'faq_ai_model',
  'wrongperson_enabled', 'wrongperson_triggers', 'wrongperson_tag', 'wrongperson_answer',
  'tmpl_alias', // apelidos dos modelos da API (nome só no CRM)
  'sheets_sync', // planilha do Google ligada ao pipeline (importação de leads)
  'drip_rules',  // gotejamento: mover leads aos poucos de uma etapa para outra
  'stage_trash', // lixeira de colunas do pipeline (desfazer exclusão)
  'lead_trash'   // lixeira de leads excluídos (30 dias)
]);
function _cfg(key, owner) {
  const own = owner || ' ';
  const v = _settings[key + '::' + own];
  if (v !== undefined && v !== null) return v;
  return own === OWNER_LEGADO ? _settings[key] : undefined;
}

// ── 👥 EQUIPE: e-mails que entram na MINHA conta (e veem os mesmos dados) ──
function _equipeMapa() { try { return JSON.parse(_settings['owner_aliases'] || '{}') || {}; } catch (_) { return {}; } }
async function _equipeSalva(mapa) {
  await supabase.from('settings').upsert({ key: 'owner_aliases', value: JSON.stringify(mapa), updated_at: new Date().toISOString() });
  _settings['owner_aliases'] = JSON.stringify(mapa);
  for (const t in _tokenOwner) delete _tokenOwner[t]; // vale na hora (sem esperar 5 min)
}
app.get('/equipe', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  const mapa = _equipeMapa();
  const meus = Object.keys(mapa).filter(e => String(mapa[e]).toLowerCase() === String(req.owner).toLowerCase());
  res.json({ dono: req.owner, membros: meus });
});
app.post('/equipe', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  const email = String(req.body?.email || '').trim().toLowerCase();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return res.status(400).json({ error: 'E-mail inválido.' });
  if (email === String(req.owner).toLowerCase()) return res.status(400).json({ error: 'Este é o seu próprio e-mail.' });
  const mapa = _equipeMapa();
  const donoAtual = mapa[email];
  if (donoAtual && String(donoAtual).toLowerCase() !== String(req.owner).toLowerCase())
    return res.status(409).json({ error: 'Este e-mail já faz parte de outra conta.' });
  // Não engole uma conta que já tem dados próprios
  try {
    const { count } = await supabase.from('contacts').select('phone', { count: 'exact', head: true }).eq('owner', email);
    if (count && count > 0) return res.status(409).json({ error: 'Este e-mail já tem uma conta com dados próprios no VETRA. Ele precisa entrar com a conta dele.' });
  } catch (_) {}
  mapa[email] = String(req.owner).toLowerCase();
  await _equipeSalva(mapa);
  console.log('👥 Equipe: ' + email + ' agora entra na conta de ' + req.owner);
  res.json({ ok: true, membros: Object.keys(mapa).filter(e => String(mapa[e]).toLowerCase() === String(req.owner).toLowerCase()) });
});
app.delete('/equipe/:email', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  const email = _decSeguro(req.params.email).trim().toLowerCase();
  const mapa = _equipeMapa();
  if (!mapa[email]) return res.json({ ok: true, membros: Object.keys(mapa).filter(e => String(mapa[e]).toLowerCase() === String(req.owner).toLowerCase()) });
  if (String(mapa[email]).toLowerCase() !== String(req.owner).toLowerCase())
    return res.status(403).json({ error: 'Este e-mail não faz parte da sua conta.' });
  delete mapa[email];
  await _equipeSalva(mapa);
  console.log('👥 Equipe: ' + email + ' saiu da conta de ' + req.owner);
  res.json({ ok: true, membros: Object.keys(mapa).filter(e => String(mapa[e]).toLowerCase() === String(req.owner).toLowerCase()) });
});

// 🔒 Chaves de settings que NUNCA passam pela rota genérica (segredos/globais)
const _SETTINGS_PROIBIDAS = /^(owner_default|owner_aliases|vapid_keys|api_token(::.*)?|notices(::.*)?|drip_rules(::.*)?|sheets_sync(::.*)?|.*token.*|.*secret.*)$/i;
app.get('/settings/:key', async (req, res) => {
  if (!supabase) return res.json({ value: null });
  const k = req.params.key;
  if (_SETTINGS_PROIBIDAS.test(k)) return res.status(403).json({ error: 'chave protegida' });
  if (CHAVES_POR_CONTA.has(k)) { const v = _cfg(k, req.owner); return res.json({ value: (v === undefined ? null : v) }); }
  const { data } = await supabase.from('settings').select('value').eq('key', k).maybeSingle();
  res.json({ value: data?.value || null });
});

app.put('/settings/:key', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
  if (_SETTINGS_PROIBIDAS.test(req.params.key)) return res.status(403).json({ error: 'chave protegida' });
  const { value } = req.body;
  const k = CHAVES_POR_CONTA.has(req.params.key)
    ? req.params.key + '::' + (req.owner || ' ')   // grava SEMPRE na chave da conta
    : req.params.key;
  const { error } = await supabase.from('settings').upsert({ key: k, value, updated_at: new Date().toISOString() });
  if (error) return res.status(500).json({ error: error.message });
  _settings[k] = value;
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
  // 🔒 SEM DONO = não envia. Antes, avisos "sem dono" iam para TODOS os aparelhos
  // cadastrados sem dono (de contas diferentes) — era o que fazia o contador de
  // uma conta aparecer/sumir por causa da outra.
  if (!owner) { console.warn('🔕 Push ignorado: mensagem sem dono definido'); return; }
  try {
    // Total de MENSAGENS não lidas DESTE dono → número no ícone do app
    try {
      const { data: rows } = await supabase.from('contacts')
        .select('unread_count').gt('unread_count', 0).eq('owner', owner);
      payload.badge = (rows || []).reduce((s, r) => s + (r.unread_count || 0), 0);
    } catch (_) {}
    payload.owner = owner; // o aparelho confere se o aviso é mesmo dele

    const { data: subs } = await supabase.from('push_subscriptions')
      .select('endpoint, subscription').eq('owner', owner);
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
const _waPairing = {}; // QR já LIDO no celular, conexão subindo → o front avisa na hora
const _waPairedAt = {}; // quando pareou NESTE processo → janela de reconexão rápida (10 min)
// Cache das mensagens recentes (recebidas E enviadas): quando o aparelho do lead
// pede "retry" na renegociação de chaves pós-pareamento, o motor REENTREGA na hora.
// Sem isso, mensagens e recibos ficavam atrasados nos primeiros minutos/horas.
const _waMsgCache = new Map();
function _waMsgCacheSet(id, msg) {
  if (!id || !msg) return;
  _waMsgCache.set(String(id), msg);
  if (_waMsgCache.size > 800) { for (const k of _waMsgCache.keys()) { _waMsgCache.delete(k); if (_waMsgCache.size <= 600) break; } }
}
// Contador de tentativas de redecodificação (formato de cache que o Baileys espera)
const _waRetryCounter = (() => { const m = new Map(); return {
  get: k => m.get(k), set: (k, v) => { m.set(k, v); return true; },
  del: k => m.delete(k), flushAll: () => m.clear()
}; })();
const _waPresence = {}; // 'instancia|jid' -> { state, lastSeen, at } (online/visto por último)
const _waPolls = {};    // wamid da enquete -> { options, encKey, creatorJid } (para decifrar votos)
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
  delete _waErr[inst]; delete qrCache[inst]; delete _waQrRetries[inst]; delete _waCreatedAt[inst]; delete _waRegistered[inst]; delete _waPairing[inst];
  try { if (supabase) await supabase.from('wa_sessions').delete().eq('instance', inst); } catch (_) {}
  console.log(`🧹 Instância não pareada encerrada: ${inst}`);
}

// Diagnóstico do motor embutido (para depurar sem acesso aos logs)
app.get('/wa/debug', async (req, res) => {
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
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
    // Reentrega quando o aparelho do lead pede "retry" (chaves novas pós-pareamento)
    getMessage: async (key) => _waMsgCache.get(String(key?.id || '')) || undefined,
    msgRetryCounterCache: _waRetryCounter,
  });
  _waSocks[instanceName] = sock;
  _waState[instanceName] = 'connecting';

  sock.ev.on('creds.update', () => {
    // Credenciais registradas = o QR FOI LIDO no celular; a conexão ainda vai
    // reiniciar até ficar "open" — este marcador faz o app avisar na hora
    if (state?.creds?.registered && !_waRegistered[instanceName] && _waState[instanceName] !== 'open') _waPairing[instanceName] = true;
    return saveCreds();
  });

  // Presença do contato (online / visto por último / digitando)
  sock.ev.on('presence.update', async (pu) => {
    try {
      const entries = Object.entries(pu.presences || {});
      if (!entries.length) return;
      const [, pr] = entries[0];
      const dado = {
        state: pr.lastKnownPresence || 'unavailable',
        lastSeen: pr.lastSeen ? pr.lastSeen * 1000 : null,
        at: Date.now()
      };
      _waPresence[instanceName + '|' + pu.id] = dado;
      // O WhatsApp costuma anexar o sufixo do aparelho (":0") ao número —
      // guarda TAMBÉM a versão limpa, senão a busca nunca encontra
      const idLimpo = String(pu.id).replace(/:\d+(?=@)/, '');
      if (idLimpo !== pu.id) _waPresence[instanceName + '|' + idLimpo] = dado;
      // WhatsApp novo pode identificar o contato por LID (código interno, não o
      // telefone) → traduz e guarda TAMBÉM sob o número real, senão o app nunca acha
      if (String(pu.id).endsWith('@lid')) {
        try {
          const map = sock.signalRepository && sock.signalRepository.lidMapping;
          let pn = (map && map.getPNForLID) ? map.getPNForLID(pu.id) : null;
          if (pn && pn.then) pn = await pn;
          if (pn) _waPresence[instanceName + '|' + pn] = dado;
        } catch (_) {}
      }
    } catch (_) {}
  });

  // Tiques de entrega/leitura das mensagens enviadas por QR (✓✓ e ✓✓ azul)
  sock.ev.on('messages.update', async (updates) => {
    if (!supabase) return;
    for (const u of updates || []) {
      const st = u.update?.status, id = u.key?.id;
      if (!st || !id) continue;
      // Só recibo do OUTRO lado sobre mensagem MINHA (key.fromMe). O "read-self"
      // (meu próprio celular abrindo a conversa) vem com fromMe=false — ignorado,
      // senão pintava de azul sem o lead ter lido.
      if (u.key && u.key.fromMe === false) continue;
      const mapped = st === 4 || st === 'READ' ? 'read' : (st === 3 || st === 'DELIVERY_ACK' ? 'delivered' : null);
      if (mapped) { try { await updateMsgStatus(id, { status: mapped }); } catch (_) {} }
    }
  });

  // Canal RESERVA de recibos (alguns aparelhos entregam entrega/leitura por aqui)
  sock.ev.on('message-receipt.update', async (events) => {
    if (!supabase) return;
    for (const ev of events || []) {
      const id = ev.key?.id; const rc = ev.receipt || {};
      if (!id) continue;
      if (ev.key && ev.key.fromMe === false) continue; // recibo de mensagem que NÃO é minha
      // Recibo do MEU próprio aparelho (outro dispositivo meu) não é leitura do lead
      try { const me = sock.user?.id ? String(sock.user.id).split(':')[0].split('@')[0] : ''; const uj = String(rc.userJid || '').split(':')[0].split('@')[0]; if (me && uj && me === uj) continue; } catch (_) {}
      const mapped = rc.readTimestamp ? 'read' : (rc.receiptTimestamp ? 'delivered' : null);
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
      delete _waPairing[instanceName];
      delete qrCache[instanceName];
      _waQrRetries[instanceName] = 0; // pareou — zera o contador de tentativas
      if (!_waRegistered[instanceName]) _waPairedAt[instanceName] = Date.now(); // 1ª conexão do processo
      _waRegistered[instanceName] = true;
      // Número QR voltou → libera um novo aviso caso desconecte de novo no futuro
      (async () => { try {
        const { data: aQ } = await supabase.from('accounts').select('owner').eq('evolution_instance', instanceName).maybeSingle();
        if (aQ) clearNoticeDisc(aQ.owner, 'disc:' + instanceName);
      } catch (_) {} })();
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
        // Avisa a dona: número QR desconectado de vez
        (async () => { try {
          const { data: a } = await supabase.from('accounts').select('name, owner').eq('evolution_instance', instanceName).maybeSingle();
          if (a) addNotice(a.owner, `🔌 O número QR "${a.name}" foi DESCONECTADO (sessão encerrada no celular). Use o botão Reconectar em Contas.`, 'disc:' + instanceName);
        } catch (_) {} })();
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
          // Janela PÓS-PAREAMENTO (10 min): o WhatsApp oscila bastante logo depois
          // de conectar — reconecta rápido para não atrasar mensagens nem recibos
          if (Date.now() - (_waPairedAt[instanceName] || 0) < 10 * 60000) waitMs = Math.min(waitMs, 8000);
          // Várias tentativas falharam numa conta pareada → avisa que ela caiu
          if (_waRegistered[instanceName] && waitMs >= 64000) {
            (async () => { try {
              const { data: a } = await supabase.from('accounts').select('name, owner').eq('evolution_instance', instanceName).maybeSingle();
              if (a) addNotice(a.owner, `🔌 O número QR "${a.name}" está DESCONECTADO (sem conseguir reconectar). Verifique o celular ou use Reconectar em Contas.`, 'disc:' + instanceName);
            } catch (_) {} })();
          }
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
      // Guarda para reentrega (o aparelho do lead pode pedir "retry" pós-pareamento)
      try { _waMsgCacheSet(m.key?.id, m.message); } catch (_) {}
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
          try {
            const { error: eEd2 } = await supabase.from('messages').update({ content: novoTxt, edited: true }).eq('wamid', _pm.key.id);
            if (eEd2) await supabase.from('messages').update({ content: novoTxt }).eq('wamid', _pm.key.id);
          } catch (_) {}
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
            // Lead reagiu → prévia da lista IGUAL ao WhatsApp: "Reagiu com ❤️ a: …"
            if (r.text && !m.key?.fromMe) {
              const { data: alvo } = await supabase.from('messages').select('content, phone, owner').eq('wamid', r.key.id).maybeSingle();
              if (alvo) {
                const trecho = String(alvo.content || 'sua mensagem').replace(/\s+/g, ' ').slice(0, 40);
                let q = supabase.from('contacts').update({
                  last_message_preview: `Reagiu com ${r.text} a: ${trecho}`,
                  last_message_at: new Date().toISOString(),
                  last_message_direction: 'inbound', last_message_status: null
                }).eq('phone', alvo.phone);
                if (alvo.owner) q = q.eq('owner', alvo.owner);
                await q;
              }
            }
          } catch (_) {}
        }
        continue;
      }
      // 🚫 Grupo, status e canal NÃO viram conversa no CRM (o webhook interno já
      // descartava) — mas a mídia deles era baixada e guardada ANTES do descarte.
      // Foi isso que encheu o Storage: 3 GB de vídeos de grupo/status que não
      // apareciam em lugar nenhum. Agora pulamos ANTES de baixar qualquer coisa.
      {
        const _rjPre = String(m.key?.remoteJid || '');
        if (_rjPre.includes('@g.us') || _rjPre.includes('@broadcast') || _rjPre.includes('@newsletter')) continue;
      }
      // Baixa a mídia (foto/áudio/vídeo/documento) e guarda no Supabase Storage
      let mediaPath = null, mediaMime = null;
      try {
        const mm = m.message.imageMessage || m.message.audioMessage || m.message.videoMessage
                || m.message.documentMessage || m.message.stickerMessage;
        // 📦 Arquivo acima do limite não entra no cofre (mesma regra da API oficial):
        // a mensagem aparece no chat, só sem o arquivo anexado.
        const _fl = mm && mm.fileLength;
        const _tamQr = !_fl ? 0 : (typeof _fl.toNumber === 'function' ? _fl.toNumber() : (Number(_fl) || 0));
        if (mm && _tamQr > COFRE_ARQ_MAX_MB * 1048576) {
          console.log(`📦 Mídia QR de ${Math.round(_tamQr / 1048576)} MB não guardada (limite ${COFRE_ARQ_MAX_MB} MB)`);
        } else if (mm && supabase && _cofreCheio) {
          console.log('📦 Cofre no limite — mídia QR não guardada');
        } else if (mm && supabase) {
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
// Envio de conteúdo especial pelo QR (localização, contato, enquete, revogação…)
async function waSendRaw(instanceName, to, payload) {
  const sock = _waSocks[instanceName];
  if (!sock || _waState[instanceName] !== 'open') throw new Error('WhatsApp desconectado — gere o QR novamente em Contas');
  const jid = await waResolveJid(sock, to);
  return await sock.sendMessage(jid, payload);
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
        // Nenhum número conectado agora: tenta de novo em 20s e, depois de 10
        // tentativas, volta a conferir a cada 6h (antes a varredura morria de vez)
        setTimeout(_avatarSweep, ++_sweepTries < 10 ? 20000 : 6 * 3600000);
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
  if (!req.owner) return res.status(401).json({ error: 'Faça login no CRM' });
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
    const _st = _waState[req.params.instance] || 'close';
    // "pairing" = QR já lido, conexão terminando de subir (o front mostra o aviso)
    return res.json({ state: _st !== 'open' && _waPairing[req.params.instance] ? 'pairing' : _st, phone: _waPhone[req.params.instance] || null });
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

// POST /accounts/:id/reconnect-qr — RECONECTA uma conta QR desconectada:
// gera um QR novo para a MESMA conta (mantém id, leads, bots e nome)
app.post('/accounts/:id/reconnect-qr', async (req, res) => {
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  if (!WA_EMBEDDED) return res.status(400).json({ error: 'Motor QR embutido não está ativo neste servidor.' });
  const { data: acc } = await supabase.from('accounts')
    .select('id, evolution_instance').eq('id', req.params.id).eq('owner', req.owner || ' ').maybeSingle();
  if (!acc || !acc.evolution_instance) return res.status(400).json({ error: 'Esta conta não é de QR Code.' });
  const inst = acc.evolution_instance;
  try {
    // Encerra o socket atual e limpa a sessão morta — força a emissão de um QR novo
    try { _waSocks[inst]?.end?.(undefined); } catch (_) {}
    delete _waSocks[inst]; delete qrCache[inst];
    _waQrRetries[inst] = 0; _waRegistered[inst] = false; _waState[inst] = 'connecting';
    _waCreatedAt[inst] = Date.now();
    await supabase.from('wa_sessions').delete().eq('instance', inst);
    await waStart(inst);
    let qr = null;
    for (let i = 0; i < 16 && !qr; i++) { await new Promise(r => setTimeout(r, 500)); qr = qrCache[inst] || null; }
    console.log(`🔄 Reconexão de QR iniciada para ${inst} — QR: ${qr ? 'SIM' : 'via polling'}`);
    res.json({ success: true, instance: inst, qr });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /evolution/save-account — salva conta Evolution no Supabase após conexão
app.post('/evolution/save-account', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  const { instance, phone } = req.body;
  if (!instance) return res.status(400).json({ error: 'instance obrigatório' });
  if (!supabase) return res.status(500).json({ error: 'Supabase não configurado' });
  // Se a conta já existe (reconexão), PRESERVA o nome personalizado — e ela
  // precisa ser SUA (senão qualquer um sobrescreveria a conta de outra pessoa)
  const { data: exist } = await supabase.from('accounts').select('name, owner').eq('phone_number_id', instance).maybeSingle();
  if (exist && exist.owner && String(exist.owner).toLowerCase() !== String(req.owner).toLowerCase())
    return res.status(403).json({ error: 'Este número pertence a outra conta.' });
  const name = exist?.name || (phone ? `WhatsApp ${phone}` : `WhatsApp QR (${instance})`);
  const { data, error } = await supabase.from('accounts')
    .upsert({ name, type: 'evolution', evolution_instance: instance, phone_display: phone || null, phone_number_id: instance, token: '', owner: req.owner || null }, { onConflict: 'phone_number_id' })
    .select().single();
  if (error) return res.status(500).json({ error: error.message });
  console.log('✅ Conta Evolution salva:', name);
  res.json({ success: true, data });
});

// DELETE /evolution/disconnect/:instance
app.delete('/evolution/disconnect/:instance', async (req, res) => {
  if (!_exigeLogin(req, res)) return;
  const inst = req.params.instance;
  // Só desconecta um número SEU
  try {
    const { data: dono } = await supabase.from('accounts').select('owner').eq('evolution_instance', inst).maybeSingle();
    if (dono && dono.owner && String(dono.owner).toLowerCase() !== String(req.owner).toLowerCase())
      return res.status(403).json({ error: 'Este número pertence a outra conta.' });
  } catch (_) {}
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
        await supabase.from('accounts').delete().eq('evolution_instance', inst).eq('owner', req.owner || ' ');
      }
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }
  try {
    await axios.delete(`${EVOLUTION_URL}/instance/delete/${inst}`, { headers: evoHdr(), timeout: 10000 });
    if (supabase) await supabase.from('accounts').delete().eq('evolution_instance', inst).eq('owner', req.owner || ' ');
    res.json({ success: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /evolution-webhook — recebe mensagens da Evolution API
app.post('/evolution-webhook', async (req, res) => {
  // 🔒 Só o motor embutido (chamada interna 127.0.0.1) ou uma Evolution externa com o
  // segredo EVOLUTION_WEBHOOK_SECRET (header x-webhook-secret). Antes era aberto: qualquer
  // um na internet podia "inventar" mensagens em qualquer conta.
  const ipRaw = String(req.socket?.remoteAddress || '');
  const loopback = /^(::1|127\.0\.0\.1|::ffff:127\.0\.0\.1)$/.test(ipRaw);
  const segredo = process.env.EVOLUTION_WEBHOOK_SECRET;
  const okSecret = segredo && String(req.headers['x-webhook-secret'] || req.query.secret || '') === segredo;
  if (!loopback && !okSecret) { console.warn('🔒 evolution-webhook recusado de', ipRaw); return res.sendStatus(401); }
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
      // Sinal INTERNO do WhatsApp (sem conteúdo de verdade)? Ignora — não vira bolha
      const _reais = Object.keys(msg).filter(k => k !== 'messageContextInfo' && k !== 'senderKeyDistributionMessage' && k !== 'deviceSentMessage');
      if (!_reais.length) return;
      if      (msg.conversation)          { content = msg.conversation; type = 'text'; }
      else if (msg.extendedTextMessage)   { content = msg.extendedTextMessage.text || ''; type = 'text'; }
      else if (msg.imageMessage)          { content = msg.imageMessage.caption || '[Imagem]'; type = 'image'; }
      else if (msg.audioMessage || msg.pttMessage) {
        const secsEv = (msg.audioMessage?.seconds || msg.pttMessage?.seconds) || 0;
        content = '🎤 Mensagem de voz' + (secsEv ? ` (${_fmtDur(secsEv)})` : '');
        type = 'audio';
        try { const wfB = msg.audioMessage?.waveform; if (wfB && wfB.length) data._wfJson = JSON.stringify(Array.from(wfB)); } catch (_) {}
      }
      else if (msg.videoMessage)          { content = msg.videoMessage.caption || '[Vídeo]'; type = 'video'; }
      else if (msg.documentMessage)       { content = `[Documento: ${msg.documentMessage.fileName || 'arquivo'}]`; type = 'document'; }
      else if (msg.stickerMessage)        { content = '[Figurinha]'; type = 'sticker'; }
      else if (msg.locationMessage)       { const l = msg.locationMessage; content = `📍 ${l.name || 'Localização'}\nhttps://maps.google.com/?q=${l.degreesLatitude},${l.degreesLongitude}`; type = 'location'; }
      else if (msg.contactMessage)        { content = `👤 ${msg.contactMessage.displayName || 'Contato'}`; type = 'contact'; }
      else if (msg.contactsArrayMessage)  { content = `👤 ${(msg.contactsArrayMessage.contacts || []).map(c => c.displayName).filter(Boolean).join(', ') || 'Contatos'}`; type = 'contact'; }
      else if (msg.pollCreationMessage || msg.pollCreationMessageV3) { const pl = msg.pollCreationMessage || msg.pollCreationMessageV3; content = `📊 ${pl.name}\n` + (pl.options || []).map(o => '▫️ ' + o.optionName).join('\n'); type = 'poll'; }
      else if (msg.pollUpdateMessage) {
        // VOTO na enquete: decifra e mostra a opção escolhida
        let escolha = '';
        try {
          const pu = msg.pollUpdateMessage;
          const pid = pu.pollCreationMessageKey?.id;
          const pinfo = _waPolls[pid];
          if (pinfo && pinfo.encKey && _baileys.decryptPollVote) {
            const voter = _baileys.jidNormalizedUser ? _baileys.jidNormalizedUser(data.key.remoteJid) : data.key.remoteJid;
            const dec = _baileys.decryptPollVote(pu.vote, { pollCreatorJid: pinfo.creatorJid, pollMsgId: pid, pollEncKey: pinfo.encKey, voterJid: voter });
            const _cr = require('crypto');
            const hashes = (dec.selectedOptions || []).map(b => Buffer.from(b).toString('hex'));
            escolha = pinfo.options.filter(o => hashes.includes(_cr.createHash('sha256').update(Buffer.from(o)).digest('hex'))).join(', ');
          }
        } catch (_) {}
        const pnome = (_waPolls[msg.pollUpdateMessage?.pollCreationMessageKey?.id] || {}).name || '';
        if (escolha) content = pnome ? `🗳 Votou na enquete "${pnome}": ${escolha}` : `🗳 Votou na enquete: ${escolha}`;
        else if (pnome) content = `🗳 Removeu o voto da enquete "${pnome}"`;
        else content = '🗳 Votou na enquete (veja a opção no celular)';
        type = 'text';
      }

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
          // Compara COM e SEM o nono dígito: o eco da mensagem enviada pelo CRM
          // volta às vezes no outro formato e escapava da checagem (duplicava).
          const _vars = _brPhoneVariants(String(phone).replace(/\D/g, ''));
          const { data: exists } = await supabase.from('messages').select('id')
            .eq('wamid', wamid).in('phone', _vars.length ? _vars : [phone]).limit(1).maybeSingle();
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
        if (!fromMe) await _somaNaoLida(phone, ownerEmail, timestamp); // soma em fila (vários arquivos juntos)

        const msgData = { phone, content, type, direction, timestamp, wamid };
        if (accountId) msgData.account_id = accountId;
        if (ownerEmail) msgData.owner = ownerEmail;
        if (data.mediaPath) { msgData.media_id = data.mediaPath; msgData.media_mime_type = data.mediaMime || null; }
        const { error: mErr } = await supabase.from('messages').insert(msgData);
        if (mErr) console.error('❌ Evolution: erro ao salvar mensagem:', mErr.message);
        // Extras opcionais (não quebram se as colunas não existirem no banco)
        try {
          if (data._wfJson && wamid) await supabase.from('messages').update({ waveform: data._wfJson }).eq('wamid', wamid).eq('phone', phone);
          const _ctxI = (Object.values(msg).find(v => v && v.contextInfo) || {}).contextInfo;
          if (_ctxI && (_ctxI.isForwarded || _ctxI.forwardingScore) && wamid)
            await supabase.from('messages').update({ forwarded: true }).eq('wamid', wamid).eq('phone', phone);
        } catch (_) {}

        // Notificação push só para mensagens RECEBIDAS (e não silenciadas 🔇)
        if (!fromMe && !(await _isContactMuted(phone, ownerEmail))) sendPushToOwner(ownerEmail, { title: name || phone, body: preview, phone, tag: 'chat-' + phone }).catch(() => {});

        // Bot e n8n só para mensagens RECEBIDAS
        if (!fromMe && type === 'text' && content) {
          try { await handleBotReply(phone, content, ownerEmail); } catch(be) { console.error('Bot reply error:', be.message); }
        }
        if (!fromMe) {
          const n8nUrl = _cfg('n8n_webhook_url', ownerEmail); // n8n da DONA desta conta
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
