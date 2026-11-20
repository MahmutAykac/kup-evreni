// Küp Evreni çok oyunculu altyapı (tüm oyunlar ortak kullanır)
// - Oda kodu + oyuncu listesi + bağlantı kurma: Supabase Realtime (presence + broadcast)
// - Oyun verisi: WebRTC DataChannel ile doğrudan cihazdan cihaza (kota yemez)
// - Doğrudan bağlantı kurulamazsa (bazı mobil ağlar): Supabase üzerinden aktarma, saniyede en fazla ~10 mesaj
// Yapı: odayı kuran oyuncu "sunucu"dur (host). Diğerleri sadece host'a bağlanır.
//
// Kullanım:
//   KupMP.lobby({ game: "kup-futbol", title: "Küp Futbol", name, min: 2, max: 6,
//                 options: [{ key: "sure", label: "Süre", choices: [["3","3 dk"],["5","5 dk"]] }],
//                 onStart(room, cfg) {...}, onClose() {...} });
//   Davet: platform oyunu ?davet=1234 ile açar → KupMP.davetVar() (bakar) · lobby() o koda kendiliğinden katılır (KupMP.davetKodu() bir kez verir)
//   room.isHost, room.id, room.players [{id,name,slot}], room.send(to, msg, reliable), room.broadcast(msg, reliable)
//   room.on("msg", (from, msg) => ...) · room.on("leave", id => ...) · room.on("hostLeft", () => ...) · room.leave()
(() => {
  "use strict";
  const SB_URL = "https://hpgfimktdcuoikgbbggk.supabase.co", SB_KEY = "sb_publishable_o212R-fxvBvUaX7RCTIVUg__NjphVnk";
  const SB_LIB = "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.57.4/dist/umd/supabase.min.js";
  const ICE = [{ urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] }, { urls: "stun:stun.cloudflare.com:3478" }];
  const RELAY_AFTER = 8000;   // bu kadar ms içinde doğrudan bağlantı olmazsa aktarmaya geç
  const RELAY_GAP = 100;      // aktarmada güvenilmez mesajlar en sık 100 ms'de bir (türüne göre birleştirilir)

  let client = null;
  function loadSB() {
    if (client) return Promise.resolve(client);
    return new Promise((res, rej) => {
      const make = () => { try { client = window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: false, autoRefreshToken: false, storageKey: "kupmp-auth" } }); res(client); } catch (e) { rej(e); } };
      if (window.supabase && window.supabase.createClient) return make();
      const s = document.createElement("script");
      s.src = SB_LIB; s.onload = make; s.onerror = () => rej(new Error("İnternet bağlantısı yok"));
      document.head.appendChild(s);
    });
  }
  const rid = () => Math.random().toString(36).slice(2, 10);
  const wait = ms => new Promise(r => setTimeout(r, ms));

  // ───────────────────────── Oda ─────────────────────────
  class Room {
    constructor(game, name) {
      this.game = game; this.name = (name || "Oyuncu").slice(0, 20); this.id = rid();
      this.isHost = false; this.code = ""; this.hostId = ""; this.players = []; this.started = false;
      this.peers = {};     // id → { pc, r, u, open, relay, timer, pend:{}, flushT }
      this.handlers = {}; this.present = []; this.options = {}; this.closed = false;
    }
    on(ev, fn) { (this.handlers[ev] = this.handlers[ev] || []).push(fn); return this; }
    off(ev, fn) { const h = this.handlers[ev]; if (h) this.handlers[ev] = h.filter(f => f !== fn); }
    emit(ev, ...a) { (this.handlers[ev] || []).slice().forEach(f => { try { f(...a); } catch (e) { console.error(e); } }); }

    async _subscribe(code) {
      const sb = await loadSB();
      this.code = code;
      this.ch = sb.channel(`kupmp-${this.game}-${code}`, { config: { presence: { key: this.id }, broadcast: { self: false, ack: false } } });
      this.ch.on("presence", { event: "sync" }, () => this._sync());
      this.ch.on("broadcast", { event: "sig" }, ({ payload: p }) => { if (p.to === this.id) this._signal(p); });
      this.ch.on("broadcast", { event: "rl" }, ({ payload: p }) => { if (p.to === this.id || p.to === "*") this._recv(p.from, p.d); });
      this.ch.on("broadcast", { event: "ctl" }, ({ payload: p }) => this._ctl(p));
      await new Promise((res, rej) => {
        const t = setTimeout(() => rej(new Error("Sunucuya bağlanılamadı")), 10000);
        this.ch.subscribe(st => {
          if (st === "SUBSCRIBED") { clearTimeout(t); res(); }
          else if (st === "CHANNEL_ERROR" || st === "TIMED_OUT") { clearTimeout(t); rej(new Error("Sunucuya bağlanılamadı")); }
        });
      });
    }
    _track() { return this.ch.track({ id: this.id, name: this.name, host: this.isHost, t: this.joinT, locked: this.started, opts: this.isHost ? this.options : undefined }); }
    _sync() {
      const st = this.ch.presenceState(); const list = [];
      for (const k in st) { const m = st[k][0]; if (m && m.id) list.push(m); }
      list.sort((a, b) => (b.host - a.host) || (a.t - b.t));
      const before = this.present.map(p => p.id);
      this.present = list;
      const host = list.find(p => p.host);
      if (!this.isHost) {
        if (host) {
          this.hostId = host.id;
          if (host.opts) this.options = host.opts;
          if (!this.peers[host.id] && this.joined) this._connect(host.id, true);
        } else if (this.joined && this.hostId) { this.hostId = ""; this.emit("hostLeft"); }
      }
      for (const id of before) if (!list.find(p => p.id === id)) {
        this._drop(id);
        if (this.started) { this.players = this.players.filter(p => p.id !== id); this.emit("leave", id); }
      }
      this.emit("lobby");
    }

    async create(opts) {
      this.isHost = true; this.joinT = Date.now(); this.options = opts || {};
      for (let i = 0; i < 6; i++) {
        const code = String(1000 + Math.floor(Math.random() * 9000));
        await this._subscribe(code);
        await wait(700);
        if (!Object.keys(this.ch.presenceState()).length) { this.hostId = this.id; await this._track(); this.joined = true; return code; }
        await this.ch.unsubscribe();
      }
      throw new Error("Oda kurulamadı, tekrar dene");
    }
    async join(code, max) {
      this.joinT = Date.now();
      await this._subscribe(code);
      let host = null;
      for (let i = 0; i < 20 && !host; i++) { await wait(250); this._sync(); host = this.present.find(p => p.host); }
      if (!host) { await this.ch.unsubscribe(); throw new Error("Oda bulunamadı. Kodu kontrol et."); }
      if (host.locked) { await this.ch.unsubscribe(); throw new Error("Bu odada maç başladı."); }
      if (max && this.present.length >= max) { await this.ch.unsubscribe(); throw new Error("Oda dolu."); }
      this.hostId = host.id; await this._track(); this.joined = true;
      this._connect(host.id, true);
    }
    setOptions(o) { this.options = Object.assign({}, this.options, o); if (this.isHost) this._track(); this.emit("lobby"); }

    // ── WebRTC ──
    _peer(id) {
      let p = this.peers[id];
      if (p) return p;
      p = this.peers[id] = { pc: new RTCPeerConnection({ iceServers: ICE }), r: null, u: null, open: false, relay: false, pend: {}, flushT: 0 };
      p.pc.onicecandidate = e => { if (e.candidate) this._sig(id, { c: e.candidate.toJSON() }); };
      p.pc.ondatachannel = e => this._chan(id, e.channel);
      p.timer = setTimeout(() => { if (!p.open) { p.relay = true; this.emit("lobby"); this.emit("peer", id); } }, RELAY_AFTER);
      p.pc.onconnectionstatechange = () => { if (p.pc.connectionState === "failed" && !p.relay) { p.relay = true; p.open = false; this.emit("lobby"); } };
      return p;
    }
    _chan(id, ch) {
      const p = this.peers[id]; if (!p) return;
      if (ch.label === "r") p.r = ch; else p.u = ch;
      ch.onopen = () => { if (p.r && p.u && p.r.readyState === "open" && p.u.readyState === "open") { p.open = true; p.relay = false; clearTimeout(p.timer); this.emit("lobby"); this.emit("peer", id); } };
      ch.onmessage = e => { let m; try { m = JSON.parse(e.data); } catch (x) { return; } this._recv(id, m); };
      ch.onclose = () => { if (p.open) { p.open = false; p.relay = true; this.emit("lobby"); } };
    }
    async _connect(id, initiator) {
      const p = this._peer(id);
      if (!initiator) return;
      this._chan(id, p.pc.createDataChannel("r", { ordered: true }));
      this._chan(id, p.pc.createDataChannel("u", { ordered: false, maxRetransmits: 0 }));
      const off = await p.pc.createOffer(); await p.pc.setLocalDescription(off);
      this._sig(id, { sdp: p.pc.localDescription.toJSON ? p.pc.localDescription.toJSON() : { type: off.type, sdp: off.sdp } });
    }
    _sig(to, d) { this.ch && this.ch.send({ type: "broadcast", event: "sig", payload: Object.assign({ to, from: this.id }, d) }); }
    async _signal(m) {
      const p = this._peer(m.from);
      try {
        if (m.sdp) {
          await p.pc.setRemoteDescription(m.sdp);
          if (p.qc) { for (const c of p.qc) await p.pc.addIceCandidate(c).catch(() => {}); p.qc = null; }
          if (m.sdp.type === "offer") { const a = await p.pc.createAnswer(); await p.pc.setLocalDescription(a); this._sig(m.from, { sdp: { type: a.type, sdp: a.sdp } }); }
        } else if (m.c) {
          if (p.pc.remoteDescription) await p.pc.addIceCandidate(m.c).catch(() => {});
          else (p.qc = p.qc || []).push(m.c);
        }
      } catch (e) { console.warn("kupmp sig", e); }
    }
    _drop(id) {
      const p = this.peers[id]; if (!p) return;
      clearTimeout(p.timer); clearTimeout(p.flushT);
      try { p.pc.close(); } catch (e) {}
      delete this.peers[id];
    }

    // ── Mesajlar ──
    _recv(from, m) {
      if (m && m.__ctl) return this._ctl(m.__ctl, from);
      this.emit("msg", from, m);
    }
    send(to, msg, reliable) {
      const p = this.peers[to]; if (!p) return;
      if (p.open) {
        const ch = reliable ? p.r : p.u;
        if (ch && ch.readyState === "open") { try { if (!reliable && ch.bufferedAmount > 64000) return; ch.send(JSON.stringify(msg)); } catch (e) {} }
        return;
      }
      if (!p.relay) { if (reliable) { (p.q = p.q || []).push(msg); this._retry(to); } return; }
      if (reliable) return this._relay(to, msg);
      p.pend[msg.t || "_"] = msg;                  // aynı türden en son mesaj gider
      if (!p.flushT) p.flushT = setTimeout(() => { p.flushT = 0; const all = p.pend; p.pend = {}; for (const k in all) this._relay(to, all[k]); }, RELAY_GAP);
    }
    _retry(to) {  // bağlantı açılınca bekleyen güvenilir mesajlar
      const p = this.peers[to]; if (!p || p.rt) return;
      p.rt = setInterval(() => {
        const q = this.peers[to]; if (!q) return;
        if (q.open || q.relay) { clearInterval(q.rt); q.rt = 0; const l = q.q || []; q.q = []; l.forEach(m => this.send(to, m, true)); }
      }, 200);
    }
    _relay(to, d) { this.ch && this.ch.send({ type: "broadcast", event: "rl", payload: { to, from: this.id, d } }); }
    broadcast(msg, reliable) { for (const id in this.peers) this.send(id, msg, reliable); }
    toHost(msg, reliable) { if (this.hostId) this.send(this.hostId, msg, reliable); }
    status(id) { const p = this.peers[id]; return !p ? "wait" : p.open ? "p2p" : p.relay ? "relay" : "wait"; }

    // ── Başlatma (host) ──
    _ctl(p, from) {
      if (p.k === "start" && !this.isHost && !this.started) {
        this.started = true; this.players = p.players; this.options = p.opts || this.options;
        this.emit("start", p.cfg || {});
      } else if (p.k === "kick" && p.id === this.id) { this.emit("hostLeft"); }
    }
    start(cfg) {
      if (!this.isHost) return;
      this.started = true;
      this.players = this.present.map((p, i) => ({ id: p.id, name: p.name, slot: i }));
      const msg = { k: "start", players: this.players, opts: this.options, cfg: cfg || {} };
      this.broadcast({ __ctl: msg }, true);
      this._track();
      this.emit("start", cfg || {});
    }
    me() { return this.players.find(p => p.id === this.id); }
    async leave() {
      if (this.closed) return; this.closed = true;
      for (const id in this.peers) this._drop(id);
      try { await this.ch.untrack(); await this.ch.unsubscribe(); } catch (e) {}
      this.handlers = {};
    }
  }

  // ───────────────────────── Lobi ekranı ─────────────────────────
  const CSS = `
#kupmp{position:fixed;inset:0;z-index:99999;background:rgba(10,14,30,.82);display:flex;align-items:center;justify-content:center;font-family:system-ui,-apple-system,"Segoe UI",sans-serif;color:#fff;padding:16px;box-sizing:border-box}
#kupmp *{box-sizing:border-box}
#kupmp .box{background:#1d2440;border:2px solid #3b4a80;border-radius:18px;width:min(420px,100%);max-height:100%;overflow:auto;padding:20px;box-shadow:0 12px 40px rgba(0,0,0,.5)}
#kupmp h2{margin:0 0 4px;font-size:22px}#kupmp .sub{opacity:.75;font-size:14px;margin-bottom:16px}
#kupmp button{font:inherit;font-weight:700;border:0;border-radius:12px;padding:12px 16px;cursor:pointer;color:#fff;background:#4a5ad8;width:100%;margin-top:8px;font-size:16px}
#kupmp button.g{background:#2fae5a}#kupmp button.x{background:#535a75}#kupmp button:disabled{opacity:.45;cursor:default}
#kupmp input,#kupmp select{font:inherit;font-size:18px;width:100%;padding:11px;border-radius:12px;border:2px solid #3b4a80;background:#121830;color:#fff;margin-top:8px}
#kupmp input{text-align:center;letter-spacing:8px;font-size:26px;font-weight:800}
#kupmp .code{font-size:46px;font-weight:900;letter-spacing:10px;text-align:center;background:#121830;border-radius:14px;padding:8px;margin:6px 0 4px;user-select:all}
#kupmp .pl{display:flex;align-items:center;gap:8px;background:#26305a;border-radius:10px;padding:9px 12px;margin-top:6px;font-size:15px}
#kupmp .pl .s{margin-left:auto;font-size:12px;opacity:.8}
#kupmp .err{color:#ff8a8a;font-size:14px;margin-top:10px;min-height:1em}
#kupmp .row{display:flex;gap:8px}#kupmp .row>*{flex:1}
#kupmp .opt{margin-top:10px;font-size:14px;opacity:.9}
#kupmp .or{text-align:center;opacity:.6;margin:14px 0 2px;font-size:13px}`;

  function lobby(o) {
    const game = o.game, min = o.min || 2, max = o.max || 8;
    let room = null, el = document.getElementById("kupmp");
    if (el) el.remove();
    if (!document.getElementById("kupmp-css")) { const s = document.createElement("style"); s.id = "kupmp-css"; s.textContent = CSS; document.head.appendChild(s); }
    el = document.createElement("div"); el.id = "kupmp"; document.body.appendChild(el);
    // Oyunun klavye/fare kontrolleri lobide çalışmasın
    ["keydown", "keyup", "mousedown", "pointerdown", "touchstart", "wheel", "click"].forEach(t => el.addEventListener(t, e => e.stopPropagation()));
    const esc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
    const close = async (leave) => { el.remove(); if (leave && room) await room.leave(); };
    const canInvite = window.parent !== window;   // platformun içindeyiz: arkadaş listesini platform açar

    function home(err) {
      el.innerHTML = `<div class="box"><h2>👥 Arkadaşlarla Oyna</h2><div class="sub">${esc(o.title || "")} · ${min === max ? max : min + "–" + max} oyuncu</div>
        <button class="g" id="mpNew">🏠 Oda Kur</button>
        <div class="or">ya da arkadaşının oda koduyla katıl</div>
        <input id="mpCode" inputmode="numeric" maxlength="4" placeholder="1234" autocomplete="off">
        <button id="mpJoin">🚪 Odaya Katıl</button>
        <button class="x" id="mpBack">Geri</button><div class="err">${esc(err || "")}</div></div>`;
      const inp = el.querySelector("#mpCode");
      inp.addEventListener("input", () => { inp.value = inp.value.replace(/\D/g, "").slice(0, 4); });
      inp.addEventListener("keydown", e => { if (e.key === "Enter") el.querySelector("#mpJoin").click(); });
      el.querySelector("#mpBack").onclick = () => { close(false); o.onClose && o.onClose(); };
      el.querySelector("#mpNew").onclick = async () => {
        busy("Oda kuruluyor…");
        room = new Room(game, o.name);
        const def = {}; (o.options || []).forEach(op => def[op.key] = op.choices[op.def || 0][0]);
        try { await room.create(def); inRoom(); } catch (e) { room = null; home(e.message); }
      };
      el.querySelector("#mpJoin").onclick = () => {
        const c = inp.value; if (c.length !== 4) return home("4 haneli kodu yaz");
        join(c);
      };
    }
    async function join(c) {
      busy("Odaya bağlanılıyor…");
      room = new Room(game, o.name);
      try { await room.join(c, max); inRoom(); } catch (e) { room = null; home(e.message); }
    }
    function busy(t) { el.innerHTML = `<div class="box"><h2>⏳ ${esc(t)}</h2></div>`; }
    function inRoom() {
      const draw = () => {
        if (room.started) return;
        const L = room.present;
        const st = p => p.id === room.id ? "sen" : !room.isHost && !p.host ? "" :
          ({ p2p: "🟢 bağlı", relay: "🟡 bağlı (yavaş ağ)", wait: "⏳ bağlanıyor" })[room.status(p.id)];
        const ok = room.isHost && L.length >= min && L.length <= max && L.every(p => p.id === room.id || room.status(p.id) !== "wait");
        const opts = (o.options || []).map(op => {
          const v = room.options[op.key];
          if (room.isHost) return `<div class="opt">${esc(op.label)}<select data-k="${op.key}">${op.choices.map(c => `<option value="${esc(c[0])}"${c[0] === v ? " selected" : ""}>${esc(c[1])}</option>`).join("")}</select></div>`;
          const c = op.choices.find(c => c[0] === v); return `<div class="opt">${esc(op.label)}: <b>${esc(c ? c[1] : "–")}</b></div>`;
        }).join("");
        const meOk = room.isHost || room.status(room.hostId) !== "wait";
        el.innerHTML = `<div class="box"><h2>🏠 Oda</h2><div class="sub">Bu kodu arkadaşına söyle, "Odaya Katıl"a yazsın</div>
          <div class="code">${room.code}</div>
          ${L.map(p => `<div class="pl">${p.host ? "👑" : "🙂"} ${esc(p.name)}<span class="s">${st(p)}</span></div>`).join("")}
          <div class="sub" style="margin:8px 0 0">${L.length}/${max} oyuncu${L.length < min ? ` · en az ${min} kişi gerekli` : ""}</div>
          ${opts}
          ${room.isHost ? `<button class="g" id="mpStart" ${ok ? "" : "disabled"}>▶ Maçı Başlat</button>` : `<div class="sub" style="margin-top:14px">${meOk ? "✅ Bağlandın. Oda sahibinin başlatması bekleniyor…" : "⏳ Oda sahibine bağlanılıyor…"}</div>`}
          ${canInvite ? `<button id="mpInvite">👥 Arkadaşını davet et</button><div class="sub" style="margin:4px 0 0;text-align:center;font-size:12px">Arkadaş listen açılacak</div>` : ""}
          <button class="x" id="mpLeave">Odadan Çık</button></div>`;
        const inv = el.querySelector("#mpInvite");
        if (inv) inv.onclick = () => { try { window.parent.postMessage({ type: "kup:invite", room: room.code }, "*"); } catch (e) {} };
        el.querySelectorAll("select").forEach(s => s.onchange = () => room.setOptions({ [s.dataset.k]: s.value }));
        el.querySelector("#mpLeave").onclick = async () => { await room.leave(); room = null; home(); };
        const b = el.querySelector("#mpStart");
        if (b) b.onclick = () => { room.start(o.makeCfg ? o.makeCfg(room) : {}); };
      };
      room.on("lobby", draw);
      room.on("hostLeft", () => { if (!room.started) { room.leave(); room = null; home("Oda sahibi çıktı."); } });
      room.on("start", cfg => { close(false); o.onStart && o.onStart(room, cfg); });
      draw();
    }
    const davet = davetKodu();   // davetle açıldıysa doğrudan o odaya katıl
    if (davet) join(davet); else home();
  }

  // Platform bir davetle açarsa oyunun adresinde ?davet=1234 olur. Bir kez kullanılır (menüye dönünce tekrar katılmasın).
  let davetKullanildi = false;
  function davetVar() {
    if (davetKullanildi) return false;
    try { return /^\d{4}$/.test(new URLSearchParams(location.search).get("davet") || ""); } catch (e) { return false; }
  }
  function davetKodu() {
    if (!davetVar()) return null;
    davetKullanildi = true;
    return new URLSearchParams(location.search).get("davet");
  }

  // Maç içinde: oyuncu ayrıldı / host gitti bildirimi için küçük yardımcı
  function toast(text, ms) {
    const d = document.createElement("div");
    d.textContent = text;
    d.style.cssText = "position:fixed;left:50%;top:14%;transform:translateX(-50%);background:rgba(20,26,50,.92);color:#fff;padding:10px 18px;border-radius:12px;font:700 15px system-ui,sans-serif;z-index:99998;pointer-events:none";
    document.body.appendChild(d); setTimeout(() => d.remove(), ms || 2500);
  }

  window.KupMP = { lobby, Room, toast, loadSB, davetVar, davetKodu };
})();
