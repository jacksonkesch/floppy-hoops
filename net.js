// Floppy Games online play.
// The Claude room lives in the page the artifact opens (index.html); a game running in its frame borrows it over a
// MessagePort. The room only carries signaling (presence): players then talk over a WebRTC data channel, or through
// presence itself if a direct connection can't be made. Matches run host-scheduled lockstep: every computer steps
// the same seeded sim with the same inputs, and the host decides which tick each input lands on and streams the
// confirmed ticks out. Guests stay a couple of ticks behind the host to ride out jitter.
(() => {
'use strict';
const PROTO = 1;
const ICE = [{ urls: 'stun:stun.l.google.com:19302' }];
const CODE_ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ';   // no I or O
const BUF = 2;                                 // ticks a guest keeps in hand against jitter
const TICK_MS = 1000 / 60 / 0.95;              // the games run at GAME_SPEED 0.95
const now = () => performance.now();
const sleep = ms => new Promise(r => setTimeout(r, ms));
const call = f => { try { if (typeof f === 'function') f(); } catch (e) { /* already gone */ } };

// ---------------------------------------------------------------- the room, served over a MessagePort
function servePort(port, roomP) {
  const rooms = new Map();
  let lobbyOff = null, dead = false;
  const send = m => { try { port.postMessage(m); } catch (e) { /* port closed */ } };
  const plain = p => ({ peer: p.peer, me: !!p.sameTab, agent: p.kind === 'agent', presence: p.presence || {} });
  roomP.then(room => {
    if (dead) return;
    send({ op: 'hello', ok: !!room });
    if (!room) return;
    const count = ps => send({ op: 'count', n: ps.filter(p => p.kind !== 'agent').length });
    try { count(room.peers()); } catch (e) { /* not connected yet */ }
    lobbyOff = room.onPeers(ch => count(ch.peers), () => {});
  });
  function drop(name) {
    const r = rooms.get(name); if (!r) return;
    rooms.delete(name); r.offs.forEach(call);
    try { const p = r.room.leave(); if (p && p.catch) p.catch(() => {}); } catch (e) { /* already left */ }
  }
  port.onmessage = async e => {
    const m = e.data || {};
    if (m.op === 'bye') { close(); return; }
    const room = await roomP;
    if (dead) return;
    if (m.op === 'join') {
      if (!room) { send({ op: 'res', id: m.id, ok: false, code: 'unavailable' }); return; }
      try {
        drop(m.name);
        const r = await room.join(m.name), rec = { room: r, offs: [] };
        rooms.set(m.name, rec);
        rec.offs.push(r.onPeers(ch => send({ op: 'peers', name: m.name, peers: ch.peers.map(plain) }),
          err => send({ op: 'rerr', name: m.name, code: err && err.code })));
        send({ op: 'res', id: m.id, ok: true });
      } catch (err) { send({ op: 'res', id: m.id, ok: false, code: err && err.code }); }
    } else if (m.op === 'presence') {
      const r = rooms.get(m.name);
      if (r) r.room.presence(m.patch).catch(err => send({ op: 'perr', name: m.name, code: err && err.code }));
    } else if (m.op === 'leave') drop(m.name);
  };
  function close() { if (dead) return; dead = true; call(lobbyOff); for (const n of [...rooms.keys()]) drop(n); try { port.close(); } catch (e) { /* fine */ } }
  return close;
}

// the artifact's room, asked for once per page
let roomP = null;
const room = () => roomP || (roomP = (window.claude && claude.use ? Promise.resolve().then(() => claude.use('room')) : Promise.resolve(null)).catch(() => null));
// is this page a game running inside the picker that holds the room?
const framedByPicker = () => { try { return parent !== window && !!(parent.FloppyNet && parent.FloppyNet.serving); } catch (e) { return false; } };
// back to the game picker: the picker closes our frame, or (on our own) we go to it
function home() { if (framedByPicker()) parent.postMessage({ fnet: 'home' }, '*'); else location.href = 'index.html'; }

// index.html: hand the room to whichever game frame asks for it
function serveFrames(roomP) {
  window.FloppyNet.serving = true;
  let stop = null;
  addEventListener('message', e => {
    if (!e.data || e.data.fnet !== 'port' || !e.ports || !e.ports[0]) return;
    if (![...document.querySelectorAll('iframe')].some(f => f.contentWindow === e.source)) return;
    call(stop); stop = servePort(e.ports[0], roomP);
  });
  return { release() { call(stop); stop = null; } };
}

// a game page: talk to the room, wherever it lives
class Svc {
  constructor() {
    this.ok = null; this.count = 0; this.onCount = null;
    this.id = 0; this.waits = new Map(); this.handlers = new Map();
    this.ready = new Promise(r => { this.readyFn = r; });
    // Where the room is depends on how we were opened, which a page can't always tell, so every likely source
    // gets a port and the first to say hello with a room wins: the picker framing us, our own Claude runtime,
    // or (framed by something we can't look into) our parent.
    this.port = null; this.tries = 0; this.fails = 0;
    const own = window.claude && claude.use;
    if (framedByPicker()) this.offer(p => parent.postMessage({ fnet: 'port' }, '*', [p]));
    else {
      if (own) this.offer(p => servePort(p, room()));
      if (parent !== window) this.offer(p => parent.postMessage({ fnet: 'port' }, '*', [p]));
      if (!own && parent === window) this.offer(p => servePort(p, Promise.resolve(null)));
    }
    setTimeout(() => this.hello(false), 12000);
    addEventListener('pagehide', () => { try { if (this.port) this.port.postMessage({ op: 'bye' }); } catch (e) { /* fine */ } });
  }
  offer(open) {
    const ch = new MessageChannel(), port = ch.port2;
    this.tries++;
    port.onmessage = e => {
      const m = e.data || {};
      if (this.port) { if (port === this.port) this.msg(m); return; }   // only the winner gets heard
      if (m.op !== 'hello') return;
      if (!m.ok) { if (++this.fails >= this.tries) this.hello(false); return; }
      this.port = port; this.hello(true);
    };
    try { open(ch.port1); } catch (e) { this.fails++; }
  }
  hello(ok) { if (this.ok === null) { this.ok = ok; this.readyFn(ok); } }
  msg(m) {
    if (m.op === 'count') { this.count = m.n; if (this.onCount) this.onCount(m.n); }
    else if (m.op === 'res') { const w = this.waits.get(m.id); if (w) { this.waits.delete(m.id); w(m); } }
    else { const h = this.handlers.get(m.name); if (h) h(m); }
  }
  post(m) { if (this.port) this.port.postMessage(m); }
  join(name, handler) {
    const id = ++this.id;
    this.handlers.set(name, handler);
    if (!this.port) return Promise.resolve({ ok: false, code: 'unavailable' });
    this.post({ op: 'join', id, name });
    return new Promise(res => { this.waits.set(id, res); setTimeout(() => { if (this.waits.delete(id)) res({ ok: false, code: 'timeout' }); }, 15000); });
  }
  presence(name, patch) { this.post({ op: 'presence', name, patch }); }
  leave(name) { this.handlers.delete(name); this.post({ op: 'leave', name }); }
}
let svc = null;
const connect = () => svc || (svc = new Svc());

// ---------------------------------------------------------------- WebRTC bits
function gathered(pc) {
  return new Promise(res => {
    let first = 0;
    const done = () => res();
    if (pc.iceGatheringState === 'complete') return done();
    pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') done(); });
    pc.addEventListener('icecandidate', e => { if (e.candidate && !first) { first = 1; setTimeout(done, 1200); } });
    setTimeout(done, 3000);
  });
}
const trimSdp = sdp => sdp.split('\r\n').filter(l => !/^a=candidate:\S+ \d+ tcp /i.test(l)).join('\r\n');
function randomCode() {
  const a = new Uint32Array(4); crypto.getRandomValues(a);
  return [...a].map(v => CODE_ABC[v % CODE_ABC.length]).join('');
}

// ---------------------------------------------------------------- a room session
// Host seat is 0; seats alternate red/blue, so seat i plays team i % 2, slot i >> 1.
// Host -> guest packet: { t: confirmed through tick, e: [[tick, seat, down] | [tick, -1, command]], a: last input
// seq heard, h: [tick, hash], r: ping echo }. Guest -> host: { i: [[seq, down]] not yet acked, a: ticks held,
// q: ping, rt: rtt, x: tick it went out of sync at }.
class Session {
  constructor(service, o) {
    this.svc = service; this.game = o.game; this.isHost = !!o.host; this.size = o.size || 1;
    this.onChange = o.onChange || (() => {}); this.onEnd = o.onEnd || (() => {}); this.onDesync = o.onDesync || null;
    this.code = String(o.code || '').toUpperCase().replace(/[^A-Z]/g, '');
    this.phase = 'joining';                 // joining -> lobby -> live -> closed
    this.error = ''; this.me = null; this.peerList = []; this.hostPeer = null; this.info = null;
    this.seats = []; this.mySeat = this.isHost ? 0 : -1;
    this.links = new Map(); this.link = null; this.busy = false;
    this.tick = 0; this.acc = 0; this.log = []; this.ownT = 0; this.lastHash = null;
    this.have = 0; this.evs = new Map(); this.pend = []; this.seq = 0; this.n = 0; this.starve = 0;
    this.rtt = 0; this.lastQ = 0; this.hashes = new Map(); this.hostHash = null; this.bad = 0; this.syncT = 0;
    this.pres = {}; this.infoJson = ''; this.relayOut = this.isHost ? {} : null; this.relayT = 0; this.relayTimer = 0; this.rc = 0;
  }
  get name() { return this.game + '-' + this.code.toLowerCase(); }
  get live() { return this.phase === 'live'; }

  async start() {
    if (!(await this.svc.ready)) return this.end('ONLINE PLAY ONLY WORKS INSIDE THE CLAUDE ARTIFACT');
    for (let tries = 0; ; tries++) {
      if (this.isHost) this.code = randomCode();
      else if (this.code.length !== 4) return this.end('ROOM CODES ARE 4 LETTERS');
      this.peerList = [];
      const res = await this.svc.join(this.name, m => this.roomMsg(m));
      if (this.phase === 'closed') return;
      if (!res.ok) return this.end(res.code === 'not_permitted' ? 'THIS PAGE CANNOT OPEN ROOMS' : 'COULD NOT REACH THE ROOM, TRY AGAIN');
      if (!this.isHost) break;
      await sleep(1200);                    // people already in the room show up over the first moment
      if (this.phase === 'closed') return;
      if (!this.peerList.some(p => !p.me && p.presence.h) || tries >= 3) break;
      this.svc.leave(this.name);            // that code is taken; roll another
    }
    if (this.isHost) {
      for (let k = 0; k < 40 && !this.me && this.phase !== 'closed'; k++) await sleep(100);
      if (this.phase === 'closed') return;
      if (!this.me) return this.end('COULD NOT REACH THE ROOM, TRY AGAIN');
      this.seats = [this.me]; this.phase = 'lobby';
      this.pubInfo();
      this.infoTimer = setInterval(() => this.pubInfo(), 1000);
      this.hostPeers(this.peerList);
    } else {
      this.pub({ j: PROTO });
      setTimeout(() => { if (!this.hostPeer && this.phase === 'joining') this.end('NO ROOM WITH THAT CODE'); }, 6000);
    }
    this.onChange();
  }

  roomMsg(m) {
    if (this.phase === 'closed') return;
    if (m.op === 'peers') {
      this.peerList = m.peers;
      const mine = m.peers.find(p => p.me); if (mine) this.me = mine.peer;
      if (this.phase === 'joining' && this.isHost) return;
      if (this.isHost) this.hostPeers(m.peers); else this.guestPeers(m.peers);
    } else if (m.op === 'rerr') this.end('LOST THE ROOM');
    else if (m.op === 'perr') console.warn('[net] presence rejected:', m.code);
  }
  pub(patch) { Object.assign(this.pres, patch); this.svc.presence(this.name, patch); }
  newLink(peer) { return { peer, pc: null, dc: null, open: false, relay: false, answered: 0, offer: null, seat: -1, ack: 0, seq: 0, rtt: 0, heard: now(), rc: 0, gone: false }; }
  linkUp(l) { return !l ? 0 : l.open ? 2 : l.relay ? 1 : 0; }
  closePc(l) { if (l.pc) { try { l.pc.close(); } catch (e) { /* fine */ } } l.pc = null; l.dc = null; l.open = false; }

  // ------------------------------------------------ host side
  hostInfo() {
    return { g: this.game, v: PROTO, size: this.size, st: this.live ? 'play' : 'lobby', seats: this.seats.slice(),
      up: this.seats.map((p, i) => i === 0 ? 2 : this.linkUp(this.links.get(p))),
      ms: this.seats.map(p => { const l = this.links.get(p); return l ? Math.round(l.rtt) : 0; }) };
  }
  pubInfo() {
    if (this.phase === 'closed') return;
    const h = this.hostInfo(), j = JSON.stringify(h);
    if (j !== this.infoJson) { this.infoJson = j; this.pub({ h }); this.onChange(); }
  }
  hostPeers(list) {
    const others = list.filter(p => !p.me && !p.agent), here = new Set(others.map(p => p.peer));
    let changed = false;
    for (const peer of this.seats.slice(1)) {
      if (here.has(peer)) continue;
      const l = this.links.get(peer);
      if (!this.live) { this.seats.splice(this.seats.indexOf(peer), 1); if (l) this.closePc(l); this.links.delete(peer); changed = true; }
      else if (l && !l.gone) { l.gone = true; this.closePc(l); delete this.relayOut[peer]; this.command({ c: 'gone', s: l.seat }); changed = true; }
    }
    for (const p of others) {
      const pr = p.presence || {};
      if (pr.j === PROTO && !this.live && !this.seats.includes(p.peer) && this.seats.length < this.size * 2) {
        this.seats.push(p.peer); this.links.set(p.peer, this.newLink(p.peer)); changed = true;
      }
      const l = this.links.get(p.peer); if (!l || l.gone) continue;
      if (pr.o && pr.o.to === this.me && pr.o.n > l.answered) l.offer = pr.o;
      if (pr.r && pr.r.c > l.rc) { l.rc = pr.r.c; if (!l.open) { if (!l.relay) changed = true; l.relay = true; } this.recv(l, pr.r); }
    }
    if (changed) this.pubInfo();
    this.pump();
    this.onChange();
  }
  pump() {
    if (this.busy || this.phase === 'closed') return;
    const l = [...this.links.values()].find(k => !k.gone && k.offer && k.offer.n > k.answered);
    if (l) this.answer(l);
  }
  async answer(l) {
    this.busy = true;
    const o = l.offer; l.answered = o.n;
    try {
      this.closePc(l);
      const pc = l.pc = new RTCPeerConnection({ iceServers: ICE });
      pc.ondatachannel = e => this.wire(l, e.channel);
      await pc.setRemoteDescription({ type: 'offer', sdp: o.sdp });
      await pc.setLocalDescription(await pc.createAnswer());
      await gathered(pc);
      if (this.phase === 'closed' || l.pc !== pc) return;
      this.pub({ ans: { to: l.peer, n: o.n, sdp: trimSdp(pc.localDescription.sdp) } });
      for (let k = 0; k < 60 && !l.open && l.pc === pc && this.phase !== 'closed'; k++) await sleep(100);
    } catch (e) { console.warn('[net] answer failed', e); }
    finally { this.busy = false; if (this.phase !== 'closed') { this.pub({ ans: null }); this.pump(); } }
  }
  canStart() { return this.isHost && this.phase === 'lobby' && this.seats.length >= 2 && this.seats.slice(1).every(p => this.linkUp(this.links.get(p))); }
  begin(cmd) {
    if (!this.canStart()) return false;
    this.phase = 'live'; this.tick = 0; this.acc = 0; this.ownT = 0; this.log = [[1, -1, cmd]];
    this.seats.forEach((p, i) => { const l = this.links.get(p); if (l) { l.seat = i; l.ack = 0; l.heard = now(); } });
    this.pubInfo(); this.onChange();
    return true;
  }
  command(c) { if (this.isHost && this.live) this.log.push([this.tick + 1, -1, c]); return this.tick + 1; }
  delay() {   // hold the host's own presses back about as long as a guest's take to reach it
    let m = 0;
    for (const l of this.links.values()) if (!l.gone && this.linkUp(l)) m = Math.max(m, l.rtt);
    return Math.max(0, Math.min(6, Math.round(m / 2 / TICK_MS)));
  }

  // ------------------------------------------------ guest side
  guestPeers(list) {
    const host = list.find(p => !p.me && p.presence && p.presence.h && p.presence.h.g === this.game && (!this.hostPeer || p.peer === this.hostPeer));
    if (!host) { if (this.hostPeer) this.end('THE HOST LEFT'); return; }
    this.hostPeer = host.peer;
    const h = this.info = host.presence.h;
    if (h.v !== PROTO) return this.end('THE HOST HAS A DIFFERENT VERSION, RELOAD BOTH PAGES');
    this.size = h.size; this.seats = h.seats.slice();
    const seat = this.seats.indexOf(this.me);
    if (seat < 0) {
      if (this.mySeat >= 0 || h.st === 'play') return this.end(this.mySeat >= 0 ? 'DROPPED FROM THE ROOM' : 'THAT MATCH ALREADY STARTED');
      if (this.seats.length >= h.size * 2) {
        if (!this.fullT) this.fullT = now();
        else if (now() - this.fullT > 3000) return this.end('THAT ROOM IS FULL');
      }
    } else this.fullT = 0;
    this.mySeat = seat;
    if (seat >= 0 && this.phase === 'joining') this.phase = 'lobby';
    if (seat >= 0 && !this.link) this.connect();
    const a = host.presence.ans, l = this.link;
    if (l && a && a.to === this.me && a.n === this.n && l.answered !== a.n && l.pc) {
      l.answered = a.n;
      l.pc.setRemoteDescription({ type: 'answer', sdp: a.sdp }).catch(e => console.warn('[net] answer rejected', e));
    }
    const r = host.presence.r && host.presence.r[this.me];
    if (l && r && r.c > l.rc) { l.rc = r.c; this.recv(l, r); }
    this.onChange();
  }
  async connect() {
    const l = this.link = this.link || this.newLink(this.hostPeer);
    const n = ++this.n;
    this.closePc(l);
    const pc = l.pc = new RTCPeerConnection({ iceServers: ICE });
    this.wire(l, pc.createDataChannel('g', { ordered: false, maxRetransmits: 0 }));
    try {
      await pc.setLocalDescription(await pc.createOffer());
      await gathered(pc);
      if (l.pc !== pc || this.phase === 'closed') return;
      this.pub({ o: { to: this.hostPeer, n, sdp: trimSdp(pc.localDescription.sdp) } });
    } catch (e) { console.warn('[net] offer failed', e); }
    setTimeout(() => { if (this.phase !== 'closed' && !l.open && !l.relay) { l.relay = true; this.flush(); this.onChange(); } }, 4000);
    setTimeout(() => { if (this.phase !== 'closed' && !l.open && l.pc === pc && this.n < 4) this.connect(); }, 9000);
  }
  hash(t, h) {
    if (this.isHost) this.lastHash = [t, h];
    else { this.hashes.set(t, h); this.check(); }
  }
  check() {
    const h = this.hostHash; if (!h || h[0] <= this.syncT) return;
    const mine = this.hashes.get(h[0]); if (mine === undefined) return;
    for (const k of [...this.hashes.keys()]) if (k <= h[0]) this.hashes.delete(k);
    if (mine !== h[1] && !this.bad) { this.bad = h[0]; console.warn('[net] out of sync at tick', h[0]); }
  }
  synced(t) { this.bad = 0; this.syncT = t; this.hashes.clear(); }

  // ------------------------------------------------ both
  wire(l, dc) {
    l.dc = dc;
    dc.onopen = () => {
      if (l.dc !== dc) return;
      l.open = true; l.heard = now();
      if (this.isHost) { delete this.relayOut[l.peer]; this.pubInfo(); } else this.pub({ r: null });
      l.relay = false; this.onChange();
    };
    dc.onclose = () => { if (l.dc === dc) { l.open = false; if (!l.gone) l.relay = true; this.onChange(); } };
    dc.onmessage = e => { let pk; try { pk = JSON.parse(e.data); } catch (err) { return; } this.recv(l, pk); };
  }
  sendTo(l, pk) {
    if (l.open && l.dc && l.dc.readyState === 'open') { if (l.dc.bufferedAmount < 65536) l.dc.send(JSON.stringify(pk)); return; }
    if (!l.relay) return;
    pk.c = ++this.rc;
    if (this.isHost) this.relayOut[l.peer] = pk; else this.relayOut = pk;
    if (this.relayTimer) return;
    this.relayTimer = setTimeout(() => { this.relayTimer = 0; this.relayT = now(); if (this.phase !== 'closed') this.pub({ r: this.relayOut }); },
      Math.max(0, 34 - (now() - this.relayT)));
  }
  recv(l, pk) {
    if (this.phase === 'closed') return;
    l.heard = now();
    if (this.isHost) {
      if (pk.q) this.sendTo(l, { r: pk.q });
      if (pk.rt) l.rtt = pk.rt;
      if (!this.live || l.gone) return;
      if (pk.i) for (const [s, v] of pk.i) if (s > l.seq) { l.seq = s; this.log.push([this.tick + 1, l.seat, v]); }
      if (pk.a > l.ack) l.ack = Math.min(pk.a, this.tick);
      if (pk.x && this.onDesync) this.onDesync(pk.x);
    } else {
      if (pk.r) { const d = now() - pk.r; this.rtt = this.rtt ? this.rtt * 0.75 + d * 0.25 : d; }
      if (pk.t > this.have && this.mySeat >= 0) {
        for (let k = this.have + 1; k <= pk.t; k++) this.evs.set(k, []);
        for (const e of pk.e || []) if (e[0] > this.have && e[0] <= pk.t) this.evs.get(e[0]).push(e);
        this.have = pk.t;
        if (this.phase === 'lobby') { this.phase = 'live'; this.onChange(); }
      }
      if (pk.a >= 0) this.pend = this.pend.filter(x => x[0] > pk.a);
      if (pk.h && (!this.hostHash || pk.h[0] > this.hostHash[0])) { this.hostHash = pk.h; this.check(); }
    }
  }
  input(down) {
    if (!this.live) return;
    const v = down ? 1 : 0;
    if (this.isHost) { this.ownT = Math.max(this.ownT, this.tick + 1 + this.delay()); this.log.push([this.ownT, 0, v]); }
    else { this.pend.push([++this.seq, v]); this.flush(); }
  }
  flush() {
    if (this.phase === 'closed') return;
    if (this.isHost) {
      let min = this.tick;
      for (const l of this.links.values()) {
        if (l.gone || l.seat < 0) continue;
        min = Math.min(min, l.ack);
        if (!this.linkUp(l)) continue;
        const pk = { t: this.tick, e: this.log.filter(e => e[0] > l.ack && e[0] <= this.tick), a: l.seq };
        if (this.lastHash) pk.h = this.lastHash;
        this.sendTo(l, pk);
      }
      if (this.log.some(e => e[0] <= min)) this.log = this.log.filter(e => e[0] > min);
    } else if (this.link) {
      const pk = { i: this.pend, a: this.have };
      if (now() - this.lastQ > 500) { this.lastQ = now(); pk.q = this.lastQ; if (this.rtt) pk.rt = Math.round(this.rtt); }
      if (this.bad) pk.x = this.bad;
      this.sendTo(this.link, pk);
    }
  }
  // the game loop calls this every frame instead of stepping itself: want = ticks the frame's time is worth,
  // run(events, tick) steps the sim one tick
  frame(want, run) {
    if (!this.live) return 0;
    let n = 0;
    if (this.isHost) {
      this.acc = Math.min(this.acc + want, 5);
      while (this.acc >= 1 && n < 5) { this.acc--; n++; const t = ++this.tick; run(this.log.filter(e => e[0] === t), t); }
      if (n) this.flush();
    } else {
      const avail = this.have - this.tick;
      this.acc += want;
      n = Math.floor(this.acc);
      if (avail - n > BUF + 1) n++;                       // drifting behind: catch up a tick at a time
      if (avail - n > BUF + 10) n = avail - BUF;          // way behind (a hitch): jump most of the way
      n = Math.max(0, Math.min(n, avail, 12));
      this.acc = Math.max(0, Math.min(1.5, this.acc - n));
      for (let i = 0; i < n; i++) { const t = ++this.tick, ev = this.evs.get(t) || []; this.evs.delete(t); run(ev, t); }
      this.starve = avail === 0 ? this.starve + 1 : 0;
      this.flush();
      if (this.link && now() - this.link.heard > 6000) this.end('LOST THE CONNECTION TO THE HOST');
    }
    return n;
  }
  view() {
    const h = this.isHost ? this.hostInfo() : this.info;
    const seats = h ? h.seats.map((p, i) => ({ me: p === this.me, host: i === 0, up: h.up[i], ms: h.ms[i] })) : [];
    return { phase: this.phase, code: this.code, size: h ? h.size : this.size, isHost: this.isHost, seats, canStart: this.canStart(), mySeat: this.mySeat, rtt: Math.round(this.rtt) };
  }
  ping() {
    if (!this.isHost) return Math.round(this.rtt);
    let m = 0; for (const l of this.links.values()) if (!l.gone) m = Math.max(m, l.rtt);
    return Math.round(m);
  }
  end(msg) {
    if (this.phase === 'closed') return;
    this.phase = 'closed'; this.error = msg || '';
    for (const l of this.links.values()) this.closePc(l);
    if (this.link) this.closePc(this.link);
    clearTimeout(this.relayTimer); clearInterval(this.infoTimer);
    this.svc.leave(this.name);
    this.onEnd(this.error);
  }
}

window.FloppyNet = { PROTO, room, home, serveFrames, connect, Session, serving: false };
})();
