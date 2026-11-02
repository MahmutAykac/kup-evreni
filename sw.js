// Küp Evreni çevrimdışı desteği (service worker)
// Sayfalar: önce internet (güncel sürüm gelsin), yoksa cihazdaki kopya.
// Sürüm numaralı kütüphaneler (three.js, supabase-js): önce cihazdaki kopya (değişmezler).
// Supabase istekleri (hesap, Robux) hiç saklanmaz: onlar her zaman sunucuya gider.
const CACHE = "kup-evreni-v1";
const GAMES = ["pizza-kulesi-tycoon", "evcil-hayvan-gezegenler", "cilgin-fabrika", "tekne-kazasi", "asmr-klavye", "kasaba-hayati", "limonata-standi"];
const PAGES = ["./", "./index.html", "./manifest.json", "./icon.svg", ...GAMES.map(g => `./${g}/web/index.html`)];
const LIBS = [
  "https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js",
  "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.57.4/dist/umd/supabase.min.js",
];

self.addEventListener("install", e => {
  e.waitUntil((async () => {
    const c = await caches.open(CACHE);
    // Biri inmezse diğerleri yine saklansın
    await Promise.all([...PAGES, ...LIBS].map(u => c.add(new Request(u, { cache: "reload" })).catch(() => {})));
    self.skipWaiting();
  })());
});
self.addEventListener("activate", e => {
  e.waitUntil((async () => {
    for (const k of await caches.keys()) if (k !== CACHE) await caches.delete(k);
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", e => {
  const req = e.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (LIBS.includes(url.href)) { e.respondWith(cacheFirst(req)); return; }
  if (url.origin !== location.origin) return;           // Supabase ve diğerleri: dokunma
  if (url.pathname.includes("/admin")) return;           // admin paneli her zaman internetten
  e.respondWith(networkFirst(req));
});

async function cacheFirst(req) {
  const hit = await caches.match(req);
  if (hit) return hit;
  const res = await fetch(req);
  if (res.ok) (await caches.open(CACHE)).put(req, res.clone());
  return res;
}
async function networkFirst(req) {
  const c = await caches.open(CACHE);
  const key = new URL(req.url); key.search = ""; // oyunlar ?v=... ile açılıyor: tek kopya tut
  try {
    const res = await fetch(req.url, { cache: "no-store" }); // sayfa isteğine (navigate) ayar eklenemez, adresle yeniden iste
    if (res.ok) c.put(key.href, res.clone());
    return res;
  } catch (err) {
    const hit = await c.match(key.href) || (req.mode === "navigate" ? await c.match(new URL("./index.html", self.registration.scope).href) : null);
    if (hit) return hit;
    throw err;
  }
}
