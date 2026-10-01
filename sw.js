// Offline support: after the first visit, the picker and all five games load from the Chromebook's cache, so they
// start with no internet. Pages are fetched fresh when online (so uploads show up), with the cache as backup;
// the big unchanging files (sounds, physics) come straight from the cache. build.sh stamps VERSION with a
// hash of the site files, so every upload replaces the old cache.
const VERSION = '29c1e07e7a';
const CACHE = 'floppy-' + VERSION, FONTS = 'floppy-fonts';
const CORE = ['./', 'index.html', 'hoops.html', 'pong.html', 'soccer.html', 'volley.html', 'boxing.html', 'clips.js', 'planck.min.js', 'manifest.webmanifest', 'icon-192.png', 'icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(CORE)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k.startsWith('floppy-') && k !== CACHE && k !== FONTS).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});

const fromCache = req => caches.match(req, { ignoreSearch: true });

self.addEventListener('fetch', e => {
  const req = e.request, url = new URL(req.url);
  if (req.method !== 'GET') return;
  // the pixel font: keep a copy so it still looks right offline
  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    e.respondWith(caches.open(FONTS).then(c => c.match(req).then(hit => hit || fetch(req).then(res => { if (res.ok || res.type === 'opaque') c.put(req, res.clone()); return res; }))));
    return;
  }
  if (url.origin !== location.origin) return;
  if (req.mode === 'navigate' || url.pathname.endsWith('.html') || url.pathname.endsWith('/')) {
    // pages: fresh from the network when it answers within 3 s, otherwise the saved copy
    e.respondWith(new Promise(resolve => {
      let done = false;
      const fallback = () => fromCache(req).then(hit => { if (!done && hit) { done = true; resolve(hit); } });
      const timer = setTimeout(fallback, 3000);
      fetch(req).then(res => {
        clearTimeout(timer);
        if (res.ok) caches.open(CACHE).then(c => c.put(req, res.clone()));
        if (!done) { done = true; resolve(res); }
      }).catch(() => { clearTimeout(timer); fromCache(req).then(hit => { if (!done) { done = true; resolve(hit || Response.error()); } }); });
    }));
    return;
  }
  // everything else (sounds, physics, icons): the saved copy, or the network the first time
  e.respondWith(fromCache(req).then(hit => hit || fetch(req)));
});
