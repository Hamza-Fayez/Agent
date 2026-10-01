const http = require('http');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 3000;
const root = __dirname;

const types = { '.html':'text/html; charset=utf-8', '.css':'text/css; charset=utf-8', '.js':'application/javascript; charset=utf-8', '.json':'application/json; charset=utf-8', '.svg':'image/svg+xml', '.png':'image/png', '.jpg':'image/jpeg', '.jpeg':'image/jpeg', '.webp':'image/webp' };

const socialDock = `
<style>
.socialDock{position:fixed;right:28px;bottom:28px;z-index:50;display:flex;gap:10px;direction:ltr}
.socialLink{width:46px;height:46px;border-radius:999px;display:grid;place-items:center;text-decoration:none;box-shadow:0 10px 28px rgba(0,0,0,.18);backdrop-filter:blur(16px);transition:transform .22s ease,box-shadow .22s ease}
.socialLink:hover{transform:translateY(-3px) scale(1.04);box-shadow:0 14px 34px rgba(0,0,0,.24)}
.socialLink:active{transform:scale(.96)}
.socialLink svg{width:24px;height:24px;display:block}
.socialLink.tiktok{background:#111;color:#fff;border:1px solid rgba(255,255,255,.16)}
.socialLink.snapchat{background:#fffc00;color:#111;border:1px solid rgba(17,17,17,.12)}
@media(max-width:760px){.socialDock{right:14px;bottom:16px;gap:8px}.socialLink{width:44px;height:44px}.socialLink svg{width:23px;height:23px}}
</style>
<div class="socialDock" aria-label="Social media links">
  <a class="socialLink tiktok" href="https://www.tiktok.com/@aml_suabhi" target="_blank" rel="noopener noreferrer" aria-label="TikTok — AML Suabhi" title="TikTok">
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M14.25 3.2v10.15a4.5 4.5 0 1 1-3.15-4.29v2.35a2.3 2.3 0 1 0 1.05 1.94V3.2h2.1Z" fill="#25F4EE" transform="translate(-.55 .35)"/>
      <path d="M14.25 3.2c.45 2.45 1.82 3.86 4.15 4.28V9.7a7.12 7.12 0 0 1-4.15-1.6v5.25a4.5 4.5 0 1 1-3.15-4.29v2.35a2.3 2.3 0 1 0 1.05 1.94V3.2h2.1Z" fill="#FE2C55" transform="translate(.45 -.1)"/>
      <path d="M14.25 3.2c.45 2.45 1.82 3.86 4.15 4.28V9.7a7.12 7.12 0 0 1-4.15-1.6v5.25a4.5 4.5 0 1 1-3.15-4.29v2.35a2.3 2.3 0 1 0 1.05 1.94V3.2h2.1Z" fill="currentColor"/>
    </svg>
  </a>
  <a class="socialLink snapchat" href="https://snapchat.com/t/OtS0exp9" target="_blank" rel="noopener noreferrer" aria-label="Snapchat — AML" title="Snapchat">
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 3.1c-2.7 0-4.65 2.14-4.65 5.15 0 1.08.12 1.95-.45 2.88-.43.7-1.15 1.2-2.06 1.49-.39.12-.48.67-.14.9.62.43 1.28.72 1.98.89.18.04.31.2.32.38.05.74.68 1.02 1.28 1.12.21.04.39.17.49.36.54 1.08 1.78 1.22 2.45.84.49-.28 1.07-.28 1.56 0 .67.38 1.91.24 2.45-.84.1-.19.28-.32.49-.36.6-.1 1.23-.38 1.28-1.12.01-.18.14-.34.32-.38.7-.17 1.36-.46 1.98-.89.34-.23.25-.78-.14-.9-.91-.29-1.63-.79-2.06-1.49-.57-.93-.45-1.8-.45-2.88C16.65 5.24 14.7 3.1 12 3.1Z" fill="#fff" stroke="currentColor" stroke-width="1.35" stroke-linejoin="round"/>
    </svg>
  </a>
</div>`;

function injectSocialDock(buffer) {
  const html = buffer.toString('utf8');
  if (html.includes('class="socialDock"')) return html;
  return html.replace(/<\/body>/i, `${socialDock}\n</body>`);
}

function sendHtml(res, buffer) {
  res.writeHead(200, { 'Content-Type':'text/html; charset=utf-8', 'Cache-Control':'no-cache' });
  res.end(injectSocialDock(buffer));
}

http.createServer((req, res) => {
  let pathname = decodeURIComponent((req.url || '/').split('?')[0]);
  if (pathname === '/') pathname = '/index.html';
  const file = path.join(root, pathname.replace(/^\/+/, ''));
  if (!file.startsWith(root)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) {
      fs.readFile(path.join(root, 'index.html'), (e2, fallback) => {
        if (e2) { res.writeHead(404); return res.end('Not found'); }
        sendHtml(res, fallback);
      });
      return;
    }
    const ext = path.extname(file).toLowerCase();
    if (ext === '.html') return sendHtml(res, data);
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control':'public, max-age=300' });
    res.end(data);
  });
}).listen(port, '0.0.0.0', () => console.log(`AML portfolio listening on ${port}`));
