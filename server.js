const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

const port = process.env.PORT || 3000;
const root = __dirname;
const types = { '.html':'text/html; charset=utf-8', '.css':'text/css; charset=utf-8', '.js':'application/javascript; charset=utf-8', '.json':'application/json; charset=utf-8', '.svg':'image/svg+xml', '.png':'image/png', '.jpg':'image/jpeg', '.jpeg':'image/jpeg', '.webp':'image/webp', '.mp4':'video/mp4', '.mov':'video/quicktime' };
const BUCKET = process.env.AWS_S3_BUCKET_NAME;
const META_KEY = 'aml-suabhi/portfolio/index.json';
const LEGACY_META_KEY = 'portfolio/index.json';
const MEDIA_PREFIX = 'aml-suabhi/works/';
const HOME_MEDIA_PREFIX = 'aml-suabhi/home-slideshow/';
const VIDEO_MEDIA_PREFIX = 'aml-suabhi/video-library/';
const MAX_UPLOAD = 300 * 1024 * 1024;
let mediaIntegrityPromise=null;
let metaCache=null;
const imageMediaCache=new Map();
const imageMediaInflight=new Map();
let imageMediaCacheBytes=0;
const IMAGE_CACHE_MAX_BYTES=64*1024*1024;
const s3 = new S3Client({
  region: process.env.AWS_DEFAULT_REGION || 'auto',
  endpoint: process.env.AWS_ENDPOINT_URL,
  forcePathStyle: false,
  credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID || '', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '' }
});
const socialDock = `
<style>
.socialDock{position:fixed;right:28px;bottom:28px;z-index:50;display:flex;gap:10px;direction:ltr}
.socialLink{width:46px;height:46px;border-radius:999px;display:grid;place-items:center;text-decoration:none;box-shadow:0 10px 28px rgba(0,0,0,.18);backdrop-filter:blur(16px);transition:transform .22s ease,box-shadow .22s ease}
.socialLink:hover{transform:translateY(-3px) scale(1.04);box-shadow:0 14px 34px rgba(0,0,0,.24)}.socialLink:active{transform:scale(.96)}
.socialLink svg{width:24px;height:24px;display:block}.socialLink.tiktok{background:#111;color:#fff;border:1px solid rgba(255,255,255,.16)}.socialLink.snapchat{background:#fffc00;color:#111;border:1px solid rgba(17,17,17,.12)}
@media(max-width:760px){.socialDock{right:14px;bottom:16px;gap:8px}.socialLink{width:44px;height:44px}.socialLink svg{width:23px;height:23px}}\n.mediaOverlayOpen .socialDock{display:none!important}
</style>
<div class="socialDock" aria-label="Social media links">
<a class="socialLink tiktok" href="https://www.tiktok.com/@aml_suabhi" target="_blank" rel="noopener noreferrer" aria-label="TikTok" title="TikTok"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14.25 3.2c.45 2.45 1.82 3.86 4.15 4.28V9.7a7.12 7.12 0 0 1-4.15-1.6v5.25a4.5 4.5 0 1 1-3.15-4.29v2.35a2.3 2.3 0 1 0 1.05 1.94V3.2h2.1Z" fill="currentColor"/></svg></a>
<a class="socialLink snapchat" href="https://snapchat.com/t/OtS0exp9" target="_blank" rel="noopener noreferrer" aria-label="Snapchat" title="Snapchat"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3.1c-2.7 0-4.65 2.14-4.65 5.15 0 1.08.12 1.95-.45 2.88-.43.7-1.15 1.2-2.06 1.49-.39.12-.48.67-.14.9.62.43 1.28.72 1.98.89.18.04.31.2.32.38.05.74.68 1.02 1.28 1.12.21.04.39.17.49.36.54 1.08 1.78 1.22 2.45.84.49-.28 1.07-.28 1.56 0 .67.38 1.91.24 2.45-.84.1-.19.28-.32.49-.36.6-.1 1.23-.38 1.28-1.12.01-.18.14-.34.32-.38.7-.17 1.36-.46 1.98-.89.34-.23.25-.78-.14-.9-.91-.29-1.63-.79-2.06-1.49-.57-.93-.45-1.8-.45-2.88C16.65 5.24 14.7 3.1 12 3.1Z" fill="#fff" stroke="currentColor" stroke-width="1.35" stroke-linejoin="round"/></svg></a>
</div>`;

function injectSocialDock(buffer){const html=buffer.toString('utf8');return html.includes('class="socialDock"')?html:html.replace(/<\/body>/i,socialDock+'\n</body>')}
function sendHtml(res,buffer){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});res.end(injectSocialDock(buffer))}
function json(res,status,data){res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(data))}
function parseCookies(req){const out={};String(req.headers.cookie||'').split(';').forEach(v=>{const i=v.indexOf('=');if(i>0)out[v.slice(0,i).trim()]=decodeURIComponent(v.slice(i+1).trim())});return out}
function sign(v){return crypto.createHmac('sha256',process.env.SESSION_SECRET||'').update(v).digest('base64url')}
function makeSession(user){const p=Buffer.from(JSON.stringify({u:user,exp:Date.now()+8*60*60*1000})).toString('base64url');return p+'.'+sign(p)}
function sessionUser(req){const token=parseCookies(req).aml_session;if(!token)return null;const [p,s]=token.split('.');if(!p||!s)return null;const a=Buffer.from(sign(p)),b=Buffer.from(s);if(a.length!==b.length||!crypto.timingSafeEqual(a,b))return null;try{const o=JSON.parse(Buffer.from(p,'base64url').toString());return o.exp>Date.now()?o.u:null}catch{return null}}
function requireAuth(req,res){const u=sessionUser(req);if(!u){json(res,401,{error:'Unauthorized'});return null}return u}
function readBody(req,limit=1024*1024){return new Promise((resolve,reject)=>{let n=0,chunks=[];req.on('data',c=>{n+=c.length;if(n>limit){reject(new Error('Too large'));req.destroy();return}chunks.push(c)});req.on('end',()=>resolve(Buffer.concat(chunks)));req.on('error',reject)})}
async function hashStoredObject(key){
  const out=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:key}));
  const hash=crypto.createHash('sha256');
  for await(const chunk of out.Body)hash.update(chunk);
  return hash.digest('hex');
}
async function readJson(req){const b=await readBody(req);return JSON.parse(b.toString('utf8')||'{}')}
function safeName(s){return String(s||'').trim().replace(/[<>:"/\\|?*\x00-\x1F]/g,'').slice(0,80)}
function slug(s){return safeName(s).toLowerCase().replace(/[^a-z0-9\u0600-\u06ff]+/g,'-').replace(/^-+|-+$/g,'').slice(0,48)||'collection'}
function extOf(name,type){const e=path.extname(name||'').toLowerCase().replace(/[^.a-z0-9]/g,'');if(e&&e.length<=8)return e;return type.startsWith('video/')?'.mp4':'.jpg'}
async function streamToBuffer(stream){const chunks=[];for await(const c of stream)chunks.push(Buffer.from(c));return Buffer.concat(chunks)}
function isImageMediaKey(key){return /\.(?:jpe?g|png|webp|gif|avif)$/i.test(key)}
function imageTypeFromKey(key){
  const ext=path.extname(key).toLowerCase();
  return types[ext]||'application/octet-stream';
}
function rememberImageMedia(key,entry){
  if(entry.buffer.length>8*1024*1024)return;
  if(imageMediaCache.has(key)){
    imageMediaCacheBytes-=imageMediaCache.get(key).buffer.length;
    imageMediaCache.delete(key);
  }
  imageMediaCache.set(key,entry);
  imageMediaCacheBytes+=entry.buffer.length;
  while(imageMediaCacheBytes>IMAGE_CACHE_MAX_BYTES&&imageMediaCache.size){
    const oldest=imageMediaCache.keys().next().value;
    const value=imageMediaCache.get(oldest);
    imageMediaCache.delete(oldest);
    imageMediaCacheBytes-=value.buffer.length;
  }
}
async function getCachedImageMedia(key){
  const hit=imageMediaCache.get(key);
  if(hit){
    imageMediaCache.delete(key);
    imageMediaCache.set(key,hit);
    return hit;
  }
  if(imageMediaInflight.has(key))return imageMediaInflight.get(key);
  const work=(async()=>{
    let lastError;
    for(let attempt=0;attempt<3;attempt++){
      try{
        const signal=AbortSignal.timeout(5000);
        const out=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:key}),{abortSignal:signal});
        const buffer=await streamToBuffer(out.Body);
        const entry={buffer,type:out.ContentType||imageTypeFromKey(key)};
        rememberImageMedia(key,entry);
        return entry;
      }catch(e){
        lastError=e;
        if(e.name==='NoSuchKey'||e.$metadata?.httpStatusCode===404)throw e;
        if(attempt<2)await new Promise(r=>setTimeout(r,200*(attempt+1)));
      }
    }
    throw lastError;
  })().finally(()=>imageMediaInflight.delete(key));
  imageMediaInflight.set(key,work);
  return work;
}

async function readMetaKey(key){
  let lastError;
  for(let attempt=0;attempt<3;attempt++){
    try{
      const signal=AbortSignal.timeout(4000);
      const o=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:key}),{abortSignal:signal});
      const parsed=JSON.parse((await streamToBuffer(o.Body)).toString('utf8'));
      if(key===META_KEY)metaCache=parsed;
      return parsed;
    }catch(e){
      if(e.name==='NoSuchKey'||e.$metadata?.httpStatusCode===404)return null;
      lastError=e;
      if(attempt<2)await new Promise(r=>setTimeout(r,250*(attempt+1)));
    }
  }
  if(key===META_KEY&&metaCache)return JSON.parse(JSON.stringify(metaCache));
  throw lastError;
}
function ensureSystemCollections(meta){
  if(!meta||!Array.isArray(meta.collections))meta={version:1,collections:[]};
  let changed=false;

  let home=meta.collections.find(c=>c.systemRole==='home_slideshow'||c.id==='home-slideshow');
  if(!home){
    const seed=meta.collections
      .filter(c=>!c.systemRole)
      .flatMap(c=>(c.items||[]).filter(i=>i.type==='image').map(i=>({...i,id:'home-seed-'+i.id,shared:true,sourceCollectionId:c.id})));
    home={id:'home-slideshow',name:'فيديو متحرك',systemRole:'home_slideshow',locked:true,createdAt:new Date().toISOString(),cover:seed[0]?.id||null,items:seed};
    meta.collections.unshift(home);
    changed=true;
  }else{
    if(home.name!=='فيديو متحرك'){home.name='فيديو متحرك';changed=true}
    if(home.systemRole!=='home_slideshow'){home.systemRole='home_slideshow';changed=true}
    if(home.locked!==true){home.locked=true;changed=true}
  }

  let videos=meta.collections.find(c=>c.systemRole==='video_library'||c.id==='video-library');
  if(!videos){
    videos={id:'video-library',name:'فيديوهات',systemRole:'video_library',locked:true,createdAt:new Date().toISOString(),cover:null,items:[]};
    meta.collections.splice(1,0,videos);
    changed=true;
  }else{
    if(videos.name!=='فيديوهات'){videos.name='فيديوهات';changed=true}
    if(videos.systemRole!=='video_library'){videos.systemRole='video_library';changed=true}
    if(videos.locked!==true){videos.locked=true;changed=true}
    const onlyVideos=(videos.items||[]).filter(i=>i.type==='video');
    if(onlyVideos.length!==(videos.items||[]).length){videos.items=onlyVideos;changed=true}
  }

  // Keep all video content in the dedicated video library.
  const existingVideoIds=new Set((videos.items||[]).map(i=>i.id));
  let movedVideos=0;
  for(const c of meta.collections){
    if(c.systemRole)continue;
    const kept=[];
    for(const item of c.items||[]){
      if(item.type==='video'){
        if(!existingVideoIds.has(item.id)){
          videos.items.push(item);
          existingVideoIds.add(item.id);
        }
        movedVideos++;
        changed=true;
      }else{
        kept.push(item);
      }
    }
    if(kept.length!==(c.items||[]).length){
      c.items=kept;
      if(c.cover&&!kept.some(i=>i.id===c.cover))c.cover=(kept.find(i=>i.type==='image')||{}).id||null;
    }
  }
  if(movedVideos)console.log('AML_VIDEO_LIBRARY_MOVED '+movedVideos);

  return {meta,changed};
}
async function readMeta(){
  if(metaCache)return JSON.parse(JSON.stringify(metaCache));
  const scoped=await readMetaKey(META_KEY);
  if(scoped){
    const ensured=ensureSystemCollections(scoped);
    if(ensured.changed)await writeMeta(ensured.meta);
    return ensured.meta;
  }
  const legacy=await readMetaKey(LEGACY_META_KEY);
  if(!legacy){
    const ensured=ensureSystemCollections({version:1,collections:[]});
    await writeMeta(ensured.meta);
    return ensured.meta;
  }
  await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:'aml-suabhi/backups/legacy-before-isolation-20261004.json',Body:JSON.stringify(legacy),ContentType:'application/json',CacheControl:'no-cache'}));
  const unexpected=new Set(['d532919e-eaaf-424c-829a-a235d96b6689','da57832a-b672-439c-82a7-0564d857c1de']);
  for(const c of legacy.collections||[]){c.items=(c.items||[]).filter(i=>!unexpected.has(i.id));if(unexpected.has(c.cover))c.cover=(c.items.find(i=>i.type==='image')||c.items[0]||{}).id||null}
  const ensured=ensureSystemCollections(legacy);
  await writeMeta(ensured.meta);
  console.log('AML_STORAGE_ISOLATED migrated='+String((legacy.collections||[]).length)+' quarantined=2');
  return ensured.meta;
}
async function runMediaIntegrityPass(){
  const meta=await readMeta();
  if(meta.mediaIntegrityVersion===5){
    console.log('AML_MEDIA_INTEGRITY_SKIP version=5');
    return;
  }
  const backupKey='aml-suabhi/backups/pre-media-integrity-'+new Date().toISOString().replace(/[:.]/g,'-')+'.json';
  await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:backupKey,Body:JSON.stringify(meta),ContentType:'application/json',CacheControl:'no-cache'}));

  const globalSeen=new Map(),homeSeen=new Map();
  let hashed=0,duplicates=0,missing=0,coversFixed=0;
  const duplicateRows=[];

  for(const c of meta.collections||[]){
    const seen=c.systemRole==='home_slideshow'?homeSeen:globalSeen;
    const kept=[];
    for(const item of c.items||[]){
      try{
        if(!item.sha256)item.sha256=await hashStoredObject(item.key);
        hashed++;
      }catch(e){
        missing++;
        console.error('AML_MEDIA_MISSING '+JSON.stringify({collection:c.id,item:item.id,name:item.name,key:item.key}));
        if(c.systemRole==='home_slideshow'&&item.shared)continue;
        kept.push(item);
        continue;
      }
      const prior=seen.get(item.sha256);
      if(prior){
        duplicates++;
        duplicateRows.push({removed:{collection:c.id,item:item.id,name:item.name,key:item.key},kept:prior});
        continue;
      }
      seen.set(item.sha256,{collection:c.id,item:item.id,name:item.name,key:item.key});
      kept.push(item);
    }
    if(kept.length!==(c.items||[]).length)c.items=kept;
    const validCover=c.cover&&c.items.some(i=>i.id===c.cover);
    if(!validCover){
      const preferred=c.items.find(i=>i.type==='image')||c.items[0]||null;
      const next=preferred?.id||null;
      if(c.cover!==next){c.cover=next;coversFixed++}
    }
  }

  meta.mediaIntegrityVersion=5;
  meta.mediaIntegrityAt=new Date().toISOString();
  await writeMeta(meta);
  console.log('AML_MEDIA_INTEGRITY_DONE '+JSON.stringify({backupKey,hashed,duplicates,missing,coversFixed,duplicateRows}));
}

async function writeMeta(meta){await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:META_KEY,Body:JSON.stringify(meta),ContentType:'application/json',CacheControl:'no-cache'}));metaCache=JSON.parse(JSON.stringify(meta))}
function publicCollection(c){
  const cover=(c.items.find(i=>i.id===c.cover)||c.items.find(i=>i.type==='image')||c.items[0]||null);
  return {
    id:c.id,name:c.name,createdAt:c.createdAt,systemRole:c.systemRole||null,locked:!!c.locked,
    cover:cover?{...cover,url:'/media/'+encodeURIComponent(cover.key)}:null,
    items:(c.items||[]).map(i=>({...i,url:'/media/'+encodeURIComponent(i.key)}))
  };
}
function publicMeta(meta,{includeSystem=true}={}){
  const rows=(meta.collections||[]).filter(c=>includeSystem||!c.systemRole).map(publicCollection);
  return {collections:rows};
}

async function handleApi(req,res,pathname,url){
  if(pathname==='/api/session'&&req.method==='GET'){const u=sessionUser(req);return u?json(res,200,{ok:true,user:u}):json(res,401,{error:'Unauthorized'})}
  if(pathname==='/api/login'&&req.method==='POST'){try{const b=await readJson(req);const okUser=String(b.username||'')===(process.env.ADMIN_USER||'');const salt=Buffer.from(process.env.ADMIN_PASSWORD_SALT||'','base64');const expected=Buffer.from(process.env.ADMIN_PASSWORD_HASH||'','base64');const actual=crypto.scryptSync(String(b.password||''),salt,32,{N:16384,r:8,p:1});const okPass=expected.length===actual.length&&crypto.timingSafeEqual(expected,actual);if(!okUser||!okPass)return json(res,401,{error:'Invalid login'});const token=makeSession(b.username);res.writeHead(200,{'Content-Type':'application/json','Set-Cookie':'aml_session='+encodeURIComponent(token)+'; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=28800'});return res.end('{"ok":true}')}catch(e){return json(res,400,{error:'Bad request'})}}
  if(pathname==='/api/logout'&&req.method==='POST'){res.writeHead(200,{'Content-Type':'application/json','Set-Cookie':'aml_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0'});return res.end('{"ok":true}')}
  if(pathname==='/api/works'&&req.method==='GET'){try{return json(res,200,publicMeta(await readMeta(),{includeSystem:false}))}catch(e){console.error(e);return json(res,500,{error:'Could not load works'})}}
  if(pathname==='/api/home-slides'&&req.method==='GET'){try{const meta=await readMeta();const c=meta.collections.find(x=>x.systemRole==='home_slideshow');return json(res,200,{collection:c?publicCollection(c):null})}catch(e){console.error(e);return json(res,500,{error:'Could not load home slides'})}}
  if(pathname==='/api/videos'&&req.method==='GET'){try{const meta=await readMeta();const c=meta.collections.find(x=>x.systemRole==='video_library');const out=c?publicCollection({...c,items:(c.items||[]).filter(i=>i.type==='video')}):null;return json(res,200,{collection:out})}catch(e){console.error(e);return json(res,500,{error:'Could not load videos'})}}
  if(pathname.startsWith('/media/')&&req.method==='GET'){
    try{
      const key=decodeURIComponent(pathname.slice(7));
      if(isImageMediaKey(key)&&!req.headers.range){
        const entry=await getCachedImageMedia(key);
        res.writeHead(200,{
          'Content-Type':entry.type,
          'Content-Length':String(entry.buffer.length),
          'Cache-Control':'public,max-age=86400,stale-while-revalidate=604800'
        });
        return res.end(entry.buffer);
      }
      const signal=AbortSignal.timeout(12000);
      const out=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:key,Range:req.headers.range}),{abortSignal:signal});
      const headers={'Content-Type':out.ContentType||'application/octet-stream','Cache-Control':'public,max-age=3600','Accept-Ranges':'bytes'};
      if(out.ContentLength!=null)headers['Content-Length']=String(out.ContentLength);
      if(out.ContentRange)headers['Content-Range']=out.ContentRange;
      res.writeHead(out.ContentRange?206:200,headers);
      return out.Body.pipe(res)
    }catch(e){
      if(e.name==='TimeoutError'||e.name==='AbortError')return json(res,504,{error:'Media temporarily unavailable'});
      return json(res,404,{error:'Not found'})
    }
  }
  if(!pathname.startsWith('/api/admin/'))return false;
  if(!requireAuth(req,res))return true;
  if(req.method!=='GET'&&mediaIntegrityPromise)await mediaIntegrityPromise;
  try{
    if(pathname==='/api/admin/collections'&&req.method==='GET')return json(res,200,publicMeta(await readMeta()));
    if(pathname==='/api/admin/collections'&&req.method==='POST'){const b=await readJson(req);const name=safeName(b.name||'');const meta=await readMeta();const id=slug(name)+'-'+crypto.randomBytes(3).toString('hex');meta.collections.unshift({id,name,createdAt:new Date().toISOString(),cover:null,items:[]});await writeMeta(meta);return json(res,201,{id,name})}
    if(pathname==='/api/admin/upload'&&req.method==='POST'){const collection=url.searchParams.get('collection');const original=safeName(url.searchParams.get('name')||'file');const type=String(req.headers['content-type']||'');const len=Number(req.headers['content-length']||0);if(!type.startsWith('image/')&&!type.startsWith('video/'))return json(res,415,{error:'Images and videos only'});if(len>MAX_UPLOAD)return json(res,413,{error:'File too large'});const meta=await readMeta();const c=meta.collections.find(x=>x.id===collection);if(!c)return json(res,404,{error:'Folder not found'});if(c.systemRole==='home_slideshow'&&!type.startsWith('image/'))return json(res,415,{error:'Home slideshow accepts images only'});if(c.systemRole==='video_library'&&!type.startsWith('video/'))return json(res,415,{error:'Video library accepts videos only'});const id=crypto.randomUUID();const prefix=c.systemRole==='home_slideshow'?HOME_MEDIA_PREFIX:c.systemRole==='video_library'?VIDEO_MEDIA_PREFIX:(MEDIA_PREFIX+c.id+'/');const key=prefix+Date.now()+'-'+crypto.randomBytes(4).toString('hex')+extOf(original,type);await new Upload({client:s3,params:{Bucket:BUCKET,Key:key,Body:req,ContentType:type,CacheControl:'public,max-age=31536000,immutable'}}).done();const sha256=await hashStoredObject(key);
    const duplicate=(c.items||[]).find(i=>i.sha256&&i.sha256===sha256);
    if(duplicate){
      await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:key}));
      return json(res,200,{ok:true,duplicate:true,item:{...duplicate,url:'/media/'+encodeURIComponent(duplicate.key)}});
    }
    const item={id,key,type:type.startsWith('video/')?'video':'image',name:original,createdAt:new Date().toISOString(),sha256};
    c.items.push(item);
    if(!c.cover)c.cover=id;
    await writeMeta(meta);
    return json(res,201,{ok:true,item:{...item,url:'/media/'+encodeURIComponent(key)}})}
    if(pathname==='/api/admin/rename'&&req.method==='POST'){const b=await readJson(req);const name=safeName(b.name||'');const meta=await readMeta();const c=meta.collections.find(x=>x.id===b.collection);if(!c)return json(res,404,{error:'Folder not found'});if(c.locked)return json(res,409,{error:'System folder cannot be renamed'});c.name=name;await writeMeta(meta);return json(res,200,{ok:true,name})}
    if(pathname==='/api/admin/cover'&&req.method==='POST'){const b=await readJson(req);const meta=await readMeta();const c=meta.collections.find(x=>x.id===b.collection);if(!c||!c.items.some(i=>i.id===b.item))return json(res,404,{error:'Not found'});c.cover=b.item;await writeMeta(meta);return json(res,200,{ok:true})}
    if(pathname==='/api/admin/item'&&req.method==='DELETE'){const cid=url.searchParams.get('collection'),iid=url.searchParams.get('item');const meta=await readMeta();const c=meta.collections.find(x=>x.id===cid);if(!c)return json(res,404,{error:'Folder not found'});const i=c.items.findIndex(x=>x.id===iid);if(i<0)return json(res,404,{error:'Item not found'});const item=c.items[i];if(!item.shared)await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:item.key}));c.items.splice(i,1);if(c.cover===iid)c.cover=c.items.find(x=>x.type==='image')?.id||c.items[0]?.id||null;await writeMeta(meta);return json(res,200,{ok:true})}
    if(pathname==='/api/admin/collection'&&req.method==='DELETE'){const id=url.searchParams.get('id');const meta=await readMeta();const i=meta.collections.findIndex(x=>x.id===id);if(i<0)return json(res,404,{error:'Folder not found'});const c=meta.collections[i];if(c.locked)return json(res,409,{error:'System folder cannot be deleted'});if(c.items.length)await s3.send(new DeleteObjectsCommand({Bucket:BUCKET,Delete:{Objects:c.items.filter(x=>!x.shared).map(x=>({Key:x.key})),Quiet:true}}));meta.collections.splice(i,1);await writeMeta(meta);return json(res,200,{ok:true})}
  }catch(e){console.error(e);return json(res,500,{error:'Server error'})}
  return json(res,404,{error:'Not found'});
}

const server=http.createServer(async (req,res)=>{
  const url=new URL(req.url||'/', 'http://localhost');
  const pathname=decodeURIComponent(url.pathname);
  try{const handled=await handleApi(req,res,pathname,url);if(handled!==false)return}catch(e){console.error(e);return json(res,500,{error:'Server error'})}
  if(pathname==='/manage'||pathname==='/manage/'){return fs.readFile(path.join(root,'manage.html'),(err,data)=>{if(err){res.writeHead(500);return res.end('Manager unavailable')}res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store','X-Robots-Tag':'noindex, nofollow'});res.end(data)})}
  let filePath=pathname==='/'?'/index.html':pathname;const file=path.join(root,filePath.replace(/^\/+/,''));if(!file.startsWith(root)){res.writeHead(403);return res.end('Forbidden')}
  fs.readFile(file,(err,data)=>{if(err){fs.readFile(path.join(root,'index.html'),(e2,fallback)=>{if(e2){res.writeHead(404);return res.end('Not found')}sendHtml(res,fallback)});return}const ext=path.extname(file).toLowerCase();if(ext==='.html')return sendHtml(res,data);res.writeHead(200,{'Content-Type':types[ext]||'application/octet-stream','Cache-Control':'public,max-age=300'});res.end(data)})
});
server.listen(port,'0.0.0.0',()=>console.log('AML portfolio listening on '+port));
mediaIntegrityPromise=readMeta()
  .then(()=>runMediaIntegrityPass())
  .catch(e=>console.error('AML_MEDIA_INTEGRITY_ERROR',e?.stack||e?.message||e))
  .finally(()=>{mediaIntegrityPromise=null});
