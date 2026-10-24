const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { spawnSync, spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const ffmpegPath = require('ffmpeg-static');
const { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
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
function injectSocialDock(buffer){return buffer.toString('utf8')}

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

/* ---------- Homepage curation settings (managed from /manage) ---------- */
const HOME_LIMITS={videos:4,photos:8,ring:7,tapeWords:10};
const HOME_DEFAULTS={
  // Hero: latest talking/interview video (black outfit, warm interior).
  heroVideoId:'7b2ae1aa-b4df-4479-901d-060b21b4842f',
  // Four curated non-bridal films: studio abaya campaign, white-backdrop portrait film, gold-jacket beauty, boutique evening.
  videoIds:['2c3397f2-39d3-4c02-96d0-e84e66327781','05e2dd49-540e-4320-a584-c7047b268429','80c7ffb5-f9d0-439f-9419-8706c5eaaa69','2bfcff33-d979-4825-be1a-48e00c995065'],
  // Curated gallery: strongest portrait, beauty, editorial, warm beauty, wider lifestyle, fashion, B&W, colour.
  photoIds:['3a56b558-3687-4f87-8f63-332f6c29b67f','0a1a6c62-0732-4afc-9ae9-d28b13c98e13','56091470-7961-4bde-8a38-59cfb5d2ffde','81fbf39c-2b0c-4d0e-a59f-0f4e5019c9fe','886295b6-c575-49a0-aabe-32fbcba9870e','493c2232-a308-46df-a6bd-1ab8c6e9bcfe','0b9ddf8b-e50c-410e-933d-970707fbb1ee','8361419f-f49e-4319-9868-689d8d863603'],
  // Profile portrait on the Info page: cream blazer, looking up.
  portraitId:'493c2232-a308-46df-a6bd-1ab8c6e9bcfe'
};
// Bumped whenever a default changes and stored settings should follow it once.
const HOME_REV=2;
const RETIRED_DEFAULT_PORTRAIT='3a56b558-3687-4f87-8f63-332f6c29b67f';
// Editable homepage copy and contact details. key:[maxLength, default]. Empty input falls back to the default.
const TEXT_FIELDS={
  heroEyebrow:[60,'AML · Model · Riyadh'],
  heroLine:[160,'عارضة أزياء سعودية من الرياض. أزياء، جمال، ومحتوى تجاري.'],
  introLine:[220,'أعمال في الأزياء والجمال والمحتوى التجاري. متاحة للتصوير والحملات والتعاونات.'],
  infoLine:[160,'أمل أحمد — عارضة أزياء، الرياض · أزياء · جمال · محتوى تجاري'],
  bio:[420,'عارضة أزياء ومقدمة محتوى في الرياض، متخصصة في الأزياء والجمال والمحتوى التجاري، ومتاحة للتصوير والحملات والتعاونات.'],
  footerLine:[160,'أمل أحمد — عارضة أزياء · الرياض. للتعاون والحجز:'],
  height:[3,'160'],weight:[3,'65'],age:[3,'28'],
  nationality:[30,'سعودية'],city:[30,'الرياض'],
  whatsapp:[15,'966500730856'],tiktok:[24,'aml_suabhi']
};
const TAPE_WORDS_DEFAULT=['AML','Fashion','Beauty','Editorial','Commercial','Campaign','Riyadh'];
const TEXT_RULES={height:/^\d{2,3}$/,weight:/^\d{2,3}$/,age:/^\d{1,3}$/,whatsapp:/^\d{8,15}$/,tiktok:/^[A-Za-z0-9._]{2,24}$/};
function cleanLine(v,max){return String(v==null?'':v).replace(/[\u0000-\u001f\u007f<>]/g,' ').replace(/\s+/g,' ').trim().slice(0,max)}
function cleanTapeWords(list){
  const out=[];
  for(const v of Array.isArray(list)?list:[]){const w=cleanLine(v,24);if(w&&!out.includes(w))out.push(w);if(out.length>=HOME_LIMITS.tapeWords)break}
  return out;
}
// Returns {text, errors}: a full, safe text object built from whatever was submitted.
function sanitizeHomeText(input){
  const src=input&&typeof input==='object'?input:{};
  const text={},errors=[];
  for(const key of Object.keys(TEXT_FIELDS)){
    const [max,def]=TEXT_FIELDS[key];
    let v=cleanLine(src[key],key==='whatsapp'||key==='tiktok'?40:max);
    if(key==='whatsapp')v=v.replace(/\D/g,'').replace(/^00/,'');
    if(key==='tiktok')v=v.replace(/^@+/,'');
    if(!v){text[key]=def;continue}
    if(TEXT_RULES[key]&&!TEXT_RULES[key].test(v)){errors.push('text.'+key);text[key]=def;continue}
    text[key]=v;
  }
  const words=cleanTapeWords(src.tapeWords);
  text.tapeWords=words.length>=2?words:[...TAPE_WORDS_DEFAULT];
  return {text,errors};
}
function uniqStrings(list,limit){const out=[];for(const v of Array.isArray(list)?list:[]){const s=String(v||'').trim();if(s&&!out.includes(s))out.push(s);if(out.length>=limit)break}return out}
function ensureHomeSettings(meta){
  let changed=false;
  if(!meta.home||typeof meta.home!=='object'){meta.home=JSON.parse(JSON.stringify(HOME_DEFAULTS));changed=true}
  const h=meta.home;
  if(typeof h.heroVideoId!=='string'){h.heroVideoId=HOME_DEFAULTS.heroVideoId;changed=true}
  if(!Array.isArray(h.videoIds)){h.videoIds=[...HOME_DEFAULTS.videoIds];changed=true}
  if(!Array.isArray(h.photoIds)){h.photoIds=[...HOME_DEFAULTS.photoIds];changed=true}
  if(h.portraitId!==null&&typeof h.portraitId!=='string'){h.portraitId=HOME_DEFAULTS.portraitId;changed=true}
  if((h.rev||1)<HOME_REV){
    // rev 2: the default profile portrait changed. Follow it only if the stored choice is still the old default,
    // so a portrait picked in Studio is never overwritten.
    if(h.portraitId===RETIRED_DEFAULT_PORTRAIT)h.portraitId=HOME_DEFAULTS.portraitId;
    h.rev=HOME_REV;changed=true;
  }
  return changed;
}
function videoLibraryOf(meta){return (meta.collections||[]).find(c=>c.systemRole==='video_library')||null}
function workImagesOf(meta){
  const out=[];
  for(const c of meta.collections||[]){
    if(c.systemRole)continue;
    for(const i of c.items||[])if(i.type==='image')out.push({...i,collectionId:c.id,collectionName:c.name||''});
  }
  return out;
}
function decorateItem(i){
  if(!i)return null;
  return {...i,url:'/media/'+encodeURIComponent(i.key),poster:i.posterKey?'/media/'+encodeURIComponent(i.posterKey):null,...(i.type==='video'&&isEnhancing(i.id)?{processing:true}:{})};
}
function resolveHome(meta){
  const settings=meta.home||HOME_DEFAULTS;
  const library=videoLibraryOf(meta);
  const videos=(library?.items||[]).filter(i=>i.type==='video');
  const images=workImagesOf(meta);
  const byVideo=id=>videos.find(v=>v.id===id)||null;
  const byImage=id=>images.find(v=>v.id===id)||null;
  let hero=byVideo(settings.heroVideoId);
  if(!hero)hero=videos.find(v=>v.posterKey)||videos[0]||null;
  let selectedVideos=uniqStrings(settings.videoIds,HOME_LIMITS.videos).map(byVideo).filter(v=>v&&v.id!==hero?.id);
  if(!selectedVideos.length)selectedVideos=videos.filter(v=>v.id!==hero?.id&&v.posterKey).slice(0,HOME_LIMITS.videos);
  let photos=uniqStrings(settings.photoIds,HOME_LIMITS.photos).map(byImage).filter(Boolean);
  if(!photos.length)photos=images.slice(0,HOME_LIMITS.photos);
  let portrait=settings.portraitId?byImage(settings.portraitId):null;
  if(!portrait)portrait=photos[0]||images[0]||null;
  // Stills that orbit in the hero carousel; until chosen in Studio they follow the gallery selection.
  let ring=uniqStrings(settings.ringIds,HOME_LIMITS.ring).map(byImage).filter(Boolean);
  if(!ring.length)ring=photos.slice(0,HOME_LIMITS.ring);
  const text=sanitizeHomeText(settings.text).text;
  return {
    settings:{heroVideoId:hero?.id||null,videoIds:selectedVideos.map(v=>v.id),photoIds:photos.map(p=>p.id),portraitId:portrait?.id||null,ringIds:ring.map(p=>p.id),text},
    limits:HOME_LIMITS,
    hero:decorateItem(hero),
    videos:selectedVideos.map(decorateItem),
    photos:photos.map(decorateItem),
    portrait:decorateItem(portrait),
    ring:ring.map(decorateItem),
    text
  };
}
function pruneHomeSettings(meta,removedIds){
  if(!meta.home)return false;
  const gone=new Set(removedIds);
  let changed=false;
  if(gone.has(meta.home.heroVideoId)){meta.home.heroVideoId=null;changed=true}
  if(gone.has(meta.home.portraitId)){meta.home.portraitId=null;changed=true}
  const v=(meta.home.videoIds||[]).filter(id=>!gone.has(id));if(v.length!==(meta.home.videoIds||[]).length){meta.home.videoIds=v;changed=true}
  const p=(meta.home.photoIds||[]).filter(id=>!gone.has(id));if(p.length!==(meta.home.photoIds||[]).length){meta.home.photoIds=p;changed=true}
  const r=(meta.home.ringIds||[]).filter(id=>!gone.has(id));if(r.length!==(meta.home.ringIds||[]).length){meta.home.ringIds=r;changed=true}
  return changed;
}

/* ---------- Persisted video posters (never seek live <video> tags for thumbnails) ---------- */
const POSTER_PREFIX='aml-suabhi/video-posters/';
// Hand-picked frames (seconds) for the curated homepage films; every other video gets an automatic frame.
const CURATED_POSTER_TIMES={
  '7b2ae1aa-b4df-4479-901d-060b21b4842f':7.75,
  '2c3397f2-39d3-4c02-96d0-e84e66327781':7.0,
  '05e2dd49-540e-4320-a584-c7047b268429':10.0,
  '80c7ffb5-f9d0-439f-9419-8706c5eaaa69':8.0,
  '2bfcff33-d979-4825-be1a-48e00c995065':6.5
};
function probeDuration(input){
  try{
    const r=spawnSync(ffmpegPath,['-hide_banner','-i',input],{encoding:'utf8',maxBuffer:4*1024*1024});
    const m=/Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(String(r.stderr||''));
    if(!m)return 0;
    return Number(m[1])*3600+Number(m[2])*60+Number(m[3]);
  }catch{return 0}
}
function autoPosterSecond(duration){
  if(!duration||!Number.isFinite(duration))return 1.5;
  return Math.max(0.5,Math.min(8,duration*0.25));
}
async function renderPosterFrame(item,second,dir){
  const ext=path.extname(item.key||'')||'.mov';
  const input=path.join(dir,item.id+ext);
  const output=path.join(dir,item.id+'.jpg');
  try{
    const obj=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:item.key}));
    await pipeline(obj.Body,fs.createWriteStream(input));
    const duration=probeDuration(input);
    let at=Number.isFinite(second)&&second>=0?second:autoPosterSecond(duration);
    if(duration&&at>duration-0.3)at=Math.max(0,duration-0.5);
    const attempt=t=>spawnSync(ffmpegPath,['-y','-hide_banner','-loglevel','error','-ss',String(t),'-i',input,'-frames:v','1','-vf',"scale='min(1080,max(720,iw))':-2:flags=lanczos",'-q:v','3',output],{encoding:'utf8',maxBuffer:8*1024*1024});
    let r=attempt(at);
    if(r.status!==0||!fs.existsSync(output)){at=0;r=attempt(0)}
    if(r.status!==0||!fs.existsSync(output))throw new Error(String(r.stderr||'ffmpeg failed').slice(-800));
    const body=fs.readFileSync(output);
    const posterKey=POSTER_PREFIX+item.id+'-'+Date.now().toString(36)+'.jpg';
    await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:posterKey,Body:body,ContentType:'image/jpeg',CacheControl:'public,max-age=31536000,immutable'}));
    return {posterKey,second:at,duration};
  }finally{
    try{fs.unlinkSync(input)}catch{}
    try{fs.unlinkSync(output)}catch{}
  }
}
let posterRunPromise=null,posterRunQueued=false;
function scheduleVideoPosters(delay=800){
  if(posterRunPromise){posterRunQueued=true;return posterRunPromise}
  posterRunPromise=new Promise(r=>setTimeout(r,delay))
    .then(()=>ensureVideoPosters())
    .catch(e=>console.error('AML_POSTERS_ERROR',e?.stack||e?.message||e))
    .finally(()=>{posterRunPromise=null;if(posterRunQueued){posterRunQueued=false;scheduleVideoPosters(300)}});
  return posterRunPromise;
}
/* Metadata is one JSON object, so every read-modify-write must be serialized.
   Admin mutations and background jobs (posters, enhancement) all go through this lock;
   background jobs additionally re-read fresh metadata right before applying their change. */
let metaLockTail=Promise.resolve();
function withMetaLock(fn){
  const run=metaLockTail.then(fn,fn);
  metaLockTail=run.then(()=>{},()=>{});
  return run;
}
async function applyToLibraryVideo(itemId,expectedKey,apply){
  return withMetaLock(async()=>{
    const fresh=await readMeta();
    const it=(videoLibraryOf(fresh)?.items||[]).find(i=>i.id===itemId&&i.type==='video');
    if(!it||(expectedKey&&it.key!==expectedKey))return null;
    apply(it);
    await writeMeta(fresh);
    return it;
  });
}
async function ensureVideoPosters(){
  const snapshot=await readMeta();
  const library=videoLibraryOf(snapshot);
  if(!library)return;
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'aml-posters-'));
  let generated=0,existing=0,skipped=0;
  const failed=[];
  // Homepage videos first, so the public page has its posters within seconds of a deploy or an upload.
  const home=snapshot.home||HOME_DEFAULTS;
  const priority=[home.heroVideoId,...(home.videoIds||[])];
  const rank=id=>{const i=priority.indexOf(id);return i<0?priority.length:i};
  const queue=(library.items||[]).filter(i=>i.type==='video').sort((a,b)=>rank(a.id)-rank(b.id));
  try{
    for(const item of queue){
      const curated=CURATED_POSTER_TIMES[item.id];
      const needsCurated=curated!=null&&item.posterSecond!==curated;
      if(item.posterKey&&!needsCurated){existing++;continue}
      if(!item.posterKey&&curated==null){
        // Adopt a poster generated by an earlier release if it already exists.
        const legacyKey=POSTER_PREFIX+item.id+'.jpg';
        let exists=false;
        try{await s3.send(new HeadObjectCommand({Bucket:BUCKET,Key:legacyKey}));exists=true}catch{}
        if(exists){await applyToLibraryVideo(item.id,item.key,it=>{if(!it.posterKey)it.posterKey=legacyKey});existing++;continue}
      }
      try{
        const r=await renderPosterFrame(item,curated,dir);
        // Persist each poster as soon as it is ready, against fresh metadata, so nothing else is overwritten.
        const applied=await applyToLibraryVideo(item.id,item.key,it=>{it.posterKey=r.posterKey;it.posterSecond=r.second;if(r.duration)it.duration=Math.round(r.duration*100)/100});
        if(applied)generated++;
        else{skipped++;try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:r.posterKey}))}catch{}}
      }catch(e){failed.push({id:item.id,error:e?.message||String(e)})}
    }
  }finally{
    try{fs.rmSync(dir,{recursive:true,force:true})}catch{}
  }
  console.log('AML_VIDEO_POSTERS_DONE '+JSON.stringify({generated,existing,skipped,failed}));
}
function runFfmpeg(args,{timeoutMs=20*60*1000}={}){
  // Asynchronous so a long encode never blocks the web server's event loop.
  return new Promise((resolve,reject)=>{
    const child=spawn(ffmpegPath,args,{stdio:['ignore','ignore','pipe']});
    let err='';
    const timer=setTimeout(()=>{try{child.kill('SIGKILL')}catch{}reject(new Error('ffmpeg timed out'))},timeoutMs);
    child.stderr.on('data',d=>{err=(err+d).slice(-2000)});
    child.on('error',e=>{clearTimeout(timer);reject(e)});
    child.on('close',code=>{clearTimeout(timer);code===0?resolve():reject(new Error('ffmpeg exit '+code+': '+err.slice(-600)))});
  });
}
/* ---------- Video enhancement: build a clean 1080p streaming master from a low-resolution upload ----------
   Used for the hero (464x832 CapCut export): crops the watermark strips, upscales with lanczos + light denoise/sharpen,
   re-encodes H.264 High at CRF 20 with faststart. The original object is kept in the bucket as `sourceKey`. */
const MASTER_PREFIX='aml-suabhi/video-masters/';
const CURATED_ENHANCE={
  '7b2ae1aa-b4df-4479-901d-060b21b4842f':{cropTop:0.07,cropBottom:0.05,version:1},
  // Night car clip (576x1024 CapCut export): the editor watermark sits still in the top-left corner; rebuild it from the surrounding pixels.
  '92d4282e-ff06-4e52-a003-78ded6b9c28e':{delogo:{x:56,y:32,w:96,h:34},version:1},
  // Striped-shirt talk (464x832): recorded with the phone on its side, so the picture is stored lying down; turn it upright.
  '3c6678cc-e055-49a8-b05c-885345153370':{rotate:'ccw',version:1}
};
const enhanceQueue=[];let enhanceRunning=false,enhanceActive=null;
function isEnhancing(id){return enhanceActive===id||enhanceQueue.some(q=>q.itemId===id)}
// Options for one video: the curated recipe (if any) plus the quarter turns chosen in Studio (item.rotate, clockwise degrees).
function enhanceOptsFor(item){
  const base=CURATED_ENHANCE[item.id]||{};
  const baseDeg=base.rotate==='ccw'?270:base.rotate==='cw'?90:0;
  const opts={...base,version:Math.max(base.version||1,item.enhanceVersion||1),rotateDeg:(baseDeg+(Number(item.rotate)||0))%360};
  delete opts.rotate;
  return opts;
}
function enhanceFilter(opts){
  const top=Math.min(.3,Math.max(0,Number(opts.cropTop)||0)),bottom=Math.min(.3,Math.max(0,Number(opts.cropBottom)||0));
  const parts=[];
  const d=opts.delogo;
  if(d&&[d.x,d.y,d.w,d.h].every(n=>Number.isInteger(n)&&n>0))parts.push('delogo=x='+d.x+':y='+d.y+':w='+d.w+':h='+d.h);
  if(top||bottom)parts.push('crop=iw:ih*'+(1-top-bottom).toFixed(4)+':0:ih*'+top.toFixed(4));
  const deg=opts.rotateDeg!=null?Number(opts.rotateDeg):opts.rotate==='ccw'?270:opts.rotate==='cw'?90:0;
  if(deg===90)parts.push('transpose=1');else if(deg===180)parts.push('transpose=1','transpose=1');else if(deg===270)parts.push('transpose=2');
  parts.push('scale=\'min(1080,iw*4)\':-2:flags=lanczos','hqdn3d=1.5:1.5:4:4','unsharp=5:5:0.45:5:5:0.0','eq=contrast=1.03:saturation=1.04','format=yuv420p');
  return parts.join(',');
}
async function enhanceVideo(itemId,opts={}){
  const version=opts.version||1;
  // Record the attempt first: if the container dies mid-encode, the next boot will not retry forever.
  const item=await applyToLibraryVideo(itemId,null,it=>{it.enhanceAttempt=version});
  if(!item)throw new Error('Video not found');
  const sourceKey=item.sourceKey||item.key;
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'aml-enhance-'));
  const input=path.join(dir,'in'+(path.extname(sourceKey||'')||'.mov'));
  const output=path.join(dir,'out.mp4');
  const started=Date.now();
  try{
    const obj=await s3.send(new GetObjectCommand({Bucket:BUCKET,Key:sourceKey}));
    await pipeline(obj.Body,fs.createWriteStream(input));
    await runFfmpeg(['-y','-hide_banner','-loglevel','error','-i',input,'-vf',enhanceFilter(opts),'-r','30','-c:v','libx264','-preset',String(opts.preset||'medium'),'-crf',String(opts.crf||23),'-maxrate','2600k','-bufsize','5200k','-profile:v','high','-level','4.1','-pix_fmt','yuv420p','-c:a','aac','-b:a','128k','-ar','48000','-movflags','+faststart','-threads','2',output]);
    if(!fs.existsSync(output)||fs.statSync(output).size<50*1024)throw new Error('Encoded file missing or empty');
    const key=MASTER_PREFIX+itemId+'-'+Date.now().toString(36)+'.mp4';
    await new Upload({client:s3,params:{Bucket:BUCKET,Key:key,Body:fs.createReadStream(output),ContentType:'video/mp4',CacheControl:'public,max-age=31536000,immutable'}}).done();
    let previousMaster=null,previousPoster=null;
    const applied=await applyToLibraryVideo(itemId,null,it=>{
      if(!it.sourceKey)it.sourceKey=it.key;
      if(it.key!==it.sourceKey)previousMaster=it.key;
      previousPoster=it.posterKey||null;
      it.key=key;it.enhancedAt=new Date().toISOString();it.enhanceVersion=version;
      // The picture changed (crop + resolution): drop the old poster so a new one is cut from the master.
      it.posterKey=null;delete it.posterSecond;
    });
    if(!applied){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:key}))}catch{}throw new Error('Video removed during enhancement')}
    if(previousMaster&&previousMaster.startsWith(MASTER_PREFIX)){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:previousMaster}))}catch{}}
    if(previousPoster&&previousPoster.startsWith(POSTER_PREFIX)){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:previousPoster}))}catch{}}
    console.log('AML_VIDEO_ENHANCED '+JSON.stringify({id:itemId,key,bytes:fs.statSync(output).size,seconds:Math.round((Date.now()-started)/1000)}));
    scheduleVideoPosters(300);
    return decorateItem(applied);
  }finally{try{fs.rmSync(dir,{recursive:true,force:true})}catch{}}
}
function queueEnhance(itemId,opts,extra){
  if(enhanceQueue.some(q=>q.itemId===itemId))return;
  enhanceQueue.push({itemId,opts,...(extra||{})});
  if(enhanceRunning)return;
  enhanceRunning=true;
  (async()=>{
    while(enhanceQueue.length){
      const job=enhanceQueue.shift();
      enhanceActive=job.itemId;
      try{await enhanceVideo(job.itemId,job.opts)}
      catch(e){
        console.error('AML_VIDEO_ENHANCE_ERROR '+job.itemId+' '+(e?.message||e));
        // a failed turn must not leave the stored angle ahead of the actual picture
        if(job.revertRotate!=null){try{await applyToLibraryVideo(job.itemId,null,it=>{it.rotate=job.revertRotate})}catch{}}
      }
      finally{enhanceActive=null}
    }
  })().finally(()=>{enhanceRunning=false});
}
function enhancementStatus(itemId){return enhanceQueue.some(q=>q.itemId===itemId)?'queued':'idle'}
async function ensureCuratedEnhancements(){
  const meta=await readMeta();
  const library=videoLibraryOf(meta);
  for(const [id,opts] of Object.entries(CURATED_ENHANCE)){
    const item=(library?.items||[]).find(i=>i.id===id&&i.type==='video');
    if(!item)continue;
    if(item.enhancedAt&&(item.enhanceVersion||1)>=(opts.version||1))continue;
    // One automatic attempt per version: a failed or interrupted encode is reported in the logs, never looped.
    if((item.enhanceAttempt||0)>=(opts.version||1)){console.log('AML_VIDEO_ENHANCE_SKIPPED '+id+' previous attempt did not finish');continue}
    queueEnhance(id,enhanceOptsFor(item));
  }
}
async function regeneratePoster(itemId,second){
  const meta=await readMeta();
  const library=videoLibraryOf(meta);
  const item=(library?.items||[]).find(i=>i.id===itemId&&i.type==='video');
  if(!item)return null;
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'aml-posters-'));
  try{
    const r=await renderPosterFrame(item,second,dir);
    const old=item.posterKey;
    item.posterKey=r.posterKey;item.posterSecond=r.second;if(r.duration)item.duration=Math.round(r.duration*100)/100;
    await writeMeta(meta);
    if(old&&old!==r.posterKey&&old.startsWith(POSTER_PREFIX)){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:old}))}catch{}}
    return decorateItem(item);
  }finally{try{fs.rmSync(dir,{recursive:true,force:true})}catch{}}
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

  return {meta,changed};
}
async function readMeta(){
  if(metaCache)return JSON.parse(JSON.stringify(metaCache));
  const scoped=await readMetaKey(META_KEY);
  if(scoped){
    const hadHome=!!(scoped.home&&typeof scoped.home==='object');
    // Snapshot the metadata exactly as stored, before any in-place normalisation below.
    const prevRev=hadHome?(scoped.home.rev||1):0;
    const beforeJson=prevRev<HOME_REV?JSON.stringify(scoped):null;
    const ensured=ensureSystemCollections(scoped);
    const homeChanged=ensureHomeSettings(ensured.meta);
    if(homeChanged&&prevRev<HOME_REV){
      // Settings were introduced or migrated: keep a backup of the metadata exactly as it was stored before.
      const backupKey='aml-suabhi/backups/'+(hadHome?'pre-home-rev'+HOME_REV+'-':'pre-home-settings-')+new Date().toISOString().replace(/[:.]/g,'-')+'.json';
      try{await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:backupKey,Body:beforeJson,ContentType:'application/json',CacheControl:'no-cache'}));console.log('AML_HOME_SETTINGS_INIT backup='+backupKey)}catch(e){console.error('AML_HOME_SETTINGS_BACKUP_FAILED',e?.message||e)}
    }
    if(ensured.changed||homeChanged)await writeMeta(ensured.meta);
    return ensured.meta;
  }
  const legacy=await readMetaKey(LEGACY_META_KEY);
  if(!legacy){
    const ensured=ensureSystemCollections({version:1,collections:[]});
    ensureHomeSettings(ensured.meta);
    await writeMeta(ensured.meta);
    return ensured.meta;
  }
  await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:'aml-suabhi/backups/legacy-before-isolation-20261004.json',Body:JSON.stringify(legacy),ContentType:'application/json',CacheControl:'no-cache'}));
  const unexpected=new Set(['d532919e-eaaf-424c-829a-a235d96b6689','da57832a-b672-439c-82a7-0564d857c1de']);
  for(const c of legacy.collections||[]){c.items=(c.items||[]).filter(i=>!unexpected.has(i.id));if(unexpected.has(c.cover))c.cover=(c.items.find(i=>i.type==='image')||c.items[0]||{}).id||null}
  const ensured=ensureSystemCollections(legacy);
  ensureHomeSettings(ensured.meta);
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
  const items=c.items||[];
  const cover=(items.find(i=>i.id===c.cover)||items.find(i=>i.type==='image')||items[0]||null);
  return {
    id:c.id,name:c.name,createdAt:c.createdAt,systemRole:c.systemRole||null,locked:!!c.locked,
    cover:decorateItem(cover),
    items:items.map(decorateItem)
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
  if(pathname==='/api/home'&&req.method==='GET'){try{return json(res,200,resolveHome(await readMeta()))}catch(e){console.error(e);return json(res,500,{error:'Could not load home'})}}
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
  // Every admin mutation is a read-modify-write of the single metadata object: run them one at a time.
  if(req.method!=='GET')return withMetaLock(()=>handleAdmin(req,res,pathname,url));
  return handleAdmin(req,res,pathname,url);
}
async function handleAdmin(req,res,pathname,url){
  try{
    if(pathname==='/api/admin/move'&&req.method==='POST'){
      const b=await readJson(req);
      const ids=Array.isArray(b.itemIds)?b.itemIds:[];
      if(!ids.length||ids.length>100||ids.some(id=>typeof id!=='string')||new Set(ids).size!==ids.length)
        return json(res,400,{error:'Provide 1–100 distinct item IDs'});
      const meta=await readMeta();
      const from=meta.collections.find(c=>c.id===b.sourceCollection);
      const to=meta.collections.find(c=>c.id===b.destinationCollection);
      if(!from||!to)return json(res,404,{error:'Folder not found'});
      if(from===to||from.systemRole==='home_slideshow'||to.systemRole)
        return json(res,400,{error:'Choose a different work album'});
      const selected=new Set(ids);
      const moving=from.items.filter(i=>selected.has(i.id));
      if(ids.some(id=>!from.items.some(i=>i.id===id)&&!to.items.some(i=>i.id===id)))
        return json(res,409,{error:'Items changed; refresh and try again'});
      if(moving.some(i=>i.shared||to.items.some(t=>t.id===i.id)))
        return json(res,409,{error:'Duplicate or shared item'});
      const featured=new Set([meta.home?.heroVideoId,...(meta.home?.videoIds||[])]);
      if(moving.some(i=>featured.has(i.id)))
        return json(res,409,{error:'Remove the video from homepage selections before moving it'});
      if(!moving.length)return json(res,200,{ok:true,moved:0,alreadyMoved:ids.length});
      const backupKey='aml-suabhi/backups/pre-item-move-'+new Date().toISOString().replace(/[:.]/g,'-')+'-'+crypto.randomBytes(4).toString('hex')+'.json';
      await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:backupKey,Body:JSON.stringify(meta),ContentType:'application/json',CacheControl:'no-cache'}));
      from.items=from.items.filter(i=>!selected.has(i.id));
      to.items.push(...moving);
      if(selected.has(from.cover))from.cover=from.items.find(i=>i.type==='image')?.id||from.items[0]?.id||null;
      if(!to.cover)to.cover=to.items[0]?.id||null;
      await writeMeta(meta);
      return json(res,200,{ok:true,moved:moving.length,sourceCount:from.items.length,destinationCount:to.items.length});
    }
    if(pathname==='/api/admin/collections'&&req.method==='GET')return json(res,200,publicMeta(await readMeta()));
    if(pathname==='/api/admin/home'&&req.method==='GET')return json(res,200,resolveHome(await readMeta()));
    if(pathname==='/api/admin/home'&&(req.method==='PUT'||req.method==='POST')){
      const b=await readJson(req);
      const meta=await readMeta();
      ensureHomeSettings(meta);
      // First time the carousel/text controls are stored: keep a copy of the metadata as it was.
      const beforeControls=(('ringIds' in b)||('text' in b))&&!('ringIds' in meta.home)&&!('text' in meta.home)?JSON.stringify(meta):null;
      const library=videoLibraryOf(meta);
      const videoIdsAll=new Set((library?.items||[]).filter(i=>i.type==='video').map(i=>i.id));
      const imageIdsAll=new Set(workImagesOf(meta).map(i=>i.id));
      const errors=[];
      const h=meta.home;
      if('heroVideoId' in b){
        const id=b.heroVideoId==null?null:String(b.heroVideoId);
        if(id&&!videoIdsAll.has(id))errors.push('heroVideoId');else h.heroVideoId=id;
      }
      if('videoIds' in b){
        const ids=uniqStrings(b.videoIds,HOME_LIMITS.videos);
        if(ids.some(id=>!videoIdsAll.has(id)))errors.push('videoIds');else h.videoIds=ids.filter(id=>id!==h.heroVideoId);
      }
      if('photoIds' in b){
        const ids=uniqStrings(b.photoIds,HOME_LIMITS.photos);
        if(ids.some(id=>!imageIdsAll.has(id)))errors.push('photoIds');else h.photoIds=ids;
      }
      if('portraitId' in b){
        const id=b.portraitId==null?null:String(b.portraitId);
        if(id&&!imageIdsAll.has(id))errors.push('portraitId');else h.portraitId=id;
      }
      if('ringIds' in b){
        const ids=uniqStrings(b.ringIds,HOME_LIMITS.ring);
        if(ids.some(id=>!imageIdsAll.has(id)))errors.push('ringIds');else h.ringIds=ids;
      }
      if('text' in b){
        const t=sanitizeHomeText(b.text);
        if(t.errors.length)errors.push(...t.errors);else h.text=t.text;
      }
      if(errors.length)return json(res,400,{error:errors.some(e=>e.startsWith('text.'))?'Invalid text value':'Unknown media id',fields:errors});
      if(beforeControls){
        const backupKey='aml-suabhi/backups/pre-home-controls-'+new Date().toISOString().replace(/[:.]/g,'-')+'.json';
        try{await s3.send(new PutObjectCommand({Bucket:BUCKET,Key:backupKey,Body:beforeControls,ContentType:'application/json',CacheControl:'no-cache'}));console.log('AML_HOME_CONTROLS_INIT backup='+backupKey)}
        catch(e){console.error('AML_HOME_CONTROLS_BACKUP_FAILED',e?.message||e);return json(res,503,{error:'Could not back up settings, nothing was changed'})}
      }
      await writeMeta(meta);
      scheduleVideoPosters(200);
      return json(res,200,resolveHome(meta));
    }
    if(pathname==='/api/admin/rotate'&&req.method==='POST'){
      const b=await readJson(req);
      const turn=Number(b.degrees);
      if(![90,180,270].includes(turn))return json(res,400,{error:'Bad angle'});
      const meta=await readMeta();
      const it=(videoLibraryOf(meta)?.items||[]).find(i=>i.id===String(b.item||'')&&i.type==='video');
      if(!it)return json(res,404,{error:'Video not found'});
      if(isEnhancing(it.id))return json(res,409,{error:'Video is already being processed'});
      const previous=Number(it.rotate)||0;
      it.rotate=(previous+turn)%360;
      await writeMeta(meta);
      // The rebuilt file always comes from the untouched original, so repeated turns never stack quality loss.
      queueEnhance(it.id,enhanceOptsFor(it),{revertRotate:previous});
      console.log('AML_VIDEO_ROTATE_QUEUED '+it.id+' total='+it.rotate);
      return json(res,202,{ok:true,item:decorateItem(it)});
    }
    if(pathname==='/api/admin/poster'&&req.method==='POST'){
      const b=await readJson(req);
      const second=b.second==null?undefined:Number(b.second);
      if(second!=null&&!(Number.isFinite(second)&&second>=0&&second<3600))return json(res,400,{error:'Bad time'});
      const item=await regeneratePoster(String(b.item||''),second);
      if(!item)return json(res,404,{error:'Video not found'});
      return json(res,200,{ok:true,item});
    }
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
    if(item.type==='video')scheduleVideoPosters(1500);
    return json(res,201,{ok:true,item:decorateItem(item)})}
    if(pathname==='/api/admin/rename'&&req.method==='POST'){const b=await readJson(req);const name=safeName(b.name||'');const meta=await readMeta();const c=meta.collections.find(x=>x.id===b.collection);if(!c)return json(res,404,{error:'Folder not found'});if(c.locked)return json(res,409,{error:'System folder cannot be renamed'});c.name=name;await writeMeta(meta);return json(res,200,{ok:true,name})}
    if(pathname==='/api/admin/cover'&&req.method==='POST'){const b=await readJson(req);const meta=await readMeta();const c=meta.collections.find(x=>x.id===b.collection);if(!c||!c.items.some(i=>i.id===b.item))return json(res,404,{error:'Not found'});c.cover=b.item;await writeMeta(meta);return json(res,200,{ok:true})}
    if(pathname==='/api/admin/item'&&req.method==='DELETE'){const cid=url.searchParams.get('collection'),iid=url.searchParams.get('item');const meta=await readMeta();const c=meta.collections.find(x=>x.id===cid);if(!c)return json(res,404,{error:'Folder not found'});const i=c.items.findIndex(x=>x.id===iid);if(i<0)return json(res,404,{error:'Item not found'});const item=c.items[i];if(!item.shared)await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:item.key}));if(!item.shared&&item.sourceKey&&item.sourceKey!==item.key){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:item.sourceKey}))}catch{}}if(item.posterKey&&item.posterKey.startsWith(POSTER_PREFIX)){try{await s3.send(new DeleteObjectCommand({Bucket:BUCKET,Key:item.posterKey}))}catch{}}c.items.splice(i,1);if(c.cover===iid)c.cover=c.items.find(x=>x.type==='image')?.id||c.items[0]?.id||null;pruneHomeSettings(meta,[iid]);await writeMeta(meta);return json(res,200,{ok:true})}
    if(pathname==='/api/admin/collection'&&req.method==='DELETE'){const id=url.searchParams.get('id');const meta=await readMeta();const i=meta.collections.findIndex(x=>x.id===id);if(i<0)return json(res,404,{error:'Folder not found'});const c=meta.collections[i];if(c.locked)return json(res,409,{error:'System folder cannot be deleted'});if(c.items.length)await s3.send(new DeleteObjectsCommand({Bucket:BUCKET,Delete:{Objects:c.items.filter(x=>!x.shared).map(x=>({Key:x.key})),Quiet:true}}));meta.collections.splice(i,1);pruneHomeSettings(meta,c.items.map(x=>x.id));await writeMeta(meta);return json(res,200,{ok:true})}
  }catch(e){console.error(e);return json(res,500,{error:'Server error'})}
  return json(res,404,{error:'Not found'});
}

const server=http.createServer(async (req,res)=>{
  const url=new URL(req.url||'/', 'http://localhost');
  const pathname=decodeURIComponent(url.pathname);
  try{const handled=await handleApi(req,res,pathname,url);if(handled!==false)return}catch(e){console.error(e);return json(res,500,{error:'Server error'})}
  if(pathname==='/manage'||pathname==='/manage/'){return fs.readFile(path.join(root,'manage.html'),(err,data)=>{if(err){res.writeHead(500);return res.end('Manager unavailable')}res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store','X-Robots-Tag':'noindex, nofollow'});res.end(data)})}
  let filePath=pathname==='/'?'/index.html':pathname;const file=path.join(root,filePath.replace(/^\/+/,''));if(!file.startsWith(root)){res.writeHead(403);return res.end('Forbidden')}
  // Never serve application source, manifests, dependencies or dotfiles as static assets.
  if(/(^|[\\/])(\.[^\\/]*|node_modules|server\.js|package(-lock)?\.json)([\\/]|$)/i.test(path.relative(root,file))){res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});return res.end('Not found')}
  fs.readFile(file,(err,data)=>{if(err){fs.readFile(path.join(root,'index.html'),(e2,fallback)=>{if(e2){res.writeHead(404);return res.end('Not found')}sendHtml(res,fallback)});return}const ext=path.extname(file).toLowerCase();if(ext==='.html')return sendHtml(res,data);res.writeHead(200,{'Content-Type':types[ext]||'application/octet-stream','Cache-Control':'public,max-age=300'});res.end(data)})
});
function startBackgroundJobs(){
  // Order matters: integrity first, then posters for every video (fast), then the long hero enhancement.
  mediaIntegrityPromise=readMeta()
    .then(()=>runMediaIntegrityPass())
    .catch(e=>console.error('AML_MEDIA_INTEGRITY_ERROR',e?.stack||e?.message||e))
    .finally(()=>{
      mediaIntegrityPromise=null;
      scheduleVideoPosters(1200)
        .then(()=>ensureCuratedEnhancements())
        .catch(e=>console.error('AML_ENHANCE_INIT_ERROR',e?.message||e));
    });
  return mediaIntegrityPromise;
}
module.exports={server,handleApi,resolveHome,publicCollection,publicMeta,decorateItem,ensureHomeSettings,ensureSystemCollections,HOME_DEFAULTS,HOME_LIMITS,sanitizeHomeText,
  _test:{readMeta,startBackgroundJobs,enhanceFilter,enhanceOptsFor,withMetaLock,busy:()=>!!(mediaIntegrityPromise||posterRunPromise||enhanceRunning||enhanceQueue.length)}};
if(require.main===module){
  server.listen(port,'0.0.0.0',()=>console.log('AML portfolio listening on '+port));
  startBackgroundJobs();
}
