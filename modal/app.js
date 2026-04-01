var API_BASE = 'https://india20sixty.tommyhillary1.workers.dev';
// ── CONFIG — loaded from /config endpoint ──────────────────────
var R2_BASE_URL = '';
fetch(API_BASE + '/config').then(function(r){return r.json();}).then(function(d){
  R2_BASE_URL=d.r2_base_url||'';
}).catch(function(){});

// ── DATA ───────────────────────────────────────────────────────
var CATS = {
  AI:        {label:'AI & ML',         color:'#00e5ff', emoji:'\uD83E\uDD16'},
  Space:     {label:'Space & Defence', color:'#b388ff', emoji:'\uD83D\uDE80'},
  Gadgets:   {label:'Gadgets & Tech',  color:'#ffd740', emoji:'\uD83D\uDCF1'},
  DeepTech:  {label:'Deep Tech',       color:'#ff6b35', emoji:'\uD83D\uDD2C'},
  GreenTech: {label:'Green & Energy',  color:'#00e676', emoji:'\u26A1'},
  Startups:  {label:'Startups',        color:'#ff6b9d', emoji:'\uD83D\uDCA1'}
};
var VPD = {
  1: ['12:00 PM IST'],
  2: ['6:00 AM IST','6:00 PM IST'],
  3: ['6:00 AM IST','12:00 PM IST','6:00 PM IST']
};
var PROG = {pending:8,processing:35,images:55,voice:68,render:82,upload:93,staged:20,mixing:90,complete:100,test_complete:100,failed:0};
var PCOL = {pending:'#ffd740',processing:'#00e5ff',images:'#b388ff',voice:'#b388ff',render:'#ff6b35',upload:'#00e5ff',staged:'#ffd740',mixing:'#ff6b35',complete:'#00e676',test_complete:'#00e676',failed:'#ff5252'};
var BLBL = {pending:'Pending',processing:'Processing',images:'Images',voice:'Voice',render:'Rendering',upload:'Uploading',staged:'Staged',mixing:'Mixing',complete:'Complete',test_complete:'Complete',failed:'Failed'};
var CHAR = {natural:'No pitch shift',woman:'+4 st, formant +1.2',man:'-2 st deeper',elder:'-4 st, formant -0.8',child:'+9 st, formant +2.0',radio:'Heavy compression + reverb'};

var activeTab='all', allJobs=[], allTopics=[], allAnalytics=[], analyticsJobs=[];
var topicFilter='ready', currentPage='home', activeCat='all', topicCat='all';
var currentVoiceMode='ai', calDate=new Date(), calEvents=[];
var studioJob=null, mediaRecorder=null, audioChunks=[], recordedBlob=null;
var audioCtx=null, analyserNode=null, recTimer=null, recSecs=0;
var selectedMusic=null, selectedPreset='natural', playbackAudio=null, isRecording=false;

// ── UTILS ─────────────────────────────────────────────────────
function ago(iso){var s=Math.floor((Date.now()-new Date(iso))/1000);if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';if(s<86400)return Math.floor(s/3600)+'h';return Math.floor(s/86400)+'d';}
function fmt(n){if(!n)return'0';if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1000)return(n/1000).toFixed(1)+'K';return String(n);}
function scClass(s){return s>=80?'sc-hi':s>=60?'sc-med':'sc-lo';}
function badge(st){var s=st||'unknown';var dot=['pending','processing'].includes(s)?'<span class="bdot"></span>':'';return '<span class="badge b-'+s+'">'+dot+(BLBL[s]||s)+'</span>';}
function showDebug(id,html){var el=document.getElementById(id);if(el)el.innerHTML='<div class="debug-box">'+html+'</div>';}

// ── PAGE NAV ──────────────────────────────────────────────────
function showPage(name,btn){
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active');});
  document.querySelectorAll('.nav-btn').forEach(function(b){b.classList.remove('active');});
  document.getElementById('page-'+name).classList.add('active');
  btn.classList.add('active'); currentPage=name;
  if(name==='staging'){ renderStagingGrid(); }
  if(name==='review'){ loadCBDP(); }
  if(name==='library'){ loadLibrary(); }
  if(name==='calendar'){loadCalendar();renderCalendar();}
  if(name==='analytics') renderAnalytics();
  if(name==='topics') renderTopicsPage();
}

// ── CATEGORY STRIPS ───────────────────────────────────────────
function buildCatStrips(){
  var s=document.getElementById('cat-strip');
  var ts=document.getElementById('topic-cat-strip');
  var mc=document.getElementById('modal-cats');
  Object.keys(CATS).forEach(function(k){
    var cat=CATS[k];
    var p=document.createElement('div'); p.className='cat-pill'; p.dataset.cat=k;
    p.innerHTML=cat.emoji+' '+cat.label+' <span class="cat-count" id="cc-'+k+'">0</span>';
    p.onclick=function(){filterByCat(k,p);};
    s.appendChild(p);
    var p2=p.cloneNode(true); p2.onclick=function(){filterTopicsByCat(k,p2);};
    ts.appendChild(p2);
    var d=document.createElement('div'); d.className='cat-check selected'; d.dataset.cat=k;
    d.innerHTML='<span>'+cat.emoji+'</span><span>'+cat.label+'</span>';
    d.onclick=function(){d.classList.toggle('selected');};
    mc.appendChild(d);
  });
}

function filterByCat(cat,btn){
  activeCat=cat;
  document.querySelectorAll('#cat-strip .cat-pill').forEach(function(p){
    var isA=p.dataset.cat===cat;
    p.classList.toggle('active',isA);
    p.style.borderColor=isA?(cat!=='all'?CATS[cat].color:'var(--accent)'):'';
    p.style.color=isA?(cat!=='all'?CATS[cat].color:'var(--accent)'):'';
  });
  renderJobs();
}

function filterTopicsByCat(cat,btn){
  topicCat=cat;
  document.querySelectorAll('#topic-cat-strip .cat-pill').forEach(function(p){
    var isA=p.dataset.cat===cat;
    p.classList.toggle('active',isA);
    p.style.borderColor=isA?(cat!=='all'?CATS[cat].color:'var(--accent)'):'';
    p.style.color=isA?(cat!=='all'?CATS[cat].color:'var(--accent)'):'';
  });
  renderTopicsPage();
}

function switchTab(tab){
  activeTab=tab;
  document.querySelectorAll('.tab').forEach(function(t){t.classList.toggle('active',t.dataset.tab===tab);});
  renderJobs();
}

// ── JOBS ──────────────────────────────────────────────────────
function filterJobs(jobs,tab){
  var j=jobs;
  if(activeCat!=='all') j=j.filter(function(x){return x.cluster===activeCat;});
  if(tab==='running') return j.filter(function(x){return['pending','processing','images','voice','render','upload','staged','mixing'].includes(x.status);});
  if(tab==='complete') return j.filter(function(x){return x.status==='complete'||x.status==='test_complete';});
  if(tab==='failed') return j.filter(function(x){return x.status==='failed';});
  return j;
}

function renderJobs(){
  var el=document.getElementById('job-list');
  var jobs=filterJobs(allJobs,activeTab);
  if(!jobs.length){el.innerHTML='<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No jobs here.</div>';return;}
  el.innerHTML=jobs.map(function(j){
    var prog=PROG[j.status]||0,col=PCOL[j.status]||'#5a6278';
    var cat=CATS[j.cluster]||null;
    var catBadge=cat?'<span style="font-size:.58rem;color:'+cat.color+'">'+cat.emoji+' '+j.cluster+'</span>':'';
    var yt=j.youtube_id&&j.youtube_id!=='TEST_MODE'?'<a class="yt-link" href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank">&#9654; Watch</a>':(j.youtube_id==='TEST_MODE'?'<span style="color:var(--muted);font-size:.58rem;font-family:var(--mono)">test</span>':'');
    var err=j.error?'<span class="job-err" title="'+j.error+'">'+j.error.slice(0,35)+'</span>':'';
    return '<div class="job-item">'
      +'<div><div class="job-topic">'+(j.topic||'Untitled')+'</div>'
      +'<div class="job-meta">'+catBadge+(j.council_score?'<span>'+j.council_score+'</span>':'')+err+(yt?'<span>'+yt+'</span>':'')+'</div></div>'
      +'<div>'+badge(j.status)+'</div>'
      +'<div class="prog-wrap"><div class="prog-bar"><div class="prog-fill" style="width:'+prog+'%;background:'+col+'"></div></div><div class="prog-pct">'+prog+'%</div></div>'
      +'<div class="time-cell">'+(j.updated_at?ago(j.updated_at)+' ago':'-')+'</div>'
      +'</div>';
  }).join('');
}

async function loadJobs(){
  try{
    var r=await fetch(API_BASE + '/jobs'); allJobs=await r.json();
    var run=allJobs.filter(function(j){return['pending','processing','images','voice','render','upload','staged','mixing'].includes(j.status);});
    var ok=allJobs.filter(function(j){return j.status==='complete'||j.status==='test_complete';});
    var fail=allJobs.filter(function(j){return j.status==='failed';});
    document.getElementById('s-total').textContent=allJobs.length;
    document.getElementById('s-running').textContent=run.length;
    document.getElementById('s-complete').textContent=ok.length;
    document.getElementById('s-failed').textContent=fail.length;
    document.getElementById('tc-all').textContent=allJobs.length;
    document.getElementById('tc-run').textContent=run.length;
    document.getElementById('tc-ok').textContent=ok.length;
    document.getElementById('tc-fail').textContent=fail.length;
    document.getElementById('last-ref').textContent='Updated '+new Date().toLocaleTimeString();
    renderJobs();
  }catch(e){console.error('loadJobs:',e);}
}

async function loadQueue(){
  try{
    var r=await fetch(API_BASE + '/topics'); allTopics=await r.json();
    var ready=allTopics.filter(function(t){return !t.used&&t.council_score>=70;});
    document.getElementById('s-topics').textContent=ready.length;
    Object.keys(CATS).forEach(function(k){
      var el=document.getElementById('cc-'+k);
      if(el)el.textContent=ready.filter(function(t){return t.cluster===k;}).length;
    });
    var el=document.getElementById('queue-list');
    if(!ready.length){el.innerHTML='<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No topics. Click Replenish.</div>';return;}
    el.innerHTML=ready.slice(0,8).map(function(t){
      var cat=CATS[t.cluster]||null;
      return '<div class="topic-row"><div class="topic-text">'+t.topic+'</div>'
        +'<div class="topic-foot">'
        +'<span class="score-pill '+scClass(t.council_score)+'">'+t.council_score+'</span>'
        +'<span style="display:flex;gap:5px;align-items:center">'
        +(cat?'<span style="font-size:.65rem;color:'+cat.color+'">'+cat.emoji+' '+t.cluster+'</span>':'')
        +'<span class="src-tag">'+(t.source||'-')+'</span>'
        +'</span></div></div>';
    }).join('');
  }catch(e){console.error('loadQueue:',e);}
}

// ── ACTION BUTTONS ────────────────────────────────────────────
async function doCreateJob(){
  var btn=document.getElementById('bc'); btn.disabled=true; btn.textContent='Creating...';
  try{
    var r=await fetch(API_BASE + '/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
    var d=await r.json();
    if(d.error)throw new Error(d.error);
    switchTab('running'); loadJobs(); loadQueue();
    showDebug('debug-home','<span class="dg">Job created: '+d.topic+'</span>');
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false; btn.innerHTML='&#9654; Create Video';}
}

async function doGenerateTopic(){
  var btn=document.getElementById('bg'); btn.disabled=true; btn.textContent='Generating...';
  try{
    var topic=prompt('Topic idea:','');
    if(topic===null){btn.disabled=false;btn.innerHTML='&#10022; Generate Topic';return;}
    var r=await fetch(API_BASE + '/generate-topic',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:topic||'Future AI India'})});
    var d=await r.json();
    if(d.error)throw new Error(d.error);
    showDebug('debug-home',d.status==='approved'?'<span class="dg">Approved! Score: '+(d.evaluation&&d.evaluation.council_score?d.evaluation.council_score:'?')+'</span>':'<span class="dr">Rejected.</span>');
    loadQueue();
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;btn.innerHTML='&#10022; Generate Topic';}
}

async function doKillIncomplete(){
  var run=allJobs.filter(function(j){return['pending','processing','images','voice','render','upload'].includes(j.status);});
  if(!run.length){showDebug('debug-home','<span class="dg">No incomplete jobs.</span>');return;}
  if(!confirm('Kill '+run.length+' job(s)?'))return;
  var btn=document.getElementById('bk'); btn.disabled=true;
  try{
    var r=await fetch(API_BASE + '/kill-incomplete',{method:'POST'});
    var d=await r.json();
    showDebug('debug-home','<span class="dg">Killed '+d.killed+'. Restored: '+d.topics_restored+'</span>');
    setTimeout(function(){loadJobs();loadQueue();},600);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;}
}

async function doRestoreFailed(){
  var f=allJobs.filter(function(j){return j.status==='failed';});
  if(!f.length){showDebug('debug-home','<span class="dg">No failed jobs.</span>');return;}
  if(!confirm('Restore '+f.length+' jobs?'))return;
  var btn=document.getElementById('bf'); btn.disabled=true;
  try{
    var r=await fetch(API_BASE + '/restore-failed',{method:'POST'});
    var d=await r.json();
    showDebug('debug-home','<span class="dg">Restored '+d.restored+'.</span>');
    setTimeout(function(){loadJobs();loadQueue();},600);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;}
}

async function doTestRender(){
  var btn=document.getElementById('bt'); btn.disabled=true; btn.textContent='Testing...';
  try{
    var r=await fetch(API_BASE + '/test-render');
    var d=await r.json();
    showDebug('debug-home','<span class="dk">'+d.url+'</span><br><span class="'+(d.ok?'dg':'dr')+'">'+d.status+'</span> - '+(d.response||d.error||'-'));
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;btn.innerHTML='&#9741; Test Render';}
}

async function doSyncAnalytics(){
  try{await fetch(API_BASE + '/sync-analytics',{method:'POST'});showDebug('debug-home','<span class="dg">Sync started.</span>');setTimeout(loadAnalytics,8000);}
  catch(e){alert(e.message);}
}

// ── MODE TOGGLE (Full Auto / Stage) ──────────────────────────
var currentMode = 'auto'; // 'auto' or 'stage'
var stageImgSrc = 'library';
var stageVoiceSrc = 'ai';
var stageCategory = null;

async function loadMode(){
  try{
    var r=await fetch(API_BASE + '/config'); var d=await r.json();
    currentMode = d.mode || 'auto';
    setModeUI(currentMode);
  }catch(e){}
}

function setModeUI(mode){
  var tog=document.getElementById('mode-tog');
  var knb=document.getElementById('mode-knob');
  var lbl=document.getElementById('mode-lbl');
  var isAuto = mode === 'auto';
  if(tog) tog.style.background = isAuto ? 'var(--accent)' : 'var(--purple)';
  if(knb) knb.style.transform  = isAuto ? 'translateX(0)' : 'translateX(16px)';
  if(lbl){
    lbl.textContent = isAuto ? '\u26A1 FULL AUTO' : '\uD83C\uDF9B STAGE MODE';
    lbl.style.color = isAuto ? 'var(--accent)' : 'var(--purple, #b388ff)';
  }
}

async function toggleMode(){
  var newMode = currentMode === 'auto' ? 'stage' : 'auto';
  try{
    var r=await fetch(API_BASE + '/set-mode',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({mode:newMode})});
    var d=await r.json();
    currentMode = d.mode || newMode;
    setModeUI(currentMode);
    showDebug('debug-home', currentMode==='auto'
      ? '<span class="dg">\u26A1 Full Auto — pipeline runs end to end without intervention</span>'
      : '<span class="dk">\uD83C\uDF9B Stage Mode — you control images, voice, and review</span>');
  }catch(e){ alert('Failed: '+e.message); }
}

// ── CREATE VIDEO — branches on mode ──────────────────────────
async function doCreateJob(){
  if(currentMode === 'stage'){
    openStageModal();
    return;
  }
  // Full Auto — fire and forget
  var btn=document.getElementById('bc'); btn.disabled=true; btn.textContent='Creating...';
  try{
    var cat=currentCat!=='all'?currentCat:null;
    var r=await fetch(API_BASE + '/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({category:cat})});
    var d=await r.json();
    if(d.error)throw new Error(d.error);
    switchTab('running'); loadJobs(); loadQueue();
    showDebug('debug-home','<span class="dg">Auto job created: '+d.topic+'</span>');
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false; btn.innerHTML='&#9654; Create Video';}
}

// ── STAGE MODAL ───────────────────────────────────────────────
function openStageModal(){
  // Populate category strip
  var strip=document.getElementById('stage-cat-strip');
  if(strip){
    strip.innerHTML='<div class="cat-pill" data-cat="all" onclick="setStageCat(this.dataset.cat,this)" style="border-color:var(--accent);color:var(--accent)">All</div>'
      +Object.keys(CATS).map(function(k){
        var c=CATS[k];
        return '<div class="cat-pill" data-cat="'+k+'" onclick="setStageCat(this.dataset.cat,this)">'+c.emoji+' '+c.label+'</div>';
      }).join('');
  }
  stageCategory = null;
  // Reset selections
  selectImgSrc('library', document.getElementById('img-opt-library'));
  selectVoiceSrc('ai', document.getElementById('voice-opt-ai'));
  document.getElementById('stage-modal').classList.remove('hidden');
}

function closeStageModal(){
  document.getElementById('stage-modal').classList.add('hidden');
}

function selectImgSrc(src, el){
  stageImgSrc = src;
  document.querySelectorAll('#stage-modal .stage-opt[id^="img-"]').forEach(function(e){e.classList.remove('active');});
  if(el) el.classList.add('active');
}

function selectVoiceSrc(src, el){
  stageVoiceSrc = src;
  document.querySelectorAll('#stage-modal .stage-opt[id^="voice-"]').forEach(function(e){e.classList.remove('active');});
  if(el) el.classList.add('active');
}

function setStageCat(cat, el){
  stageCategory = cat === 'all' ? null : cat;
  document.querySelectorAll('#stage-cat-strip .cat-pill').forEach(function(p){p.style.borderColor='';p.style.color='';});
  if(el){el.style.borderColor='var(--accent)';el.style.color='var(--accent)';}
}

async function doStageCreate(){
  var btn=document.getElementById('stage-go-btn');
  btn.disabled=true; btn.textContent='Creating...';

  // If library selected, go to library tab to pick images
  if(stageImgSrc === 'library'){
    closeStageModal();
    btn.disabled=false; btn.innerHTML='&#9654; Create Video';
    showPage('library', document.querySelectorAll('.nav-btn')[3]);
    showDebug('debug-home','<span class="dk">Select 3 images from the library, then click Create Video</span>');
    return;
  }

  // If upload selected, trigger file picker
  if(stageImgSrc === 'upload'){
    closeStageModal();
    btn.disabled=false; btn.innerHTML='&#9654; Create Video';
    document.getElementById('lib-upload-input').click();
    return;
  }

  // Generate mode — create job, pipeline picks images via engine chain
  try{
    var body = {
      category: stageCategory,
      voice_mode: stageVoiceSrc,
      image_src: stageImgSrc
    };
    var r=await fetch(API_BASE + '/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    var d=await r.json();
    if(d.error)throw new Error(d.error);
    closeStageModal();
    switchTab('running'); loadJobs(); loadQueue();
    showDebug('debug-home','<span class="dg">Stage job created: '+d.topic+'</span>');
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false; btn.innerHTML='&#9654; Create Video';}
}

// ── SCHEDULE ──────────────────────────────────────────────────
async function loadSchedule(){
  try{
    var r=await fetch(API_BASE + '/get-schedule'); var d=await r.json(); var vpd=d.videos_per_day||1;
    document.querySelectorAll('.vpd-btn').forEach(function(b){
      var isA=b.id==='vpd-'+vpd;
      b.className='btn '+(isA?'btn-primary':'btn-ghost')+' vpd-btn';
    });
    document.getElementById('sched-times').textContent=(VPD[vpd]||[]).join('  \u2022  ');
    document.getElementById('sched-desc').textContent=vpd+' video'+(vpd>1?'s':'')+'/day';
  }catch(e){}
}
async function setVPD(n){
  try{
    var r=await fetch(API_BASE + '/set-schedule',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({videos_per_day:n})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    loadSchedule();
    showDebug('debug-home','<span class="dg">Schedule: '+n+' video'+(n>1?'s':'')+'/day</span>');
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
}

// ── REPLENISH MODAL ───────────────────────────────────────────
function openReplenishModal(){document.getElementById('rep-modal').classList.remove('hidden');}
function closeReplenishModal(){document.getElementById('rep-modal').classList.add('hidden');}
async function doReplenish(){
  var cats=[].slice.call(document.querySelectorAll('#modal-cats .cat-check.selected')).map(function(d){return d.dataset.cat;});
  var target=parseInt(document.getElementById('tgt-slider').value);
  closeReplenishModal();
  showDebug('debug-home','<span class="dk">Replenishing ['+cats.join(', ')+'] target '+target+'...</span>');
  try{
    var r=await fetch(API_BASE + '/replenish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({categories:cats,target:target})});
    var d=await r.json();
    showDebug('debug-home','<span class="dg">Replenish triggered.</span> '+JSON.stringify(d).slice(0,80));
    setTimeout(loadQueue,5000);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
}

// ── STAGING ───────────────────────────────────────────────────
var allStaged=[];
async function loadStaging(){
  try{
    var r=await fetch(API_BASE + '/staging'); allStaged=await r.json();
    var cnt=document.getElementById('stg-cnt'); if(cnt)cnt.textContent=allStaged.length;
    if(currentPage==='staging')renderStagingGrid();
  }catch(e){}
}
function renderStagingGrid(){
  var el=document.getElementById('staged-grid'); if(!el)return;
  if(!allStaged.length){
    el.innerHTML='<div class="empty"><span class="empty-icon">\uD83C\uDFAC</span>No staged videos.<br>'+(currentVoiceMode==='human'?'Create a video and it will appear here.':'Switch to Human Voice mode first.')+'</div>';
    return;
  }
  el.innerHTML=allStaged.map(function(j){
    var cat=CATS[j.cluster]||{color:'var(--muted)',emoji:'\uD83D\uDCF9',label:j.cluster||'?'};
    var scr=(j.script_package&&j.script_package.text)||'';
    var ageStr=j.created_at?ago(j.created_at)+' ago':'';
    return '<div class="staged-card" data-jobid="'+j.id+'" onclick="openStudio(this.dataset.jobid)">'
      +'<div class="staged-head">'
      +'<div class="staged-topic">'+(j.topic||'Untitled')+'</div>'
      +'<div class="staged-meta">'
      +'<span style="font-size:.68rem;color:'+cat.color+'">'+cat.emoji+' '+cat.label+'</span>'
      +'<span class="score-pill '+scClass(j.council_score||0)+'">'+(j.council_score||0)+'</span>'
      +'<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted)">'+ageStr+'</span>'
      +'</div></div>'
      +'<div class="staged-body">'+scr.slice(0,110)+(scr.length>110?'\u2026':'')+'</div>'
      +'<div class="staged-foot">'
      +'<span class="vm-badge vm-human">\uD83C\uDFA4 Needs Voice</span>'
      +'<span class="btn btn-primary" style="font-size:.68rem;padding:4px 11px">Open Studio \u2192</span>'
      +'</div></div>';
  }).join('');
}

// ── CBDP ─────────────────────────────────────────────────────
var currentStagingTab='staged';

// ── STUDIO ────────────────────────────────────────────────────
async function openStudio(jobId){
  studioJob=allStaged.find(function(j){return j.id===jobId;}); if(!studioJob)return;
  document.getElementById('stu-title').textContent=studioJob.topic||'Studio';
  document.getElementById('stu-id').textContent=jobId;
  document.getElementById('stu-script').textContent=(studioJob.script_package&&studioJob.script_package.text)||'No script';

  var vid=document.getElementById('stu-vid');
  // Use full public URL — video_public_url is enriched by /staging endpoint
  // Fall back to constructing from R2_BASE_URL + path
  var videoUrl=studioJob.video_public_url
    ||(R2_BASE_URL&&studioJob.video_r2_url?R2_BASE_URL+'/'+studioJob.video_r2_url:'');
  if(videoUrl){
    vid.src=videoUrl;
    vid.load();
    // Show a helpful message if video fails to load
    vid.onerror=function(){
      console.error('Studio video failed to load:',videoUrl);
      vid.style.display='none';
      var errEl=document.getElementById('stu-vid-err');
      if(errEl){errEl.style.display='flex';}
    };
    vid.oncanplay=function(){
      vid.style.display='';
      var errEl=document.getElementById('stu-vid-err');
      if(errEl){errEl.style.display='none';}
    };
  } else {
    vid.removeAttribute('src');
    var errEl=document.getElementById('stu-vid-err');
    if(errEl){errEl.style.display='flex';}
  }
  await loadMusicList(); resetRec();
  document.getElementById('studio').classList.remove('hidden');
  document.body.style.overflow='hidden';
}
function closeStudio(){
  document.getElementById('studio').classList.add('hidden');
  document.body.style.overflow='';
  stopRec(); if(playbackAudio){playbackAudio.pause();playbackAudio=null;} studioJob=null;
}
async function loadMusicList(){
  try{
    var r=await fetch(API_BASE + '/music-library'); var d=await r.json();
    var icons={Epic:'\u26A1',Hopeful:'\uD83C\uDF05',Tech:'\uD83D\uDCBB',Emotional:'\uD83D\uDCAB',Neutral:'\uD83C\uDFB5'};
    document.getElementById('music-list').innerHTML=d.tracks.map(function(t){
      return '<div class="music-track '+(selectedMusic===t.id?'selected':'')+'" data-tid="'+t.id+'" onclick="selectMusic(this.dataset.tid)">'
        +'<span>'+(icons[t.category]||'\uD83C\uDFB5')+'</span>'
        +'<div><div class="music-name">'+t.label+'</div><div class="music-cat">'+t.category+' \u00b7 '+t.duration+'s</div></div>'
        +'<span style="color:var(--green)">'+(selectedMusic===t.id?'\u2713':'')+'</span>'
        +'</div>';
    }).join('');
  }catch(e){document.getElementById('music-list').innerHTML='<div style="color:var(--muted);padding:8px;font-size:.75rem">Music unavailable</div>';}
}
function selectMusic(id){selectedMusic=id;loadMusicList();}
function setChar(el,preset){
  selectedPreset=preset;
  document.querySelectorAll('.char-btn').forEach(function(b){b.classList.remove('active');});
  el.classList.add('active');
  document.getElementById('char-desc').textContent=CHAR[preset]||'';
}

// ── RECORDER ─────────────────────────────────────────────────
async function startRec(){
  try{
    var stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true,sampleRate:44100}});
    audioCtx=new AudioContext({sampleRate:44100});
    var src=audioCtx.createMediaStreamSource(stream);
    analyserNode=audioCtx.createAnalyser(); analyserNode.fftSize=2048;
    var hpf=audioCtx.createBiquadFilter(); hpf.type='highpass'; hpf.frequency.value=80;
    var comp=audioCtx.createDynamicsCompressor(); comp.threshold.value=-24; comp.ratio.value=4; comp.attack.value=0.003; comp.release.value=0.25;
    var lim=audioCtx.createDynamicsCompressor(); lim.threshold.value=-3; lim.ratio.value=20; lim.attack.value=0.001; lim.release.value=0.1;
    src.connect(hpf); hpf.connect(comp); comp.connect(analyserNode); analyserNode.connect(lim); lim.connect(audioCtx.destination);
    drawWaveform();
    audioChunks=[]; mediaRecorder=new MediaRecorder(stream,{mimeType:'audio/webm'});
    mediaRecorder.ondataavailable=function(e){if(e.data.size>0)audioChunks.push(e.data);};
    mediaRecorder.onstop=function(){
      recordedBlob=new Blob(audioChunks,{type:'audio/webm'});
      document.getElementById('rec-ply').disabled=false;
      document.getElementById('rec-rst').disabled=false;
      document.getElementById('rec-status').textContent='\u2713 Recorded ('+Math.round(recordedBlob.size/1024)+'KB)';
      document.getElementById('rec-status').className='rec-status';
      clearInterval(recTimer);
    };
    mediaRecorder.start(100); isRecording=true; recSecs=0;
    recTimer=setInterval(function(){recSecs++;var m=Math.floor(recSecs/60),s=recSecs%60;document.getElementById('rec-dur').textContent=m+':'+(s<10?'0':'')+s;},1000);
    document.getElementById('rec-rec').disabled=true; document.getElementById('rec-stp').disabled=false;
    document.getElementById('rec-status').textContent='\u25CF RECORDING...'; document.getElementById('rec-status').className='rec-status recording';
  }catch(e){alert('Microphone error: '+e.message);}
}
function stopRec(){
  if(mediaRecorder&&mediaRecorder.state!=='inactive'){mediaRecorder.stop();mediaRecorder.stream.getTracks().forEach(function(t){t.stop();});}
  isRecording=false; document.getElementById('rec-rec').disabled=false; document.getElementById('rec-stp').disabled=true;
}
function playRec(){
  if(!recordedBlob)return;
  if(playbackAudio){playbackAudio.pause();playbackAudio=null;document.getElementById('rec-ply').textContent='\u25B6';return;}
  playbackAudio=new Audio(URL.createObjectURL(recordedBlob)); playbackAudio.play();
  document.getElementById('rec-ply').textContent='\u23F8';
  playbackAudio.onended=function(){document.getElementById('rec-ply').textContent='\u25B6';playbackAudio=null;};
}
function resetRec(){
  stopRec(); if(playbackAudio){playbackAudio.pause();playbackAudio=null;}
  audioChunks=[]; recordedBlob=null; recSecs=0;
  document.getElementById('rec-rec').disabled=false;
  document.getElementById('rec-stp').disabled=true; document.getElementById('rec-ply').disabled=true; document.getElementById('rec-rst').disabled=true;
  document.getElementById('rec-status').textContent='Ready'; document.getElementById('rec-status').className='rec-status';
  document.getElementById('rec-dur').textContent='0:00';
  var c=document.getElementById('waveform'); if(c){var ctx2=c.getContext('2d');ctx2.clearRect(0,0,c.width,c.height);}
}
function drawWaveform(){
  if(!analyserNode)return;
  var canvas=document.getElementById('waveform'); var ctx2=canvas.getContext('2d');
  var W=canvas.width=canvas.offsetWidth; var H=canvas.height;
  var buf=new Uint8Array(analyserNode.frequencyBinCount);
  function draw(){
    if(!isRecording)return; requestAnimationFrame(draw);
    analyserNode.getByteTimeDomainData(buf);
    ctx2.fillStyle='rgba(13,19,32,0.4)'; ctx2.fillRect(0,0,W,H);
    ctx2.lineWidth=1.5; ctx2.strokeStyle='#00e5ff'; ctx2.beginPath();
    var step=W/buf.length;
    for(var i=0;i<buf.length;i++){var y=(buf[i]/128.0)*(H/2);i===0?ctx2.moveTo(0,y):ctx2.lineTo(i*step,y);}
    ctx2.stroke();
  }
  draw();
}
function previewMix(){
  var vid=document.getElementById('stu-vid');
  if(vid&&vid.src){
    vid.currentTime=0;
    vid.play().catch(function(e){console.warn('Preview play failed:',e);});
  }
  if(recordedBlob){
    if(playbackAudio){playbackAudio.pause();playbackAudio=null;}
    playbackAudio=new Audio(URL.createObjectURL(recordedBlob));
    playbackAudio.play();
  }
}

// ── PUBLISH ───────────────────────────────────────────────────
async function doPublish(publishAt){
  if(!studioJob){alert('No job open');return;}
  if(!recordedBlob){alert('Please record your voice first');return;}
  var sEl=document.getElementById('pub-status');
  var n=document.getElementById('pub-now'); var s=document.getElementById('pub-sch');
  n.disabled=s.disabled=true; sEl.textContent='\u23F3 Uploading voice...'; sEl.style.color='var(--yellow)';
  try{
    var ur=await fetch(API_BASE + '/upload-voice?job_id='+studioJob.id,{method:'POST',body:recordedBlob,headers:{'Content-Type':'audio/webm'}});
    if(!ur.ok)throw new Error('Upload failed: '+ur.status);
    sEl.textContent='\u23F3 Starting mix...';
    var mr=await fetch(API_BASE + '/mix',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      job_id:studioJob.id,music_track:selectedMusic||'neutral_01',
      music_volume:(parseInt(document.getElementById('mus-vol').value)||8)/100,
      publish_at:publishAt||null,
      voice_offset_ms:parseInt(document.getElementById('voice-off').value)||0
    })});
    if(!mr.ok)throw new Error('Mix failed: '+mr.status);
    sEl.textContent='\u2713 '+(publishAt?'Scheduled!':'Publishing soon!');
    sEl.style.color='var(--green)';
    allStaged=allStaged.filter(function(j){return j.id!==studioJob.id;}); renderStagingGrid();
    setTimeout(closeStudio,2000);
  }catch(e){sEl.textContent='\u2717 '+e.message;sEl.style.color='var(--red)';}
  finally{n.disabled=s.disabled=false;}
}
function publishNow(){doPublish(null);}
function publishScheduled(){var dt=document.getElementById('pub-at').value;if(!dt){alert('Pick a date/time first');return;}doPublish(new Date(dt).toISOString());}

// ── CALENDAR ─────────────────────────────────────────────────
async function loadCalendar(){
  try{var r=await fetch(API_BASE + '/calendar');calEvents=await r.json();if(currentPage==='calendar')renderCalendar();}catch(e){}
}
function renderCalendar(){
  var el=document.getElementById('cal-grid'); if(!el)return;
  var y=calDate.getFullYear(),m=calDate.getMonth();
  document.getElementById('cal-lbl').textContent=calDate.toLocaleDateString('en-IN',{month:'long',year:'numeric'});
  var first=new Date(y,m,1).getDay(),days=new Date(y,m+1,0).getDate();
  var today=new Date(); var html='';
  for(var i=0;i<first;i++)html+='<div class="cal-cell" style="opacity:.1"></div>';
  for(var d=1;d<=days;d++){
    var isToday=today.getDate()===d&&today.getMonth()===m&&today.getFullYear()===y;
    var evts=calEvents.filter(function(e){var ed=new Date(e.scheduled_at||e.created_at);return ed.getFullYear()===y&&ed.getMonth()===m&&ed.getDate()===d;});
    var evHtml=evts.map(function(e){
      var cat=CATS[e.cluster]||{color:'var(--accent)'};
      var t=e.scheduled_at?new Date(e.scheduled_at).toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'}):'';
      return '<div class="cal-evt" style="background:'+cat.color+'22;color:'+cat.color+'">'+(t?t+' ':'')+((e.topic||'').slice(0,14))+'</div>';
    }).join('');
    html+='<div class="cal-cell '+(isToday?'today':'')+'"><div class="cal-dn" style="'+(isToday?'color:var(--accent);font-weight:700':'')+'">'+d+'</div>'+evHtml+'</div>';
  }
  el.innerHTML=html;
}
function calPrev(){calDate=new Date(calDate.getFullYear(),calDate.getMonth()-1,1);renderCalendar();}
function calNext(){calDate=new Date(calDate.getFullYear(),calDate.getMonth()+1,1);renderCalendar();}
function calToday(){calDate=new Date();renderCalendar();}

// ── ANALYTICS ────────────────────────────────────────────────
async function loadAnalytics(){
  try{var r=await fetch(API_BASE + '/analytics');var d=await r.json();allAnalytics=d.analytics||[];analyticsJobs=d.jobs||[];if(currentPage==='analytics')renderAnalytics();}catch(e){}
}
function renderAnalytics(){
  var rows=allAnalytics;
  if(!rows.length){['a-views','a-likes','a-comments','a-avg'].forEach(function(id){var e=document.getElementById(id);if(e)e.textContent='-';});document.getElementById('video-grid').innerHTML='<div class="empty">\uD83D\uDCCA No analytics yet.</div>';document.getElementById('perf-list').innerHTML='';document.getElementById('flop-list').innerHTML='';return;}
  document.getElementById('a-views').textContent=fmt(rows.reduce(function(s,r){return s+(r.youtube_views||0);},0));
  document.getElementById('a-likes').textContent=fmt(rows.reduce(function(s,r){return s+(r.youtube_likes||0);},0));
  document.getElementById('a-comments').textContent=fmt(rows.reduce(function(s,r){return s+(r.comment_count||0);},0));
  document.getElementById('a-avg').textContent=fmt(rows.length?Math.round(rows.reduce(function(s,r){return s+(r.score||0);},0)/rows.length):0);
  document.getElementById('a-count').textContent=rows.length+' videos';
  var sorted=[].concat(rows).sort(function(a,b){return b.score-a.score;});
  document.getElementById('video-grid').innerHTML=sorted.map(function(r){
    var job=analyticsJobs.find(function(j){return j.id===r.video_id;})||{};
    var hasYt=job.youtube_id&&job.youtube_id!=='TEST_MODE';
    return '<div class="video-card"><div class="video-thumb">\uD83C\uDFAC</div><div class="video-body">'
      +'<div class="video-topic">'+(job.topic||'Unknown')+'</div>'
      +'<div class="video-stats"><span>\uD83D\uDC41 <b>'+fmt(r.youtube_views||0)+'</b></span><span>\u2764 <b>'+fmt(r.youtube_likes||0)+'</b></span></div>'
      +'<div class="video-score">'+fmt(r.score||0)+'</div>'
      +(hasYt?'<a class="video-link" href="https://youtube.com/watch?v='+job.youtube_id+'" target="_blank">&#9654; Watch</a>':'')
      +'</div></div>';
  }).join('');
  function perfRow(r){var j=analyticsJobs.find(function(x){return x.id===r.video_id;})||{};return '<div class="perf-row"><div class="perf-topic">'+(j.topic||'-')+'</div><div class="perf-num pn-views">'+fmt(r.youtube_views||0)+'</div><div class="perf-num pn-likes">'+fmt(r.youtube_likes||0)+'</div><div class="perf-num pn-score">'+fmt(r.score||0)+'</div></div>';}
  document.getElementById('perf-list').innerHTML=sorted.slice(0,5).map(perfRow).join('')||'<div class="empty" style="padding:16px">No data</div>';
  var withV=rows.filter(function(r){return r.youtube_views>0;});
  document.getElementById('flop-list').innerHTML=[].concat(withV).sort(function(a,b){return a.score-b.score;}).slice(0,5).map(perfRow).join('')||'<div class="empty" style="padding:16px">No data</div>';
}

// ── TOPICS PAGE ───────────────────────────────────────────────
function filterTopics(f){
  topicFilter=f;
  ['all','ready','used'].forEach(function(k){var b=document.getElementById('bt-'+k);if(b)b.className='btn '+(k===f?'btn-primary':'btn-ghost');});
  renderTopicsPage();
}
function renderTopicsPage(){
  var topics=allTopics;
  if(topicFilter==='ready')topics=topics.filter(function(t){return !t.used&&t.council_score>=70;});
  if(topicFilter==='used')topics=topics.filter(function(t){return t.used;});
  if(topicCat!=='all')topics=topics.filter(function(t){return t.cluster===topicCat;});
  document.getElementById('topics-count').textContent=topics.length+' topics';
  var el=document.getElementById('topics-list');
  if(!topics.length){el.innerHTML='<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No topics.</div>';return;}
  el.innerHTML=topics.map(function(t){
    var cat=CATS[t.cluster]||null;
    var canGen=!t.used&&t.council_score>=70;
    return '<div class="topic-row"><div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px">'
      +'<div style="flex:1;min-width:0"><div class="topic-text">'+t.topic+'</div>'
      +'<div class="topic-foot">'
      +'<span class="score-pill '+scClass(t.council_score)+'">'+t.council_score+'</span>'
      +'<span style="display:flex;gap:7px;align-items:center">'
      +'<span class="used-pill '+(t.used?'':'used-no')+'">'+(t.used?'Used':'Ready')+'</span>'
      +(cat?'<span style="font-size:.65rem;color:'+cat.color+'">'+cat.emoji+' '+cat.label+'</span>':'')
      +'<span class="src-tag">'+(t.source||'-')+'</span>'
      +'</span></div></div>'
      +(canGen
        ?'<button class="btn btn-primary" style="font-size:.68rem;padding:5px 12px;flex-shrink:0;white-space:nowrap" '
         +'data-tid="'+t.id+'" onclick="generateNow(this.dataset.tid,this)">\u25B6 Generate Now</button>'
        :'')
      +'</div></div>';
  }).join('');
}

// ── CBDP REVIEW QUEUE ────────────────────────────────────────
var allReview=[];

async function loadCBDP(){
  try{
    var r=await fetch(API_BASE + '/review'); var data=await r.json();
    allReview=Array.isArray(data)?data:[];
    var rc=document.getElementById('rev-cnt'); if(rc)rc.textContent=allReview.length;
    var rc2=document.getElementById('cbdp-count'); if(rc2)rc2.textContent=allReview.length;
    renderReviewGrid();
  }catch(e){console.error('loadCBDP:',e);}
}

function renderReviewGrid(){
  var el=document.getElementById('cbdp-grid'); if(!el)return;
  if(!allReview||!allReview.length){
    el.innerHTML='<div class="empty" style="grid-column:1/-1">'
      +'<span class="empty-icon">\uD83C\uDFAC</span>'
      +'No videos in review queue.<br>'
      +'<span style="color:var(--muted);font-size:.8rem">'
      +'Videos land here when PUBLISH is OFF, or when YouTube upload fails after a successful render.'
      +'</span></div>';
    return;
  }
  try{
    el.innerHTML=allReview.map(function(j){
      var cat=CATS[j.cluster]||{color:'var(--muted)',emoji:'\uD83D\uDCF9',label:j.cluster||'?'};
      var scr=(j.script_package&&j.script_package.text)||'';
      var title=(j.script_package&&j.script_package.title)||j.topic||'Untitled';
      var age=j.updated_at?ago(j.updated_at)+' ago':'';
      var reason=j.review_reason||'Ready for review';
      var hasVideo=!!(j.has_video&&j.video_public_url);
      var videoUrl=j.video_public_url||'';
      var statusColor=j.status==='review'?'var(--accent)':'var(--yellow)';
      var statusLabel=j.status==='review'?'REVIEW':'CBDP';
      return '<div class="staged-card" style="cursor:default">'
        +'<div class="staged-head">'
        +'<div class="staged-topic" style="font-size:.85rem">'+(title||'Untitled')+'</div>'
        +'<div class="staged-meta">'
        +'<span style="font-size:.65rem;color:'+cat.color+'">'+cat.emoji+' '+cat.label+'</span>'
        +'<span class="score-pill '+scClass(j.council_score||0)+'">'+(j.council_score||0)+'</span>'
        +'<span style="font-size:.6rem;font-weight:600;color:'+statusColor+'">'+statusLabel+'</span>'
        +'<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted)">'+age+'</span>'
        +'</div></div>'
        +'<div style="padding:5px 12px;background:var(--surface2);font-family:var(--mono);font-size:.6rem;color:var(--muted);border-bottom:0.5px solid var(--border)">'+reason+'</div>'
        +(hasVideo
          ?'<video src="'+videoUrl+'" controls preload="metadata" style="width:100%;max-height:220px;background:#000;display:block"></video>'
           +'<div style="text-align:center;padding:4px 0;border-bottom:0.5px solid var(--border)">'
           +'<a href="'+videoUrl+'" target="_blank" style="font-family:var(--mono);font-size:.62rem;color:var(--accent);text-decoration:none">\u25B6 Open in new tab</a>'
           +'</div>'
          :'<div style="background:var(--surface2);height:64px;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:3px">'
           +'<span style="font-size:.72rem;color:var(--muted)">\uD83D\uDCF9 No video file saved</span>'
           +'<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted)">This job failed before R2 save — reject and re-run</span>'
           +'</div>')
        +'<div class="staged-body" style="font-size:.72rem;line-height:1.6;color:var(--text-muted)">'
        +(scr?scr.slice(0,140)+(scr.length>140?'\u2026':''):'<span style="color:var(--muted)">No script saved</span>')
        +'</div>'
        +'<div class="staged-foot" style="gap:6px">'
        +(hasVideo
          ?'<button class="btn btn-primary" style="flex:2;font-size:.72rem" data-jid="'+j.id+'" onclick="publishCBDP(this.dataset.jid,this)">\uD83D\uDE80 Publish to YouTube</button>'
          :'<button class="btn" style="flex:2;font-size:.72rem;opacity:.4;cursor:not-allowed" disabled>\uD83D\uDE80 No video — reject &amp; retry</button>')
        +'<button class="btn btn-red" style="flex:1;font-size:.72rem" data-jid="'+j.id+'" onclick="rejectCBDP(this.dataset.jid,this)">\u2715 Reject</button>'
        +'</div></div>';
    }).join('');
  }catch(err){
    console.error('renderReviewGrid error:',err);
    el.innerHTML='<div class="empty" style="grid-column:1/-1"><span class="empty-icon">\u26A0</span>Error: '+err.message+'</div>';
  }
}

async function publishCBDP(jobId,btn){
  // Find the job to check if it's staged (silent) or has audio already
  var job=allReview.find(function(j){return j.id===jobId;});
  var isStagedSilent = job && job.status==='staged';
  var confirmMsg = isStagedSilent
    ? 'This video has no audio. Add AI voice and publish to YouTube?'
    : 'Publish this video to YouTube now?';
  if(!confirm(confirmMsg))return;
  btn.disabled=true;
  btn.textContent=isStagedSilent?'\uD83C\uDFA4 Adding voice...':'\u23F3 Publishing...';
  try{
    var endpoint = isStagedSilent
      ? API_BASE + '/add-voice-and-publish'
      : API_BASE + '/publish-job';
    var r=await fetch(endpoint,{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({job_id:jobId})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    btn.textContent=isStagedSilent?'\u2713 Voice generating...':'\u2713 Sent!';
    btn.style.background='var(--green)';
    allReview=allReview.filter(function(j){return j.id!==jobId;});
    var rc=document.getElementById('rev-cnt'); if(rc)rc.textContent=allReview.length;
    setTimeout(function(){renderReviewGrid();loadJobs();},1500);
  }catch(e){
    btn.textContent='\uD83D\uDE80 Publish';
    btn.disabled=false;
    alert('Publish failed: '+e.message);
  }
}

async function rejectCBDP(jobId,btn){
  if(!confirm('Reject this video? The topic will return to queue for reuse.'))return;
  btn.disabled=true; btn.textContent='\u23F3...';
  try{
    var r=await fetch(API_BASE + '/reject-job',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({job_id:jobId})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    allReview=allReview.filter(function(j){return j.id!==jobId;});
    var rc=document.getElementById('rev-cnt'); if(rc)rc.textContent=allReview.length;
    renderReviewGrid(); loadQueue();
  }catch(e){btn.textContent='\u2715 Reject';btn.disabled=false;alert('Reject failed: '+e.message);}
}

// ── GENERATE NOW (from Topics page) ──────────────────────────
async function generateNow(topicId,btn){
  if(!confirm('Generate a video from this topic right now?'))return;
  btn.disabled=true; btn.textContent='\u23F3 Creating...';
  try{
    var r=await fetch(API_BASE + '/run-topic',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({topic_id:topicId})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    btn.textContent='\u2713 Job created!'; btn.style.color='var(--green)';
    showDebug('debug-home','<span class="dg">Video job created from topic: '+d.topic+'</span>');
    setTimeout(function(){loadJobs();loadQueue();renderTopicsPage();},800);
  }catch(e){btn.textContent='\u25B6 Generate Now';btn.disabled=false;alert('Failed: '+e.message);}
}

// ── IMAGE LIBRARY ─────────────────────────────────────────────
var allImages=[], selectedImages=[], libTopicFilter='all';

async function uploadLibImages(input){
  var files=Array.from(input.files);
  if(!files.length)return;
  var btn=input.parentElement;
  var orig=btn.innerHTML;
  btn.style.color='var(--yellow)';

  var topic=prompt('Tag these images with a topic name (used for filtering):','uploaded');
  if(!topic)topic='uploaded';

  var ok=0, fail=0;
  for(var i=0;i<files.length;i++){
    var f=files[i];
    btn.innerHTML='\u23F3 '+f.name.slice(0,20)+'... ('+(i+1)+'/'+files.length+')';
    try{
      var r=await fetch(API_BASE + '/upload-image?topic='+encodeURIComponent(topic)+'&filename='+encodeURIComponent(f.name),{
        method:'POST',
        headers:{'Content-Type':f.type||'image/png'},
        body:f
      });
      var d=await r.json();
      if(d.error)throw new Error(d.error);
      ok++;
    }catch(e){
      console.error('Upload failed:',f.name,e);
      fail++;
    }
  }
  btn.innerHTML=orig; btn.style.color='';
  input.value=''; // reset file input
  var msg='\u2713 Uploaded '+ok+' image'+(ok!==1?'s':'');
  if(fail) msg+=' (\u2717 '+fail+' failed)';
  showDebug('debug-home','<span class="dg">'+msg+'</span>');
  loadLibrary();
}

async function loadLibrary(){
  var el=document.getElementById('lib-grid');
  if(el) el.innerHTML='<div style="grid-column:1/-1;text-align:center;padding:24px;color:var(--muted);font-size:.8rem">\u23F3 Loading images...</div>';
  try{
    var r=await fetch(API_BASE + '/image-library'); var d=await r.json();
    allImages=Array.isArray(d.images)?d.images:[];
    var lc=document.getElementById('lib-count'); if(lc)lc.textContent=allImages.length;
    buildLibFilter();
    renderLibrary();
  }catch(e){
    console.error('loadLibrary:',e);
    if(el)el.innerHTML='<div style="grid-column:1/-1;text-align:center;padding:24px;color:var(--red)">\u26A0 Failed to load: '+e.message+'</div>';
  }
}

function buildLibFilter(){
  var topics=[...new Set(allImages.map(function(i){return i.topic||'unknown';}))].filter(Boolean);
  var el=document.getElementById('lib-filter'); if(!el)return;
  el.innerHTML='<div class="cat-pill" data-topic="all" onclick="filterLib(this.dataset.topic,this)"'
    +(libTopicFilter==='all'?' style="border-color:var(--accent);color:var(--accent)"':'')
    +'>All ('+allImages.length+')</div>'
    +topics.slice(0,12).map(function(t){
      var count=allImages.filter(function(i){return i.topic===t;}).length;
      return '<div class="cat-pill" data-topic="'+t.replace(/"/g,'&quot;')+'" onclick="filterLib(this.dataset.topic,this)">'+t.slice(0,25)+' ('+count+')</div>';
    }).join('');
}

function filterLib(topic,el){
  libTopicFilter=topic;
  document.querySelectorAll('#lib-filter .cat-pill').forEach(function(p){p.style.borderColor='';p.style.color='';});
  if(el){el.style.borderColor='var(--accent)';el.style.color='var(--accent)';}
  renderLibrary();
}

function renderLibrary(){
  var el=document.getElementById('lib-grid'); if(!el)return;
  var imgs=libTopicFilter==='all'?allImages:allImages.filter(function(i){return i.topic===libTopicFilter;});
  if(!imgs.length){
    el.innerHTML='<div style="grid-column:1/-1;text-align:center;padding:40px;color:var(--muted)">'
      +'\uD83D\uDDBC No images yet.<br>'
      +'<span style="font-size:.75rem">Images are saved automatically when you create a video.</span>'
      +'</div>';
    return;
  }
  el.innerHTML=imgs.map(function(img,idx){
    var sel=selectedImages.indexOf(img.url)>-1;
    var selIdx=selectedImages.indexOf(img.url);
    return '<div class="lib-img-card'+(sel?' lib-selected':'')+'" '
      +'data-imgurl="'+img.url.replace(/"/g,'&quot;')+'" onclick="toggleLibImage(this)" '
      +'style="position:relative;cursor:pointer;border-radius:8px;overflow:hidden;border:2px solid '+(sel?'var(--accent)':'transparent')+'">'
      +'<img src="'+img.url+'" loading="lazy" '
      +'style="width:100%;aspect-ratio:9/16;object-fit:cover;display:block" '
      +'onerror="this.parentElement.style.display=\'none\'">'
      +(sel?'<div style="position:absolute;top:6px;right:6px;background:var(--accent);color:#000;border-radius:50%;width:22px;height:22px;display:flex;align-items:center;justify-content:center;font-size:.75rem;font-weight:700">'+(selIdx+1)+'</div>':'')
      +'<div style="position:absolute;bottom:0;left:0;right:0;padding:5px 7px;background:linear-gradient(transparent,rgba(0,0,0,.8));font-size:.58rem;color:rgba(255,255,255,.8)">'
      +img.topic.slice(0,30)
      +'</div>'
      +'</div>';
  }).join('');
}

function toggleLibImage(el){
  var url=el.dataset.imgurl;
  var idx=selectedImages.indexOf(url);
  if(idx>-1){
    selectedImages.splice(idx,1);
  } else {
    if(selectedImages.length>=3){
      alert('Select exactly 3 images. Deselect one first.');
      return;
    }
    selectedImages.push(url);
  }
  var sc=document.getElementById('lib-sel-count');
  if(sc)sc.textContent=selectedImages.length+' / 3 selected';
  var btn=document.getElementById('lib-create-btn');
  if(btn){btn.disabled=selectedImages.length!==3;btn.style.opacity=selectedImages.length===3?'1':'.4';}
  renderLibrary();
}

async function createVideoFromLibrary(){
  if(selectedImages.length!==3){alert('Select exactly 3 images first.');return;}
  var btn=document.getElementById('lib-create-btn');
  btn.disabled=true; btn.textContent='\u23F3 Creating...';
  try{
    var r=await fetch(API_BASE + '/run-with-images',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({image_urls:selectedImages})});
    var d=await r.json();
    if(d.error)throw new Error(d.error);
    selectedImages=[];
    renderLibrary();
    var sc=document.getElementById('lib-sel-count'); if(sc)sc.textContent='0 / 3 selected';
    btn.textContent='\u2713 Job created!'; btn.style.color='var(--green)';
    showDebug('debug-home','<span class="dg">Video job created from library images</span>');
    setTimeout(function(){loadJobs();showPage('home',document.querySelector('.nav-btn'));},1200);
  }catch(e){
    btn.textContent='\u25B6 Create Video'; btn.disabled=false; btn.style.opacity='1';
    alert('Failed: '+e.message);
  }
}

// ── INIT ─────────────────────────────────────────────────────
buildCatStrips();
function loadAll(){loadJobs();loadQueue();loadAnalytics();loadMode();loadSchedule();loadStaging();loadCBDP();loadCalendar();}
loadAll();
setInterval(function(){loadJobs();loadQueue();loadStaging();loadCBDP();if(currentPage==='analytics')loadAnalytics();if(currentPage==='calendar')renderCalendar();},6000);