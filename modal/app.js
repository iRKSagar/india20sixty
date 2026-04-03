// ============================================================
// India20Sixty — Dashboard app.js v4.0
// Clean rewrite matching new nav structure:
// Home | Create | Queue | Library | Calendar | Analytics | Topics | Settings
// ============================================================

var API_BASE = 'https://india20sixty.tommyhillary1.workers.dev';
var R2_BASE_URL = '';

// ── CONSTANTS ─────────────────────────────────────────────────
var CATS = {
  AI:        { label:'AI & ML',         color:'#00e5ff', emoji:'\uD83E\uDD16' },
  Space:     { label:'Space & Defence', color:'#b388ff', emoji:'\uD83D\uDE80' },
  Gadgets:   { label:'Gadgets & Tech',  color:'#ffd740', emoji:'\uD83D\uDCF1' },
  DeepTech:  { label:'Deep Tech',       color:'#ff6b35', emoji:'\uD83D\uDD2C' },
  GreenTech: { label:'Green & Energy',  color:'#00e676', emoji:'\u26A1' },
  Startups:  { label:'Startups',        color:'#ff6b9d', emoji:'\uD83D\uDCA1' },
};
var VPD_SCHED = {
  1: ['12:00 PM IST'],
  2: ['6:00 AM IST','6:00 PM IST'],
  3: ['6:00 AM IST','12:00 PM IST','6:00 PM IST'],
};
var PROG = {
  pending:8, processing:35, images:55, voice:68, render:82, upload:93,
  staged:20, mixing:90, manual_pending:5,
  complete:100, test_complete:100, failed:0,
  review:100, cbdp:100,
};
var PCOL = {
  pending:'#ffd740', processing:'#00e5ff', images:'#b388ff', voice:'#b388ff',
  render:'#ff6b35', upload:'#00e5ff', staged:'#ffd740', mixing:'#ff6b35',
  manual_pending:'#ffd740',
  complete:'#00e676', test_complete:'#00e676', failed:'#ff5252',
  review:'#b388ff', cbdp:'#b388ff',
};
var BLBL = {
  pending:'Pending', processing:'Processing', images:'Images', voice:'Voice',
  render:'Rendering', upload:'Uploading', staged:'Staged', mixing:'Mixing',
  manual_pending:'Manual — Waiting',
  complete:'Complete', test_complete:'Complete', failed:'Failed',
  review:'In Review', cbdp:'CBDP',
};
var MOOD_COLORS = {
  cinematic_epic:'#b388ff', breaking_news:'#00e5ff', hopeful_future:'#00e676',
  dark_serious:'#ff5252', cold_tech:'#60b4ff', vibrant_pop:'#ffd740',
  nostalgic_film:'#ff6b35', warm_human:'#ff6b9d',
};

// ── STATE ──────────────────────────────────────────────────────
var allJobs = [], allTopics = [], allAnalytics = [], analyticsJobs = [];
var allStaged = [], allReview = [], allManual = [];
var allImages = [], selectedImages = [], libTopicFilter = 'all';
var libSelectedImages2 = [];
var topicFilter = 'ready', topicCat = 'all';
var activeTab = 'all', currentPage = 'home';
var activeCat = 'all', autoCat = null, manualCat = null;
var currentQueueTab = 'voice';
var currentCreateTab = 'auto';
var calDate = new Date(), calEvents = [];
var studioJob = null, mediaRecorder = null, audioChunks = [], recordedBlob = null;
var audioCtx = null, analyserNode = null, recTimer = null, recSecs = 0;
var selectedMusic = null, playbackAudio = null, isRecording = false;

// ── UTILS ──────────────────────────────────────────────────────
function esc(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;');
}
function ago(iso) {
  var s = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s / 60) + 'm';
  if (s < 86400) return Math.floor(s / 3600) + 'h';
  return Math.floor(s / 86400) + 'd';
}
function fmt(n) {
  if (!n) return '0';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}
function scClass(s) { return s >= 80 ? 'sc-hi' : s >= 60 ? 'sc-med' : 'sc-lo'; }
function badge(st) {
  var s = st || 'unknown';
  var dot = ['pending','processing','voice','render'].includes(s) ? '<span class="bdot"></span>' : '';
  return '<span class="badge b-' + s + '">' + dot + (BLBL[s] || s) + '</span>';
}
function moodBadge(mood, label) {
  if (!mood) return '';
  var col = MOOD_COLORS[mood] || 'var(--accent)';
  return '<span class="mood-badge" style="color:' + col + ';border-color:' + col + '22">'
    + (label || mood) + '</span>';
}
function showDebug(id, html) {
  var el = document.getElementById(id);
  if (el) el.innerHTML = '<div class="debug-box">' + html + '</div>';
}
function clearDebug(id) {
  var el = document.getElementById(id);
  if (el) el.innerHTML = '';
}

// ── CONFIG ─────────────────────────────────────────────────────
fetch(API_BASE + '/config')
  .then(function(r) { return r.json(); })
  .then(function(d) { R2_BASE_URL = d.r2_base_url || ''; })
  .catch(function() {});

// Update live clock
setInterval(function() {
  var el = document.getElementById('live-time');
  if (el) el.textContent = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}, 1000);

// ── PAGE NAV ───────────────────────────────────────────────────
function showPage(name, btn) {
  document.querySelectorAll('.page').forEach(function(p) { p.classList.remove('active'); });
  document.querySelectorAll('.nav-btn').forEach(function(b) { b.classList.remove('active'); });
  document.getElementById('page-' + name).classList.add('active');
  if (btn) btn.classList.add('active');
  currentPage = name;
  if (name === 'queue')     { loadQueue_panel(); }
  if (name === 'library')   { loadLibrary(); }
  if (name === 'calendar')  { loadCalendar(); renderCalendar(); }
  if (name === 'analytics') { renderAnalytics(); }
  if (name === 'topics')    { renderTopicsPage(); }
  if (name === 'settings')  { loadSettings(); }
}

// ── CATEGORY STRIPS ────────────────────────────────────────────
function buildCatStrips() {
  // Auto create strip
  var as = document.getElementById('auto-cat-strip');
  if (as) {
    as.innerHTML = '<div class="cat-pill active" data-cat="null" onclick="setAutoCat(null,this)" style="border-color:var(--accent);color:var(--accent)">Any</div>'
      + Object.keys(CATS).map(function(k) {
          var c = CATS[k];
          return '<div class="cat-pill" data-cat="' + k + '" onclick="setAutoCat(\'' + k + '\',this)">'
            + c.emoji + ' ' + c.label + '</div>';
        }).join('');
  }
  // Manual cat strip
  var ms = document.getElementById('manual-cat-strip');
  if (ms) {
    ms.innerHTML = Object.keys(CATS).map(function(k) {
      var c = CATS[k];
      return '<div class="cat-pill" data-cat="' + k + '" onclick="setManualCat(\'' + k + '\',this)">'
        + c.emoji + ' ' + c.label + '</div>';
    }).join('');
  }
  // Topics cat strip
  var ts = document.getElementById('topic-cat-strip');
  if (ts) {
    ts.innerHTML = '<div class="cat-pill active" data-cat="all" onclick="setTopicCat(\'all\',this)" style="border-color:var(--accent);color:var(--accent)">All</div>'
      + Object.keys(CATS).map(function(k) {
          var c = CATS[k];
          return '<div class="cat-pill" data-cat="' + k + '" onclick="setTopicCat(\'' + k + '\',this)">'
            + c.emoji + ' ' + c.label + '</div>';
        }).join('');
  }
  // Replenish modal cats
  var mc = document.getElementById('modal-cats');
  if (mc) {
    mc.innerHTML = Object.keys(CATS).map(function(k) {
      var c = CATS[k];
      return '<div class="cat-check selected" data-cat="' + k + '" onclick="this.classList.toggle(\'selected\')">'
        + '<span>' + c.emoji + '</span><span>' + c.label + '</span></div>';
    }).join('');
  }
}

function setAutoCat(cat, el) {
  autoCat = cat;
  document.querySelectorAll('#auto-cat-strip .cat-pill').forEach(function(p) {
    p.classList.remove('active');
    p.style.borderColor = ''; p.style.color = '';
  });
  if (el) {
    el.classList.add('active');
    var col = cat ? (CATS[cat] && CATS[cat].color) || 'var(--accent)' : 'var(--accent)';
    el.style.borderColor = col; el.style.color = col;
  }
}
function setManualCat(cat, el) {
  manualCat = cat;
  document.querySelectorAll('#manual-cat-strip .cat-pill').forEach(function(p) {
    p.classList.remove('active'); p.style.borderColor = ''; p.style.color = '';
  });
  if (el) {
    el.classList.add('active');
    var col = (CATS[cat] && CATS[cat].color) || 'var(--accent)';
    el.style.borderColor = col; el.style.color = col;
  }
}
function setTopicCat(cat, el) {
  topicCat = cat;
  document.querySelectorAll('#topic-cat-strip .cat-pill').forEach(function(p) {
    p.classList.remove('active'); p.style.borderColor = ''; p.style.color = '';
  });
  if (el) {
    el.classList.add('active');
    var col = cat === 'all' ? 'var(--accent)' : (CATS[cat] && CATS[cat].color) || 'var(--accent)';
    el.style.borderColor = col; el.style.color = col;
  }
  renderTopicsPage();
}

// ── CREATE TABS ─────────────────────────────────────────────────
function switchCreateTab(name, el) {
  currentCreateTab = name;
  document.querySelectorAll('.create-tab').forEach(function(t) { t.classList.remove('active'); });
  document.querySelectorAll('.create-panel').forEach(function(p) { p.classList.remove('active'); });
  if (el) el.classList.add('active');
  var panel = document.getElementById('cpanel-' + name);
  if (panel) panel.classList.add('active');
  if (name === 'library')  loadLibrary();
  if (name === 'longform') loadLongformJobs();
  if (name === 'manual')   initManualPanel();
}

// ── QUEUE TABS ──────────────────────────────────────────────────
function switchQueueTab(name) {
  currentQueueTab = name;
  ['voice','review','manual'].forEach(function(n) {
    var tab = document.getElementById('qtab-' + n);
    var panel = document.getElementById('qpanel-' + n);
    if (tab)  tab.classList.toggle('active', n === name);
    if (panel) panel.style.display = n === name ? '' : 'none';
  });
}

// ── JOB TABS (Home) ─────────────────────────────────────────────
function switchTab(tab) {
  activeTab = tab;
  document.querySelectorAll('#page-home .tab').forEach(function(t) {
    t.classList.toggle('active', t.dataset.tab === tab);
  });
  renderJobs();
}

// ── JOBS ────────────────────────────────────────────────────────
function filterJobs(jobs, tab) {
  if (tab === 'running') return jobs.filter(function(x) {
    return ['pending','processing','images','voice','render','upload','staged','mixing'].includes(x.status);
  });
  if (tab === 'complete') return jobs.filter(function(x) {
    return x.status === 'complete' || x.status === 'test_complete';
  });
  if (tab === 'failed') return jobs.filter(function(x) { return x.status === 'failed'; });
  return jobs;
}

var ACTIVE_STATUSES = ['pending','processing','images','voice','render','upload'];

function renderJobs() {
  var el = document.getElementById('job-list');
  if (!el) return;
  var active = allJobs.filter(function(j) { return ACTIVE_STATUSES.includes(j.status); });
  if (!active.length) {
    el.innerHTML = '<div style="text-align:center;padding:28px;color:var(--muted);font-family:var(--mono);font-size:.75rem">'
      + '&#10003; All clear — no active jobs</div>';
    return;
  }
  el.innerHTML = active.map(function(j) {
    var prog = PROG[j.status] || 0;
    var col  = PCOL[j.status] || '#5a6278';
    var cat  = CATS[j.cluster] || null;
    var catBadge = cat ? '<span style="font-size:.58rem;color:' + cat.color + '">' + cat.emoji + ' ' + j.cluster + '</span>' : '';
    var pkg  = j.script_package || {};
    var mood = pkg.mood ? moodBadge(pkg.mood, pkg.mood_label) : '';
    var elapsed = j.started_at ? '<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted)">' + ago(j.started_at) + '</span>' : '';
    return '<div class="job-item" id="jrow-' + j.id + '">'
      + '<div style="flex:1;min-width:0">'
      + '<div class="job-topic">' + esc(j.topic || 'Untitled') + '</div>'
      + '<div class="job-meta">' + catBadge + mood + elapsed + '</div>'
      + '</div>'
      + '<div style="display:flex;align-items:center;gap:6px;flex-shrink:0">'
      + badge(j.status)
      + '<div class="prog-wrap" style="width:70px"><div class="prog-bar"><div class="prog-fill" style="width:' + prog + '%;background:' + col + '"></div></div></div>'
      + '<button class="btn btn-red btn-sm" style="padding:4px 8px;font-size:.7rem" onclick="killJob(\'' + j.id + '\')" title="Kill this job">&#10005;</button>'
      + '</div>'
      + '</div>';
  }).join('');
}

async function loadJobs() {
  try {
    var r = await fetch(API_BASE + '/jobs');
    allJobs = await r.json();
    var run  = allJobs.filter(function(j) { return ACTIVE_STATUSES.includes(j.status); });
    var ok   = allJobs.filter(function(j) {
      var d = new Date(j.updated_at || j.created_at);
      var today = new Date(); today.setHours(0,0,0,0);
      return (j.status === 'complete' || j.status === 'test_complete') && d >= today;
    });
    var fail = allJobs.filter(function(j) { return j.status === 'failed'; });
    setText('s-total',   allJobs.length);
    setText('s-running', run.length);
    setText('s-complete',ok.length);
    setText('last-ref', 'Updated ' + new Date().toLocaleTimeString());
    // Failed badge
    var fb = document.getElementById('failed-badge');
    if (fb) {
      if (fail.length > 0) {
        fb.textContent = '\u25CF ' + fail.length + ' failed';
        fb.style.display = '';
      } else {
        fb.style.display = 'none';
      }
    }
    renderJobs();
  } catch(e) { console.error('loadJobs:', e); }
}

async function loadTopicsCount() {
  try {
    var r = await fetch(API_BASE + '/topics');
    allTopics = await r.json();
    var ready = allTopics.filter(function(t) { return !t.used && t.council_score >= 70; });
    var space = allTopics.filter(function(t) { return !t.used && t.council_score >= 70 && t.cluster === 'Space'; });
    setText('s-topics', ready.length);
    setText('s-space',  space.length);
    renderQueuePanel();
  } catch(e) {}
}

function setText(id, val) {
  var el = document.getElementById(id);
  if (el) el.textContent = val;
}

function renderQueuePanel() {
  var el = document.getElementById('queue-list'); if (!el) return;
  var ready = allTopics.filter(function(t) { return !t.used && t.council_score >= 70; });
  if (!ready.length) {
    el.innerHTML = '<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No topics. Click Replenish.</div>';
    return;
  }
  el.innerHTML = ready.slice(0, 6).map(function(t) {
    var cat = CATS[t.cluster] || null;
    return '<div class="topic-row"><div class="topic-text">' + t.topic + '</div>'
      + '<div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px">'
      + '<span class="score-pill ' + scClass(t.council_score) + '">' + t.council_score + '</span>'
      + (cat ? '<span style="font-size:.62rem;color:' + cat.color + '">' + cat.emoji + ' ' + t.cluster + '</span>' : '')
      + '</div></div>';
  }).join('');
}

// ── SERVICE HEALTH ──────────────────────────────────────────────
async function loadHealth() {
  try {
    var r = await fetch(API_BASE + '/service-health');
    var d = await r.json();

    var modalOk = d.modal && d.modal.ok;
    setText('h-modal', modalOk ? ('v' + (d.modal.version || '?') + ' \u2713') : '\u2717 down');
    var mEl = document.getElementById('h-modal');
    if (mEl) mEl.className = 'health-val ' + (modalOk ? 'hv-ok' : 'hv-err');

    var cOk = d.topic_council && d.topic_council.ok;
    setText('h-council', cOk ? ('Queue: ' + (d.topic_council.queue_depth || 0) + ' \u2713') : '\u2717 sleeping');
    var cEl = document.getElementById('h-council');
    if (cEl) cEl.className = 'health-val ' + (cOk ? 'hv-ok' : 'hv-warn');

    setText('h-topics', d.topics_ready != null ? d.topics_ready + ' ready' : '-');
    setText('h-jobs-today', d.jobs_today != null ? d.jobs_today + ' triggered' : '-');
    setText('h-published', d.complete_today != null ? d.complete_today + ' live' : '-');

    // Settings page mirrors
    setText('sh-modal', modalOk ? ('v' + (d.modal.version || '?') + ' \u2713') : '\u2717 down');
    var smEl = document.getElementById('sh-modal');
    if (smEl) smEl.className = 'health-val ' + (modalOk ? 'hv-ok' : 'hv-err');
    setText('sh-council', cOk ? '\u2713 online' : '\u26A0 sleeping (cold start ~30s)');
    var scEl = document.getElementById('sh-council');
    if (scEl) scEl.className = 'health-val ' + (cOk ? 'hv-ok' : 'hv-warn');
    setText('sh-topics', d.topics_ready != null ? d.topics_ready + ' ready' : '-');

  } catch(e) {
    console.error('loadHealth:', e);
    ['h-modal','h-council'].forEach(function(id) { setText(id, 'error'); });
  }
}

// ── CREATE — AUTO ───────────────────────────────────────────────
async function doAutoCreate() {
  var btn = document.getElementById('auto-create-btn');
  btn.disabled = true; btn.textContent = 'Creating...';
  clearDebug('debug-create');
  try {
    var body = {};
    if (autoCat) body.category = autoCat;
    var r = await fetch(API_BASE + '/run', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showDebug('debug-create', '<span class="dg">&#10003; Job created: ' + d.topic + '</span>');
    switchTab('running'); loadJobs(); loadTopicsCount();
    showPage('home', document.querySelector('.nav-btn'));
  } catch(e) {
    showDebug('debug-create', '<span class="dr">&#10007; ' + e.message + '</span>');
  } finally {
    btn.disabled = false; btn.innerHTML = '&#9654; Create Video';
  }
}

// ── CREATE — MANUAL ─────────────────────────────────────────────
// ============================================================
// MANUAL CREATE — step-progressive, topic-driven
// ============================================================

var manualState = {
  topicSrc:    'queue',   // 'queue' | 'custom'
  topicId:     null,
  topic:       '',
  cluster:     'AI',
  script:      '',
  scriptPkg:   null,
  visualMode:  localStorage.getItem('m_visual') || 'images',  // 'images' | 'video'
  voiceMode:   localStorage.getItem('m_voice')  || 'ai',      // 'ai' | 'record' | 'upload'
  imgUrls:     [null, null, null],   // uploaded/generated image URLs
  videoUrl:    null,                 // uploaded video R2 URL
  voiceUrl:    null,                 // uploaded voice R2 URL
  manualCat:   null,
};

function initManualPanel() {
  setManualTopicSrc(manualState.topicSrc);
  setManualVisualMode(manualState.visualMode);
  setManualVoiceMode(manualState.voiceMode);
  loadManualQueueTopics(null);
  buildManualCatStrip();
}

function buildManualCatStrip() {
  var el = document.getElementById('manual-cat-strip');
  if (!el) return;
  el.innerHTML = '<div class="cat-pill active" onclick="loadManualQueueTopics(null,this)" style="border-color:var(--accent);color:var(--accent)">All</div>'
    + Object.keys(CATS).map(function(k) {
        var c = CATS[k];
        return '<div class="cat-pill" onclick="loadManualQueueTopics(\'' + k + '\',this)">' + c.emoji + ' ' + c.label + '</div>';
      }).join('');
}

function setManualTopicSrc(src) {
  manualState.topicSrc = src;
  document.getElementById('mts-queue').className  = 'btn btn-sm' + (src==='queue'  ? ' btn-primary' : '');
  document.getElementById('mts-custom').className = 'btn btn-sm' + (src==='custom' ? ' btn-primary' : '');
  document.getElementById('mts-queue-panel').style.display  = src === 'queue'  ? '' : 'none';
  document.getElementById('mts-custom-panel').style.display = src === 'custom' ? '' : 'none';
  if (src === 'queue') loadManualQueueTopics(manualState.manualCat);
}

var _manualTopicsCache = [];

async function loadManualQueueTopics(cat, el) {
  manualState.manualCat = cat;
  if (el) {
    document.querySelectorAll('#manual-cat-strip .cat-pill').forEach(function(p) {
      p.classList.remove('active'); p.style.borderColor=''; p.style.color='';
    });
    el.classList.add('active');
    if (cat) { el.style.borderColor=CATS[cat]?.color||'var(--accent)'; el.style.color=CATS[cat]?.color||'var(--accent)'; }
    else      { el.style.borderColor='var(--accent)'; el.style.color='var(--accent)'; }
  }
  var container = document.getElementById('manual-topic-cards');
  if (!container) return;
  container.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-family:var(--mono);font-size:.72rem">Loading...</div>';
  try {
    var topics = allTopics.filter(function(t) {
      return !t.used && t.council_score >= 70 && (!cat || t.cluster === cat);
    }).sort(function(a,b) { return b.council_score - a.council_score; }).slice(0,20);
    _manualTopicsCache = topics;
    if (!topics.length) {
      container.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-family:var(--mono);font-size:.72rem">No topics in queue. Replenish first.</div>';
      return;
    }
    container.innerHTML = topics.map(function(t, i) {
      var cat2 = CATS[t.cluster] || null;
      var hasScript = !!(t.script_package && t.script_package.text);
      return '<div onclick="selectManualTopicByIdx(' + i + ')" style="'
        + 'padding:10px 12px;background:var(--bg);border:1px solid var(--border);border-radius:8px;cursor:pointer;transition:border-color .15s;margin-bottom:4px"'
        + ' onmouseover="this.style.borderColor=\'var(--accent)\'" onmouseout="this.style.borderColor=\'var(--border)\'">'
        + '<div style="font-size:.78rem;font-weight:600;margin-bottom:3px">' + esc(t.topic) + '</div>'
        + '<div style="display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:.6rem;color:var(--muted)">'
        + (cat2 ? '<span style="color:' + cat2.color + '">' + cat2.emoji + ' ' + t.cluster + '</span>' : '')
        + '<span>Score: ' + t.council_score + '</span>'
        + (hasScript ? '<span style="color:var(--green)">&#10003; Script ready</span>' : '<span style="color:var(--yellow)">&#9888; No script</span>')
        + '</div>'
        + '</div>';
    }).join('');
  } catch(e) {
    container.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:8px">Error: ' + e.message + '</div>';
  }
}

function selectManualTopicByIdx(idx) {
  var t = _manualTopicsCache[idx];
  if (!t) return;
  selectManualTopic(t.id, t.topic, t.cluster, t.council_score, t.script_package);
}

function selectManualTopic(id, topic, cluster, score, scriptPkg) {
  manualState.topicId   = id;
  manualState.topic     = topic;
  manualState.cluster   = cluster;
  manualState.scriptPkg = scriptPkg;

  document.getElementById('m-sel-name').textContent = topic;
  document.getElementById('m-sel-meta').textContent = (CATS[cluster]?.emoji||'') + ' ' + cluster + ' · Score: ' + score;
  document.getElementById('m-selected-topic').style.display = '';

  // Pre-fill script from script_package if available
  var script = (scriptPkg && scriptPkg.text) ? scriptPkg.text : '';
  document.getElementById('m-script').value = script;
  manualState.script = script;
  updateManualWordCount();

  // Pre-fill image prompts if available
  var prompts = (scriptPkg && scriptPkg.scene_prompts) ? scriptPkg.scene_prompts : ['','',''];
  ['m-img-p1','m-img-p2','m-img-p3'].forEach(function(id2, i) {
    var el = document.getElementById(id2);
    if (el) el.value = prompts[i] || '';
  });

  // Reveal steps
  document.getElementById('m-step-script').style.display  = '';
  document.getElementById('m-step-visuals').style.display = '';
  document.getElementById('m-step-voice').style.display   = '';
  document.getElementById('m-step-actions').style.display = '';
  updateManualSummary();
}

function clearManualTopic() {
  manualState.topicId = null; manualState.topic = ''; manualState.scriptPkg = null;
  document.getElementById('m-selected-topic').style.display = 'none';
  document.getElementById('m-step-script').style.display    = 'none';
  document.getElementById('m-step-visuals').style.display   = 'none';
  document.getElementById('m-step-voice').style.display     = 'none';
  document.getElementById('m-step-actions').style.display   = 'none';
}

async function generateManualScript() {
  var topic = (document.getElementById('m-custom-topic').value || '').trim();
  if (!topic) { alert('Enter a topic first'); return; }
  manualState.topic = topic; manualState.topicId = null; manualState.cluster = 'AI';
  document.getElementById('m-sel-name').textContent = topic;
  document.getElementById('m-sel-meta').textContent = 'Custom topic — script generating...';
  document.getElementById('m-selected-topic').style.display = '';
  document.getElementById('m-step-script').style.display = '';
  document.getElementById('m-script').value = 'Generating script...';
  try {
    var r = await fetch(API_BASE + '/generate-topic', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ topic })
    });
    var d = await r.json();
    var script = (d.script_package && d.script_package.text) || (d.text) || '';
    if (script) {
      document.getElementById('m-script').value = script;
      manualState.script = script;
      manualState.scriptPkg = d.script_package || null;
      var prompts = (d.script_package && d.script_package.scene_prompts) || ['','',''];
      ['m-img-p1','m-img-p2','m-img-p3'].forEach(function(id2, i) {
        var el = document.getElementById(id2);
        if (el) el.value = prompts[i] || '';
      });
    } else {
      document.getElementById('m-script').value = '';
    }
    updateManualWordCount();
    document.getElementById('m-step-visuals').style.display = '';
    document.getElementById('m-step-voice').style.display   = '';
    document.getElementById('m-step-actions').style.display = '';
    document.getElementById('m-sel-meta').textContent = 'Custom topic';
    updateManualSummary();
  } catch(e) {
    document.getElementById('m-script').value = '';
    alert('Script generation failed: ' + e.message);
  }
}

function updateManualWordCount() {
  var script = document.getElementById('m-script').value || '';
  var words  = script.trim().split(/\s+/).filter(Boolean).length;
  var secs   = Math.round(words * 0.45);
  var el     = document.getElementById('m-wc');
  if (el) {
    el.textContent = words + ' words · ~' + secs + 's';
    el.style.color = words > 65 ? 'var(--red)' : words > 55 ? 'var(--yellow)' : 'var(--green)';
  }
  manualState.script = script;
  updateManualSummary();
}

function copyManualScript() {
  var script = document.getElementById('m-script').value || '';
  navigator.clipboard.writeText(script).catch(function() {});
}

async function regenManualScript() {
  if (!manualState.topic) return;
  document.getElementById('m-script').value = 'Regenerating...';
  try {
    var r = await fetch(API_BASE + '/generate-topic', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ topic: manualState.topic })
    });
    var d = await r.json();
    var script = (d.script_package && d.script_package.text) || '';
    document.getElementById('m-script').value = script;
    updateManualWordCount();
  } catch(e) { alert('Regen failed: ' + e.message); }
}

function setManualVisualMode(mode) {
  manualState.visualMode = mode;
  localStorage.setItem('m_visual', mode);
  document.getElementById('vtog-images').className = 'btn btn-sm' + (mode==='images' ? ' btn-primary' : '');
  document.getElementById('vtog-video').className  = 'btn btn-sm' + (mode==='video'  ? ' btn-primary' : '');
  document.getElementById('vmode-images').style.display = mode === 'images' ? '' : 'none';
  document.getElementById('vmode-video').style.display  = mode === 'video'  ? '' : 'none';
  updateManualSummary();
}

function setManualVoiceMode(mode) {
  manualState.voiceMode = mode;
  localStorage.setItem('m_voice', mode);
  ['ai','record','upload'].forEach(function(m) {
    var btn = document.getElementById('vtog-' + m);
    if (btn) btn.className = 'btn btn-sm' + (m === mode ? ' btn-primary' : '');
    var panel = document.getElementById('vvoice-' + m);
    if (panel) panel.style.display = m === mode ? '' : 'none';
  });
  updateManualSummary();
}

function updateManualSummary() {
  var el = document.getElementById('m-summary');
  if (!el) return;
  var visual = manualState.visualMode === 'images' ? '&#128444; 3 auto-gen images (~3 Leonardo credits)' : '&#127909; Uploaded video (0 credits)';
  var voice  = manualState.voiceMode  === 'ai'     ? '&#129302; AI Voice (~200 ElevenLabs chars)'
             : manualState.voiceMode  === 'record'  ? '&#127908; Record in Studio after render'
             : '&#8679; Uploaded audio (0 credits)';
  var words  = (manualState.script || '').trim().split(/\s+/).filter(Boolean).length;
  el.innerHTML = '&#128221; Topic: ' + esc(manualState.topic || '—') + '<br>'
    + '&#127912; Visual: ' + visual + '<br>'
    + '&#127908; Voice: ' + voice + '<br>'
    + '&#128196; Script: ' + words + ' words';
}

async function autoGenManualImg(idx) {
  var prompts = ['m-img-p1','m-img-p2','m-img-p3'];
  var prompt  = (document.getElementById(prompts[idx]).value || '').trim();
  if (!prompt) { alert('Add an image prompt first'); return; }
  var statusEl = document.getElementById('m-img-status');
  if (statusEl) statusEl.textContent = 'Generating image ' + (idx+1) + '... (~30s)';
  // Image generation triggers via the pipeline when job is created
  // This is a preview trigger only — actual generation happens in pipeline
  if (statusEl) statusEl.textContent = 'Image ' + (idx+1) + ' will auto-generate when job is created.';
}

async function autoGenAllManualImgs() {
  var statusEl = document.getElementById('m-img-status');
  if (statusEl) statusEl.textContent = 'All 3 images will auto-generate when job is created.';
}

function uploadManualImg(event, idx) {
  var file = event.target.files[0];
  if (!file) return;
  var statusEl = document.getElementById('m-img-status');
  if (statusEl) statusEl.textContent = 'Image ' + (idx+1) + ' ready: ' + file.name;
  manualState.imgUrls[idx] = file;
}

function handleManualVideoUpload(event) {
  var file = event.target.files[0];
  if (!file) return;
  var statusEl = document.getElementById('m-video-status');
  var sizeMB = (file.size / 1024 / 1024).toFixed(1);
  if (statusEl) statusEl.textContent = '&#10003; ' + file.name + ' (' + sizeMB + ' MB) ready to upload';
  manualState.videoFile = file;
}

function handleManualVoiceUpload(event) {
  var file = event.target.files[0];
  if (!file) return;
  var statusEl = document.getElementById('m-voice-status');
  if (statusEl) statusEl.textContent = '&#10003; ' + file.name + ' ready';
  manualState.voiceFile = file;
}

async function doManualCreate(action) {
  var topic  = manualState.topic;
  var script = (document.getElementById('m-script') ? document.getElementById('m-script').value : '').trim();
  var result = document.getElementById('m-result');

  if (!topic)  { if (result) result.innerHTML = '<span style="color:var(--red)">Select a topic first</span>'; return; }
  if (!script) { if (result) result.innerHTML = '<span style="color:var(--red)">Script is required</span>'; return; }

  var words = script.split(/\s+/).filter(Boolean).length;
  if (words > 70) { if (result) result.innerHTML = '<span style="color:var(--red)">Script too long: ' + words + ' words (max 65)</span>'; return; }

  if (result) result.innerHTML = '<span style="color:var(--muted)">Creating job...</span>';

  try {
    // Step 1: Create the job
    var r = await fetch(API_BASE + '/create-manual-job', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ topic, script, cluster: manualState.cluster || 'AI' })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    var jobId = d.job_id;

    // Step 2: Handle video upload if video mode
    if (manualState.visualMode === 'video' && manualState.videoFile) {
      if (result) result.innerHTML = '<span style="color:var(--muted)">Uploading video...</span>';
      var vr = await fetch(API_BASE + '/upload-manual-video?job_id=' + jobId, {
        method: 'POST', headers: { 'content-type': 'video/mp4' },
        body: manualState.videoFile
      });
      var vd = await vr.json();
      if (vd.error) throw new Error('Video upload: ' + vd.error);
    }

    // Step 3: Trigger pipeline based on visual + voice mode
    if (manualState.visualMode === 'images') {
      // Images mode — get image prompts from textareas
      var imgPrompts = [
        document.getElementById('m-img-p1')?.value || '',
        document.getElementById('m-img-p2')?.value || '',
        document.getElementById('m-img-p3')?.value || '',
      ];
      // Trigger full pipeline with script override
      var pr = await fetch(API_BASE + '/run-topic', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic_id: manualState.topicId || null, topic, script })
      });
    }

    if (manualState.voiceMode === 'ai' && manualState.visualMode === 'video') {
      // Video uploaded + AI voice
      var ar = await fetch(API_BASE + '/add-voice-and-publish', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ job_id: jobId })
      });
    }

    var successMsg = action === 'stage'
      ? '&#10003; Staged — go to Queue to record or review'
      : '&#10003; Job created — pipeline is running';

    if (result) result.innerHTML = '<span style="color:var(--green)">' + successMsg + ' (ID: ' + jobId + ')</span>';

    // Reset state
    manualState.topicId = null; manualState.topic = ''; manualState.scriptPkg = null;
    manualState.videoFile = null; manualState.voiceFile = null; manualState.imgUrls = [null,null,null];
    clearManualTopic();
    setTimeout(function() { loadJobs(); loadTopicsCount(); }, 1500);

  } catch(e) {
    if (result) result.innerHTML = '<span style="color:var(--red)">&#10007; ' + e.message + '</span>';
  }
}

function updateWordCount() { updateManualWordCount(); } // backward compat alias

// ── QUEUE PAGE ──────────────────────────────────────────────────
async function loadQueue_panel() {
  await Promise.all([loadStaging(), loadCBDP(), loadManualJobs()]);
  updateQueueBadge();
}

function updateQueueBadge() {
  var total = allStaged.length + allReview.length + allManual.length;
  var el = document.getElementById('queue-badge');
  if (el) el.textContent = total;
  setText('qc-voice',  allStaged.length);
  setText('qc-review', allReview.length);
  setText('qc-manual', allManual.length);
}

// ── STAGING (Awaiting Voice) ─────────────────────────────────────
async function loadStaging() {
  try {
    var r = await fetch(API_BASE + '/staging');
    allStaged = await r.json();
    renderStagingGrid();
  } catch(e) {}
}

function renderStagingGrid() {
  var el = document.getElementById('staged-grid'); if (!el) return;
  if (!allStaged.length) {
    el.innerHTML = '<div class="empty" style="grid-column:1/-1"><span class="empty-icon">\uD83C\uDFAC</span>No staged videos.<br>Create a video in Human Voice mode and it appears here.</div>';
    return;
  }
  el.innerHTML = allStaged.map(function(j) {
    var cat = CATS[j.cluster] || { color:'var(--muted)', emoji:'\uD83D\uDCF9', label: j.cluster || '?' };
    var scr = (j.script_package && j.script_package.text) || '';
    var pkg = j.script_package || {};
    return '<div class="queue-card" data-jobid="' + j.id + '" onclick="openStudio(this.dataset.jobid)" style="cursor:pointer">'
      + '<div class="queue-card-head">'
      + '<div class="queue-card-topic">' + (j.topic || 'Untitled') + '</div>'
      + '<div class="queue-card-meta">'
      + '<span style="font-size:.66rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>'
      + '<span class="score-pill ' + scClass(j.council_score || 0) + '">' + (j.council_score || 0) + '</span>'
      + (pkg.mood ? moodBadge(pkg.mood, pkg.mood_label) : '')
      + '<span style="font-family:var(--mono);font-size:.57rem;color:var(--muted)">' + (j.created_at ? ago(j.created_at) + ' ago' : '') + '</span>'
      + '</div></div>'
      + '<div class="queue-card-body">' + scr.slice(0, 100) + (scr.length > 100 ? '\u2026' : '') + '</div>'
      + '<div class="queue-card-foot">'
      + '<span style="font-family:var(--mono);font-size:.6rem;background:rgba(0,230,118,.1);color:var(--green);padding:2px 8px;border-radius:4px">\uD83C\uDFA4 Needs Voice</span>'
      + '<span class="btn btn-primary btn-sm">Open Studio \u2192</span>'
      + '</div></div>';
  }).join('');
}

// ── CBDP / REVIEW ───────────────────────────────────────────────
async function loadCBDP() {
  try {
    var r    = await fetch(API_BASE + '/review');
    var data = await r.json();
    allReview = Array.isArray(data) ? data : [];
    setText('rev-cnt', allReview.length); // legacy badge if any
    renderReviewGrid();
  } catch(e) { console.error('loadCBDP:', e); }
}

function renderReviewGrid() {
  var el = document.getElementById('cbdp-grid'); if (!el) return;
  if (!allReview.length) {
    el.innerHTML = '<div class="empty" style="grid-column:1/-1"><span class="empty-icon">\uD83C\uDFAC</span>No videos in review.<br><span style="font-size:.78rem">Videos appear here when Publish is OFF or upload fails after render.</span></div>';
    return;
  }
  el.innerHTML = allReview.map(function(j) {
    var cat      = CATS[j.cluster] || { color:'var(--muted)', emoji:'\uD83D\uDCF9', label: j.cluster || '?' };
    var scr      = (j.script_package && j.script_package.text) || '';
    var title    = (j.script_package && j.script_package.title) || j.topic || 'Untitled';
    var reason   = j.review_reason || 'Ready for review';
    var hasVideo = !!(j.has_video && j.video_public_url);
    var videoUrl = j.video_public_url || '';
    var isSilent = j.status === 'staged';
    return '<div class="queue-card" style="cursor:default">'
      + '<div class="queue-card-head">'
      + '<div class="queue-card-topic">' + title + '</div>'
      + '<div class="queue-card-meta">'
      + '<span style="font-size:.63rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>'
      + '<span class="score-pill ' + scClass(j.council_score || 0) + '">' + (j.council_score || 0) + '</span>'
      + badge(j.status)
      + '</div></div>'
      + '<div style="padding:4px 12px;background:var(--surface2);font-family:var(--mono);font-size:.58rem;color:var(--muted);border-bottom:0.5px solid var(--border)">' + reason + '</div>'
      + (hasVideo
        ? '<video src="' + videoUrl + '" controls preload="metadata" style="width:100%;max-height:200px;background:#000;display:block"></video>'
        : '<div style="background:var(--surface2);height:56px;display:flex;align-items:center;justify-content:center;font-family:var(--mono);font-size:.65rem;color:var(--muted)">No video file saved</div>')
      + '<div class="queue-card-body">' + scr.slice(0, 120) + (scr.length > 120 ? '\u2026' : '') + '</div>'
      + '<div class="queue-card-foot">'
      + (hasVideo
        ? '<button class="btn btn-primary btn-sm" style="flex:2" data-jid="' + j.id + '" data-silent="' + isSilent + '" onclick="publishReview(this.dataset.jid,this.dataset.silent===\'true\',this)">'
          + (isSilent ? '\uD83C\uDFA4 Add Voice + Publish' : '\uD83D\uDE80 Publish to YouTube') + '</button>'
        : '<button class="btn btn-sm" style="flex:2;opacity:.4;cursor:not-allowed" disabled>No video — reject &amp; retry</button>')
      + '<button class="btn btn-red btn-sm" data-jid="' + j.id + '" onclick="rejectReview(this.dataset.jid,this)">\u2715 Reject</button>'
      + '</div></div>';
  }).join('');
}

async function publishReview(jobId, isSilent, btn) {
  var msg = isSilent ? 'Add AI voice and publish to YouTube?' : 'Publish this video to YouTube now?';
  if (!confirm(msg)) return;
  btn.disabled = true;
  btn.textContent = isSilent ? '\uD83C\uDFA4 Generating...' : '\u23F3 Publishing...';
  try {
    var endpoint = isSilent ? API_BASE + '/add-voice-and-publish' : API_BASE + '/publish-job';
    var r = await fetch(endpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    btn.textContent = isSilent ? '\u2713 Voice generating...' : '\u2713 Sent!';
    btn.style.background = 'var(--green)';
    allReview = allReview.filter(function(j) { return j.id !== jobId; });
    updateQueueBadge();
    setTimeout(function() { renderReviewGrid(); loadJobs(); }, 1500);
  } catch(e) {
    btn.textContent = isSilent ? '\uD83C\uDFA4 Add Voice + Publish' : '\uD83D\uDE80 Publish';
    btn.disabled = false;
    alert('Failed: ' + e.message);
  }
}

async function rejectReview(jobId, btn) {
  if (!confirm('Reject this video? Topic returns to queue.')) return;
  btn.disabled = true; btn.textContent = '\u23F3...';
  try {
    var r = await fetch(API_BASE + '/reject-job', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    allReview = allReview.filter(function(j) { return j.id !== jobId; });
    updateQueueBadge();
    renderReviewGrid(); loadTopicsCount();
  } catch(e) { btn.textContent = '\u2715 Reject'; btn.disabled = false; alert('Failed: ' + e.message); }
}

// ── MANUAL JOBS ─────────────────────────────────────────────────
async function loadManualJobs() {
  try {
    var r = await fetch(API_BASE + '/manual-jobs');
    allManual = await r.json();
    renderManualGrid();
  } catch(e) { console.error('loadManualJobs:', e); }
}

function renderManualGrid() {
  var el = document.getElementById('manual-grid'); if (!el) return;
  if (!allManual.length) {
    el.innerHTML = '<div class="empty" style="grid-column:1/-1">'
      + '<span class="empty-icon">&#9999;</span>'
      + 'No manual jobs yet.<br>'
      + '<span style="font-size:.8rem">Go to Create \u2192 Manual to write a script and create a job.</span>'
      + '</div>';
    return;
  }
  el.innerHTML = allManual.map(function(j) {
    var cat    = CATS[j.cluster] || { color:'var(--muted)', emoji:'\uD83D\uDCF9', label: j.cluster || '?' };
    var pkg    = j.script_package || {};
    var scr    = pkg.text || '';
    var words  = pkg.word_count || (scr.split(/\s+/).filter(Boolean).length);
    var isWaitingVideo  = j.status === 'manual_pending';
    var isWaitingVoice  = j.status === 'staged' && j.has_video;
    var isProcessing    = ['voice','upload','mixing'].includes(j.status);
    var isDone          = j.status === 'complete' || j.status === 'test_complete';
    return '<div class="queue-card" style="cursor:default">'
      + '<div class="queue-card-head">'
      + '<div class="queue-card-topic">' + (j.topic || 'Untitled') + '</div>'
      + '<div class="queue-card-meta">'
      + '<span style="font-size:.63rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>'
      + badge(j.status)
      + '<span style="font-family:var(--mono);font-size:.57rem;color:var(--muted)">' + words + ' words</span>'
      + '</div></div>'
      + '<div style="padding:10px 14px">'
      // Step indicator
      + '<div class="state-step ' + (!j.has_video && !isDone ? 'ss-current' : 'ss-done') + '" style="margin-bottom:5px">'
      + '1 Job created' + (isWaitingVideo ? ' \u2014 waiting for video upload' : ' \u2713')
      + '</div>'
      + '<div class="state-step ' + (isWaitingVoice ? 'ss-current' : (isDone || isProcessing ? 'ss-done' : 'ss-pending')) + '" style="margin-bottom:5px">'
      + '2 Video uploaded' + (isWaitingVoice ? ' \u2713 \u2014 generate voice to publish' : (isDone || isProcessing ? ' \u2713' : ''))
      + '</div>'
      + '<div class="state-step ' + (isDone ? 'ss-done' : (isProcessing ? 'ss-current' : 'ss-pending')) + '">'
      + '3 Voice + Publish' + (isDone ? ' \u2713' : (isProcessing ? ' \u2014 in progress...' : ''))
      + '</div>'
      + '</div>'
      + (scr ? '<div class="queue-card-body" style="max-height:44px">' + scr.slice(0, 90) + (scr.length > 90 ? '\u2026' : '') + '</div>' : '')
      + '<div class="queue-card-foot">'
      // Action buttons per state
      + (isWaitingVideo
        ? '<label class="btn btn-primary btn-sm" style="flex:1;cursor:pointer;justify-content:center">'
          + '\u2191 Upload Video'
          + '<input type="file" accept="video/*" style="display:none" data-jid="' + j.id + '" onchange="uploadManualVideo(this)">'
          + '</label>'
        : '')
      + (isWaitingVoice
        ? '<button class="btn btn-primary btn-sm" style="flex:1" data-jid="' + j.id + '" onclick="generateManualVoice(this.dataset.jid,this)">\uD83C\uDFA4 Generate Voice + Publish</button>'
        : '')
      + (isProcessing
        ? '<span style="font-family:var(--mono);font-size:.65rem;color:var(--yellow)">\u23F3 In progress...</span>'
        : '')
      + (isDone
        ? (j.youtube_id && j.youtube_id !== 'TEST_MODE'
            ? '<a class="yt-link" href="https://youtube.com/watch?v=' + j.youtube_id + '" target="_blank">\u25B6 Watch on YouTube</a>'
            : '<span style="font-family:var(--mono);font-size:.65rem;color:var(--green)">\u2713 Complete</span>')
        : '')
      + (j.has_video && j.video_public_url && !isDone
        ? '<a href="' + j.video_public_url + '" target="_blank" class="btn btn-ghost btn-sm">\u25B6</a>'
        : '')
      + '</div></div>';
  }).join('');
}

async function uploadManualVideo(input) {
  var jobId = input.dataset.jid;
  var file  = input.files[0];
  if (!file) return;
  // Find the button wrapper and show progress
  var label = input.parentElement;
  label.textContent = '\u23F3 Uploading ' + Math.round(file.size / 1024 / 1024 * 10) / 10 + 'MB...';
  try {
    var r = await fetch(API_BASE + '/upload-manual-video?job_id=' + jobId, {
      method: 'POST',
      headers: { 'Content-Type': 'video/mp4' },
      body: file
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    await loadManualJobs();
    updateQueueBadge();
  } catch(e) {
    alert('Upload failed: ' + e.message);
    label.innerHTML = '\u2191 Upload Video<input type="file" accept="video/*" style="display:none" data-jid="' + jobId + '" onchange="uploadManualVideo(this)">';
  }
}

async function generateManualVoice(jobId, btn) {
  if (!confirm('Generate AI voice from the script and publish to YouTube?')) return;
  btn.disabled = true; btn.textContent = '\uD83C\uDFA4 Starting...';
  try {
    var r = await fetch(API_BASE + '/add-voice-and-publish', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    btn.textContent = '\u2713 Processing...';
    btn.style.background = 'var(--green)';
    setTimeout(function() { loadManualJobs(); loadJobs(); }, 3000);
  } catch(e) {
    btn.textContent = '\uD83C\uDFA4 Generate Voice + Publish';
    btn.disabled = false;
    alert('Failed: ' + e.message);
  }
}

// ── SETTINGS ───────────────────────────────────────────────────
var currentSettings = {};

async function loadSettings() {
  try {
    var r = await fetch(API_BASE + '/settings');
    currentSettings = await r.json();
    applySettingsUI(currentSettings);
  } catch(e) { console.error('loadSettings:', e); }
}

function applySettingsUI(s) {
  // Mode buttons
  var autoBtn  = document.getElementById('mode-auto-btn');
  var stgBtn   = document.getElementById('mode-stage-btn');
  if (autoBtn) autoBtn.className = 'btn btn-sm ' + (s.mode === 'auto' ? 'btn-primary' : 'btn-ghost');
  if (stgBtn)  stgBtn.className  = 'btn btn-sm ' + (s.mode === 'stage' ? 'btn-primary' : 'btn-ghost');

  // Voice mode buttons
  var vmAi    = document.getElementById('vm-ai-btn');
  var vmHuman = document.getElementById('vm-human-btn');
  if (vmAi)    vmAi.className    = 'btn btn-sm ' + (s.voice_mode === 'ai'    ? 'btn-primary' : 'btn-ghost');
  if (vmHuman) vmHuman.className = 'btn btn-sm ' + (s.voice_mode === 'human' ? 'btn-primary' : 'btn-ghost');

  // Publish toggle
  setToggle('pub-tog', 'pub-knob', s.publish, '#00e676', '#ff5252');

  // Subscribe CTA toggle
  setToggle('cta-tog', 'cta-knob', s.subscribe_cta, '#00e676', 'var(--border2)');

  // Videos per day
  [1, 2, 3].forEach(function(n) {
    var el = document.getElementById('vpd-' + n);
    if (el) el.className = 'vpd-opt' + (s.videos_per_day === n ? ' active' : '');
  });
  var sched = VPD_SCHED[s.videos_per_day] || [];
  setText('vpd-times', 'Schedule: ' + (sched.join(' \u2022 ') || '-'));

  // Engine toggles
  var imgMode = s.image_engine || 'inbuilt';
  var voxMode = s.voice_engine || 'inbuilt';
  var ieIn  = document.getElementById('ie-inbuilt-btn');
  var ieEx  = document.getElementById('ie-external-btn');
  var veIn  = document.getElementById('ve-inbuilt-btn');
  var veEx  = document.getElementById('ve-external-btn');
  if (ieIn) ieIn.className = 'btn btn-sm ' + (imgMode === 'inbuilt' ? 'btn-primary' : 'btn-ghost');
  if (ieEx) ieEx.className = 'btn btn-ghost btn-sm ' + (imgMode === 'external' ? 'btn-primary' : 'btn-ghost');
  if (veIn) veIn.className = 'btn btn-sm ' + (voxMode === 'inbuilt' ? 'btn-primary' : 'btn-ghost');
  if (veEx) veEx.className = 'btn btn-ghost btn-sm ' + (voxMode === 'external' ? 'btn-primary' : 'btn-ghost');

  // Update longform precursor engine labels
  updateLfPrecursor();
}

function setToggle(togId, knobId, on, onColor, offColor) {
  var tog  = document.getElementById(togId);
  var knob = document.getElementById(knobId);
  if (!tog || !knob) return;
  tog.style.background  = on ? onColor : (offColor || 'var(--muted2)');
  knob.style.transform  = on ? 'translateX(18px)' : 'translateX(0)';
}

async function toggleSetting(key) {
  var newVal = !currentSettings[key];
  var body   = {};
  body[key]  = newVal;
  currentSettings[key] = newVal;
  applySettingsUI(currentSettings);
  try {
    await fetch(API_BASE + '/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
  } catch(e) {
    currentSettings[key] = !newVal; // revert
    applySettingsUI(currentSettings);
    showDebug('debug-settings', '<span class="dr">Failed: ' + e.message + '</span>');
  }
}

async function setMode(mode) {
  currentSettings.mode = mode;
  if (mode === 'auto') { currentSettings.publish = true; currentSettings.voice_mode = 'ai'; }
  applySettingsUI(currentSettings);
  try {
    await fetch(API_BASE + '/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode })
    });
    showDebug('debug-settings', '<span class="dg">Mode: ' + mode + '</span>');
  } catch(e) { showDebug('debug-settings', '<span class="dr">' + e.message + '</span>'); }
}

async function setVoiceMode(vm) {
  currentSettings.voice_mode = vm;
  applySettingsUI(currentSettings);
  try {
    await fetch(API_BASE + '/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ voice_mode: vm })
    });
  } catch(e) { showDebug('debug-settings', '<span class="dr">' + e.message + '</span>'); }
}

async function setVPD(n) {
  currentSettings.videos_per_day = n;
  applySettingsUI(currentSettings);
  try {
    await fetch(API_BASE + '/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ videos_per_day: n })
    });
  } catch(e) { showDebug('debug-settings', '<span class="dr">' + e.message + '</span>'); }
}

async function setEngineMode(type, mode) {
  // type = 'image' | 'voice', mode = 'inbuilt' | 'external'
  var key = type + '_engine';
  currentSettings[key] = mode;
  applySettingsUI(currentSettings);
  try {
    var body = {};
    body[key] = mode;
    await fetch(API_BASE + '/settings', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    showDebug('debug-settings',
      '<span class="dg">&#10003; ' + type + ' engine → ' + mode + '</span>');
  } catch(e) {
    showDebug('debug-settings', '<span class="dr">' + e.message + '</span>');
  }
}

// ── ACTION BUTTONS ──────────────────────────────────────────────
async function doGenerateTopic() {
  var topic = prompt('Topic idea:', '');
  if (topic === null) return;
  try {
    var r = await fetch(API_BASE + '/generate-topic', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic: topic || 'Future AI India' })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showDebug('debug-create', d.status === 'approved'
      ? '<span class="dg">&#10003; Approved! Score: ' + (d.evaluation && d.evaluation.council_score ? d.evaluation.council_score : '?') + '</span>'
      : '<span class="dr">Rejected.</span>');
    loadTopicsCount();
  } catch(e) { showDebug('debug-create', '<span class="dr">&#10007; ' + e.message + '</span>'); }
}

async function doKillIncomplete() {
  var run = allJobs.filter(function(j) { return ['pending','processing','images','voice','render','upload'].includes(j.status); });
  if (!run.length) { alert('No incomplete jobs.'); return; }
  if (!confirm('Kill ' + run.length + ' job(s)?')) return;
  try {
    var r = await fetch(API_BASE + '/kill-incomplete', { method: 'POST' });
    var d = await r.json();
    var msg = '<span class="dg">Killed ' + d.killed + '. Restored: ' + d.topics_restored + '</span>';
    showDebug('debug-create', msg);
    showDebug('debug-settings-inner', msg);
    setTimeout(function() { loadJobs(); loadTopicsCount(); }, 600);
  } catch(e) {
    showDebug('debug-settings-inner', '<span class="dr">' + e.message + '</span>');
  }
}

async function doRestoreFailed() {
  var f = allJobs.filter(function(j) { return j.status === 'failed'; });
  if (!f.length) { alert('No failed jobs.'); return; }
  if (!confirm('Restore ' + f.length + ' jobs?')) return;
  try {
    var r = await fetch(API_BASE + '/restore-failed', { method: 'POST' });
    var d = await r.json();
    var msg = '<span class="dg">Restored ' + d.restored + '.</span>';
    showDebug('debug-create', msg);
    showDebug('debug-settings-inner', msg);
    setTimeout(function() { loadJobs(); loadTopicsCount(); }, 600);
  } catch(e) {
    showDebug('debug-settings-inner', '<span class="dr">' + e.message + '</span>');
  }
}

async function doTestRender() {
  try {
    var r = await fetch(API_BASE + '/test-render');
    var d = await r.json();
    var msg = '<span class="dk">' + d.url + '</span><br>'
      + '<span class="' + (d.ok ? 'dg' : 'dr') + '">' + d.status + '</span> - '
      + (d.response || d.error || '-');
    showDebug('debug-create', msg);
    showDebug('debug-settings-inner', msg);
  } catch(e) {
    showDebug('debug-settings-inner', '<span class="dr">' + e.message + '</span>');
  }
}

async function doSyncAnalytics() {
  try {
    await fetch(API_BASE + '/sync-analytics', { method: 'POST' });
    setTimeout(loadAnalytics, 8000);
  } catch(e) { alert(e.message); }
}

// ── REPLENISH MODAL ─────────────────────────────────────────────
function openReplenishModal() { document.getElementById('rep-modal').classList.remove('hidden'); }
function closeReplenishModal() { document.getElementById('rep-modal').classList.add('hidden'); }

async function doReplenish() {
  var cats = Array.from(document.querySelectorAll('#modal-cats .cat-check.selected'))
    .map(function(d) { return d.dataset.cat; });
  var target = parseInt(document.getElementById('tgt-slider').value);
  closeReplenishModal();
  // Show status on both pages
  var pending = '<span style="color:var(--yellow)">&#8635; Replenishing topics... (10-30s)</span>';
  showDebug('debug-topics', pending);
  showDebug('debug-create', pending);
  // Switch to topics tab so user can see results appear
  showPage('topics', document.querySelector('[onclick*="topics"]'));
  try {
    var r = await fetch(API_BASE + '/replenish', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ categories: cats, target: target })
    });
    var d = await r.json();
    var ok = '<span style="color:var(--green)">&#10003; Replenish triggered — topics appear in ~60s. Categories: ' + (cats.join(', ') || 'all') + '</span>';
    showDebug('debug-topics', ok);
    showDebug('debug-create', ok);
    setTimeout(loadTopicsCount, 20000);
    setTimeout(loadTopicsCount, 40000);
    setTimeout(function() { loadTopicsCount(); if (currentPage === 'topics') loadTopics(); }, 65000);
  } catch(e) {
    var err = '<span style="color:var(--red)">&#10007; Replenish failed: ' + e.message + '</span>';
    showDebug('debug-topics', err);
    showDebug('debug-create', err);
  }
}

// ── STUDIO ─────────────────────────────────────────────────────
async function openStudio(jobId) {
  studioJob = allStaged.find(function(j) { return j.id === jobId; });
  if (!studioJob) return;
  var pkg = studioJob.script_package || {};
  setText('stu-title', studioJob.topic || 'Studio');
  setText('stu-id', jobId);
  setText('stu-script', pkg.text || 'No script');
  var moodEl = document.getElementById('stu-mood');
  if (moodEl) moodEl.innerHTML = pkg.mood ? 'Mood: ' + moodBadge(pkg.mood, pkg.mood_label) : '';

  var vid = document.getElementById('stu-vid');
  var videoUrl = studioJob.video_public_url
    || (R2_BASE_URL && studioJob.video_r2_url ? R2_BASE_URL + '/' + studioJob.video_r2_url : '');
  if (videoUrl) {
    vid.src = videoUrl; vid.load();
    vid.onerror = function() {
      vid.style.display = 'none';
      var errEl = document.getElementById('stu-vid-err');
      if (errEl) errEl.style.display = 'flex';
    };
    vid.oncanplay = function() {
      vid.style.display = '';
      var errEl = document.getElementById('stu-vid-err');
      if (errEl) errEl.style.display = 'none';
    };
  } else {
    vid.removeAttribute('src');
    var errEl = document.getElementById('stu-vid-err');
    if (errEl) errEl.style.display = 'flex';
  }

  await loadMusicList(); resetRec();
  document.getElementById('studio').classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

function closeStudio() {
  document.getElementById('studio').classList.add('hidden');
  document.body.style.overflow = '';
  stopRec();
  if (playbackAudio) { playbackAudio.pause(); playbackAudio = null; }
  studioJob = null;
}

async function loadMusicList() {
  try {
    var r = await fetch(API_BASE + '/music-library');
    var d = await r.json();
    var icons = { Epic:'\u26A1', Hopeful:'\uD83C\uDF05', Tech:'\uD83D\uDCBB', Emotional:'\uD83D\uDCAB', Neutral:'\uD83C\uDFB5' };
    document.getElementById('music-list').innerHTML = d.tracks.map(function(t) {
      return '<div class="music-track ' + (selectedMusic === t.id ? 'selected' : '') + '" data-tid="' + t.id + '" onclick="selectMusic(this.dataset.tid)">'
        + '<span>' + (icons[t.category] || '\uD83C\uDFB5') + '</span>'
        + '<div><div class="music-name">' + t.label + '</div><div class="music-cat">' + t.category + ' \u00b7 ' + t.duration + 's</div></div>'
        + '<span style="color:var(--green)">' + (selectedMusic === t.id ? '\u2713' : '') + '</span>'
        + '</div>';
    }).join('');
  } catch(e) { document.getElementById('music-list').innerHTML = '<div style="color:var(--muted);padding:8px;font-size:.73rem">Music unavailable</div>'; }
}

function selectMusic(id) { selectedMusic = id; loadMusicList(); }

function previewMix() {
  var vid = document.getElementById('stu-vid');
  if (vid && vid.src) { vid.currentTime = 0; vid.play().catch(function() {}); }
  if (recordedBlob) {
    if (playbackAudio) { playbackAudio.pause(); playbackAudio = null; }
    playbackAudio = new Audio(URL.createObjectURL(recordedBlob));
    playbackAudio.play();
  }
}

// ── RECORDER ───────────────────────────────────────────────────
async function startRec() {
  try {
    var stream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true, sampleRate: 44100 }
    });
    audioCtx = new AudioContext({ sampleRate: 44100 });
    var src  = audioCtx.createMediaStreamSource(stream);
    analyserNode = audioCtx.createAnalyser(); analyserNode.fftSize = 2048;
    var hpf  = audioCtx.createBiquadFilter(); hpf.type = 'highpass'; hpf.frequency.value = 80;
    var comp = audioCtx.createDynamicsCompressor();
    comp.threshold.value = -24; comp.ratio.value = 4;
    comp.attack.value = 0.003; comp.release.value = 0.25;
    src.connect(hpf); hpf.connect(comp); comp.connect(analyserNode);
    analyserNode.connect(audioCtx.destination);
    drawWaveform();
    audioChunks = []; mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
    mediaRecorder.ondataavailable = function(e) { if (e.data.size > 0) audioChunks.push(e.data); };
    mediaRecorder.onstop = function() {
      recordedBlob = new Blob(audioChunks, { type: 'audio/webm' });
      document.getElementById('rec-ply').disabled = false;
      document.getElementById('rec-rst').disabled = false;
      document.getElementById('rec-status').textContent = '\u2713 Recorded (' + Math.round(recordedBlob.size / 1024) + 'KB)';
      document.getElementById('rec-status').className = 'rec-status';
      clearInterval(recTimer);
    };
    mediaRecorder.start(100); isRecording = true; recSecs = 0;
    recTimer = setInterval(function() {
      recSecs++;
      var m = Math.floor(recSecs / 60), s = recSecs % 60;
      setText('rec-dur', m + ':' + (s < 10 ? '0' : '') + s);
    }, 1000);
    document.getElementById('rec-rec').disabled = true;
    document.getElementById('rec-stp').disabled = false;
    document.getElementById('rec-status').textContent = '\u25CF RECORDING...';
    document.getElementById('rec-status').className = 'rec-status recording';
  } catch(e) { alert('Microphone error: ' + e.message); }
}

function stopRec() {
  if (mediaRecorder && mediaRecorder.state !== 'inactive') {
    mediaRecorder.stop(); mediaRecorder.stream.getTracks().forEach(function(t) { t.stop(); });
  }
  isRecording = false;
  document.getElementById('rec-rec').disabled = false;
  document.getElementById('rec-stp').disabled = true;
}

function playRec() {
  if (!recordedBlob) return;
  if (playbackAudio) { playbackAudio.pause(); playbackAudio = null; document.getElementById('rec-ply').textContent = '\u25B6'; return; }
  playbackAudio = new Audio(URL.createObjectURL(recordedBlob)); playbackAudio.play();
  document.getElementById('rec-ply').textContent = '\u23F8';
  playbackAudio.onended = function() { document.getElementById('rec-ply').textContent = '\u25B6'; playbackAudio = null; };
}

function resetRec() {
  stopRec(); if (playbackAudio) { playbackAudio.pause(); playbackAudio = null; }
  audioChunks = []; recordedBlob = null; recSecs = 0;
  document.getElementById('rec-rec').disabled = false;
  document.getElementById('rec-stp').disabled = true;
  document.getElementById('rec-ply').disabled = true;
  document.getElementById('rec-rst').disabled = true;
  document.getElementById('rec-status').textContent = 'Ready';
  document.getElementById('rec-status').className = 'rec-status';
  setText('rec-dur', '0:00');
  var c = document.getElementById('waveform');
  if (c) { var ctx2 = c.getContext('2d'); ctx2.clearRect(0, 0, c.width, c.height); }
}

function drawWaveform() {
  if (!analyserNode) return;
  var canvas = document.getElementById('waveform');
  var ctx2   = canvas.getContext('2d');
  var W = canvas.width = canvas.offsetWidth, H = canvas.height;
  var buf = new Uint8Array(analyserNode.frequencyBinCount);
  function draw() {
    if (!isRecording) return; requestAnimationFrame(draw);
    analyserNode.getByteTimeDomainData(buf);
    ctx2.fillStyle = 'rgba(13,19,32,0.4)'; ctx2.fillRect(0, 0, W, H);
    ctx2.lineWidth = 1.5; ctx2.strokeStyle = '#00e5ff'; ctx2.beginPath();
    var step = W / buf.length;
    for (var i = 0; i < buf.length; i++) {
      var y = (buf[i] / 128.0) * (H / 2);
      i === 0 ? ctx2.moveTo(0, y) : ctx2.lineTo(i * step, y);
    }
    ctx2.stroke();
  }
  draw();
}

// ── PUBLISH (Studio) ────────────────────────────────────────────
async function doPublish(publishAt) {
  if (!studioJob) { alert('No job open'); return; }
  if (!recordedBlob) { alert('Please record your voice first'); return; }
  var sEl = document.getElementById('pub-status');
  var n = document.getElementById('pub-now'), s = document.getElementById('pub-sch');
  n.disabled = s.disabled = true;
  sEl.textContent = '\u23F3 Uploading voice...'; sEl.style.color = 'var(--yellow)';
  try {
    var ur = await fetch(API_BASE + '/upload-voice?job_id=' + studioJob.id, {
      method: 'POST', body: recordedBlob, headers: { 'Content-Type': 'audio/webm' }
    });
    if (!ur.ok) throw new Error('Upload failed: ' + ur.status);
    sEl.textContent = '\u23F3 Starting mix...';
    var mr = await fetch(API_BASE + '/mix', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        job_id:          studioJob.id,
        music_track:     selectedMusic || 'neutral_01',
        music_volume:    (parseInt(document.getElementById('mus-vol').value) || 8) / 100,
        publish_at:      publishAt || null,
        voice_offset_ms: parseInt(document.getElementById('voice-off').value) || 0,
      })
    });
    if (!mr.ok) throw new Error('Mix failed: ' + mr.status);
    sEl.textContent = '\u2713 ' + (publishAt ? 'Scheduled!' : 'Publishing soon!');
    sEl.style.color = 'var(--green)';
    allStaged = allStaged.filter(function(j) { return j.id !== studioJob.id; });
    renderStagingGrid(); updateQueueBadge();
    setTimeout(closeStudio, 2000);
  } catch(e) {
    sEl.textContent = '\u2717 ' + e.message; sEl.style.color = 'var(--red)';
  } finally { n.disabled = s.disabled = false; }
}
function publishNow() { doPublish(null); }
function publishScheduled() {
  var dt = document.getElementById('pub-at').value;
  if (!dt) { alert('Pick a date/time first'); return; }
  doPublish(new Date(dt).toISOString());
}

// ── IMAGE LIBRARY (Create from Library) ─────────────────────────
var libTopicFilter = 'all';

// ── LIBRARY STATE ───────────────────────────────────────────────
var _libCluster = 'all';
var _libJobType = 'all';

function setLibCluster(cluster, el) {
  _libCluster = cluster;
  document.querySelectorAll('[id^="libtab-"]').forEach(function(t) {
    t.classList.remove('active');
  });
  if (el) el.classList.add('active');
  loadLibrary();
}

function setLibJobType(jt, el) {
  _libJobType = jt;
  document.querySelectorAll('[id^="libjt-"]').forEach(function(t) {
    t.classList.remove('active');
  });
  if (el) el.classList.add('active');
  loadLibrary();
}

async function loadLibrary() {
  var grid2 = document.getElementById('lib-grid2');
  if (grid2) grid2.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:24px;color:var(--muted);font-family:var(--mono);font-size:.75rem">&#9203; Loading...</div>';

  try {
    var params = [];
    if (_libCluster !== 'all') params.push('cluster=' + _libCluster);
    if (_libJobType !== 'all') params.push('job_type=' + _libJobType);
    var qs = params.length ? '?' + params.join('&') : '';

    var r = await fetch(API_BASE + '/image-library' + qs);
    var d = await r.json();
    allImages = Array.isArray(d.images) ? d.images : [];
    setText('lib-count2', allImages.length);
    renderLibrary();
  } catch(e) {
    if (grid2) grid2.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:24px;color:var(--red)">&#9888; ' + e.message + '</div>';
  }
}

function buildLibFilter() {
  var topics = [...new Set(allImages.map(function(i) { return i.topic || 'unknown'; }))].filter(Boolean);
  var html = '<div class="cat-pill" data-topic="all" onclick="filterLib(\'all\',this)" style="border-color:var(--accent);color:var(--accent)">All (' + allImages.length + ')</div>'
    + topics.slice(0, 12).map(function(t) {
        var count = allImages.filter(function(i) { return i.topic === t; }).length;
        return '<div class="cat-pill" data-topic="' + t.replace(/"/g, '&quot;') + '" onclick="filterLib(this.dataset.topic,this)">' + t.slice(0, 22) + ' (' + count + ')</div>';
      }).join('');
  var f1 = document.getElementById('lib-filter');
  var f2 = document.getElementById('lib-filter2');
  if (f1) f1.innerHTML = html;
  if (f2) f2.innerHTML = html;
}

function filterLib(topic, el) {
  libTopicFilter = topic;
  document.querySelectorAll('#lib-filter .cat-pill, #lib-filter2 .cat-pill').forEach(function(p) {
    p.style.borderColor = ''; p.style.color = '';
  });
  document.querySelectorAll('[data-topic="' + topic + '"]').forEach(function(p) {
    p.style.borderColor = 'var(--accent)'; p.style.color = 'var(--accent)';
  });
  renderLibrary();
}

function renderLibrary() {
  var html2 = allImages.length ? allImages.map(function(img) {
    var sel2  = libSelectedImages2.indexOf(img.url) > -1;
    var idx2  = libSelectedImages2.indexOf(img.url);
    var cat   = CATS[img.cluster] || null;
    var engBadge = img.engine && img.engine !== 'unknown'
      ? '<div style="position:absolute;top:4px;left:4px;font-family:var(--mono);font-size:.52rem;'
        + 'background:rgba(0,0,0,.7);color:' + (img.engine.includes('FLUX') ? 'var(--accent)' : 'var(--muted)') + ';'
        + 'padding:1px 4px;border-radius:3px">' + img.engine.replace('FLUX-A10G','FLUX').slice(0,10) + '</div>'
      : '';
    var jtBadge = img.job_type === 'longform'
      ? '<div style="position:absolute;top:4px;right:4px;font-family:var(--mono);font-size:.52rem;'
        + 'background:rgba(179,136,255,.3);color:var(--purple);padding:1px 4px;border-radius:3px">LF</div>'
      : '';
    var clBadge = cat
      ? '<div style="font-size:.55rem;color:' + cat.color + ';padding:1px 0">' + cat.emoji + ' ' + img.cluster + '</div>'
      : '';
    return '<div class="lib-img-card ' + (sel2 ? 'selected' : '') + '" '
      + 'data-imgurl="' + img.url.replace(/"/g,'&quot;') + '" onclick="toggleLib2(this)" '
      + 'style="position:relative">'
      + '<img src="' + img.url + '" loading="lazy" '
        + 'style="width:100%;aspect-ratio:9/16;object-fit:cover;display:block" '
        + 'onerror="this.parentElement.style.display=\'none\'">'
      + engBadge + jtBadge
      + (sel2 ? '<div class="lib-num">' + (idx2+1) + '</div>' : '')
      + '<div class="lib-topic">' + clBadge + esc(img.topic.slice(0,24)) + '</div>'
      + '</div>';
  }).join('')
  : '<div style="grid-column:1/-1;text-align:center;padding:36px;color:var(--muted);font-family:var(--mono);font-size:.75rem">'
    + 'No images yet — run Full Auto to start building the library.</div>';

  var g2 = document.getElementById('lib-grid2');
  if (g2) g2.innerHTML = html2;
}

function toggleLib1(el) {
  var url = el.dataset.imgurl;
  var idx = selectedImages.indexOf(url);
  if (idx > -1) { selectedImages.splice(idx, 1); }
  else {
    if (selectedImages.length >= 3) { alert('Select exactly 3 images. Deselect one first.'); return; }
    selectedImages.push(url);
  }
  var sc = document.getElementById('lib-sel-count');
  if (sc) sc.textContent = selectedImages.length + ' / 3 selected';
  var btn = document.getElementById('lib-create-btn');
  if (btn) { btn.disabled = selectedImages.length !== 3; btn.style.opacity = selectedImages.length === 3 ? '1' : '.4'; }
  renderLibrary();
}

function toggleLib2(el) {
  var url = el.dataset.imgurl;
  var idx = libSelectedImages2.indexOf(url);
  if (idx > -1) { libSelectedImages2.splice(idx, 1); }
  else {
    if (libSelectedImages2.length >= 3) { alert('Select exactly 3 images. Deselect one first.'); return; }
    libSelectedImages2.push(url);
  }
  var sc = document.getElementById('lib-sel-count2');
  if (sc) sc.textContent = libSelectedImages2.length + ' / 3 selected';
  var btn = document.getElementById('lib-create-btn2');
  if (btn) { btn.disabled = libSelectedImages2.length !== 3; btn.style.opacity = libSelectedImages2.length === 3 ? '1' : '.4'; }
  renderLibrary();
}

async function createVideoFromLibrary() {
  if (selectedImages.length !== 3) { alert('Select exactly 3 images first.'); return; }
  await _createFromImages(selectedImages, 'lib-create-btn', 'lib-sel-count', function() { selectedImages = []; });
}
async function createVideoFromLibrary2() {
  if (libSelectedImages2.length !== 3) { alert('Select exactly 3 images first.'); return; }
  await _createFromImages(libSelectedImages2, 'lib-create-btn2', 'lib-sel-count2', function() { libSelectedImages2 = []; });
}
async function _createFromImages(imgs, btnId, cntId, resetFn) {
  var btn = document.getElementById(btnId);
  btn.disabled = true; btn.textContent = '\u23F3 Creating...';
  try {
    var r = await fetch(API_BASE + '/run-with-images', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ image_urls: imgs })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    resetFn();
    renderLibrary();
    setText(cntId, '0 / 3 selected');
    btn.textContent = '\u2713 Job created!';
    setTimeout(function() { loadJobs(); showPage('home', document.querySelector('.nav-btn')); }, 1200);
  } catch(e) {
    btn.textContent = '\u25B6 Create Video'; btn.disabled = false; btn.style.opacity = '1';
    alert('Failed: ' + e.message);
  }
}

async function uploadLibImages(input) {
  var files = Array.from(input.files);
  if (!files.length) return;
  var btn = input.parentElement;
  var orig = btn.innerHTML;
  var topic = prompt('Tag these images (for filtering):', 'uploaded');
  if (!topic) topic = 'uploaded';
  var ok = 0, fail = 0;
  for (var i = 0; i < files.length; i++) {
    var f = files[i];
    btn.innerHTML = '\u23F3 ' + f.name.slice(0, 20) + '... (' + (i + 1) + '/' + files.length + ')';
    try {
      var r = await fetch(API_BASE + '/upload-image?topic=' + encodeURIComponent(topic) + '&filename=' + encodeURIComponent(f.name), {
        method: 'POST', headers: { 'Content-Type': f.type || 'image/png' }, body: f
      });
      var d = await r.json();
      if (d.error) throw new Error(d.error);
      ok++;
    } catch(e) { fail++; }
  }
  btn.innerHTML = orig; input.value = '';
  loadLibrary();
}

// ── TOPICS PAGE ─────────────────────────────────────────────────
function filterTopics(f) {
  topicFilter = f;
  ['all','ready','used'].forEach(function(k) {
    var b = document.getElementById('bt-' + k);
    if (b) b.className = 'btn btn-sm ' + (k === f ? 'btn-primary' : 'btn-ghost');
  });
  renderTopicsPage();
}

function renderTopicsPage() {
  var topics = allTopics;
  if (topicFilter === 'ready') topics = topics.filter(function(t) { return !t.used && t.council_score >= 70; });
  if (topicFilter === 'used')  topics = topics.filter(function(t) { return t.used; });
  if (topicCat !== 'all')      topics = topics.filter(function(t) { return t.cluster === topicCat; });
  setText('topics-count', topics.length + ' topics');
  var el = document.getElementById('topics-list');
  if (!topics.length) { el.innerHTML = '<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No topics.</div>'; return; }
  el.innerHTML = topics.map(function(t) {
    var cat    = CATS[t.cluster] || null;
    var canGen = !t.used && t.council_score >= 70;
    return '<div class="topic-row">'
      + '<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px">'
      + '<div style="flex:1;min-width:0">'
      + '<div class="topic-text">' + t.topic + '</div>'
      + '<div style="display:flex;align-items:center;gap:7px;margin-top:4px">'
      + '<span class="score-pill ' + scClass(t.council_score) + '">' + t.council_score + '</span>'
      + (t.used ? '<span style="font-family:var(--mono);font-size:.56rem;background:rgba(0,230,118,.1);color:var(--green);padding:1px 6px;border-radius:3px">Used</span>'
                : '<span style="font-family:var(--mono);font-size:.56rem;background:rgba(255,82,82,.1);color:var(--red);padding:1px 6px;border-radius:3px">Ready</span>')
      + (cat ? '<span style="font-size:.63rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>' : '')
      + '</div></div>'
      + (canGen ? '<button class="btn btn-primary btn-sm" data-tid="' + t.id + '" onclick="generateNow(this.dataset.tid,this)">\u25B6 Now</button>' : '')
      + '</div></div>';
  }).join('');
}

async function generateNow(topicId, btn) {
  if (!confirm('Generate a video from this topic right now?')) return;
  btn.disabled = true; btn.textContent = '\u23F3...';
  try {
    var r = await fetch(API_BASE + '/run-topic', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic_id: topicId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    btn.textContent = '\u2713 Done!'; btn.style.color = 'var(--green)';
    setTimeout(function() { loadJobs(); loadTopicsCount(); renderTopicsPage(); }, 800);
  } catch(e) { btn.textContent = '\u25B6 Now'; btn.disabled = false; alert('Failed: ' + e.message); }
}

// ── ANALYTICS ───────────────────────────────────────────────────
async function loadAnalytics() {
  try {
    var r = await fetch(API_BASE + '/analytics');
    var d = await r.json();
    allAnalytics = d.analytics || []; analyticsJobs = d.jobs || [];
    if (currentPage === 'analytics') renderAnalytics();
  } catch(e) {}
}

function renderAnalytics() {
  var rows = allAnalytics;
  if (!rows.length) {
    ['a-views','a-likes','a-comments','a-avg'].forEach(function(id) { setText(id, '-'); });
    setText('a-count', '0 videos');
    var vg = document.getElementById('video-grid');
    if (vg) vg.innerHTML = '<div class="empty">\uD83D\uDCCA No analytics yet.</div>';
    return;
  }
  setText('a-views',    fmt(rows.reduce(function(s, r) { return s + (r.youtube_views || 0); }, 0)));
  setText('a-likes',    fmt(rows.reduce(function(s, r) { return s + (r.youtube_likes || 0); }, 0)));
  setText('a-comments', fmt(rows.reduce(function(s, r) { return s + (r.comment_count || 0); }, 0)));
  setText('a-avg',      fmt(rows.length ? Math.round(rows.reduce(function(s, r) { return s + (r.score || 0); }, 0) / rows.length) : 0));
  setText('a-count', rows.length + ' videos');
  var sorted = rows.slice().sort(function(a, b) { return b.score - a.score; });
  var vg = document.getElementById('video-grid');
  if (vg) vg.innerHTML = sorted.map(function(r) {
    var job = analyticsJobs.find(function(j) { return j.id === r.video_id; }) || {};
    var hasYt = job.youtube_id && job.youtube_id !== 'TEST_MODE';
    return '<div class="video-card"><div class="video-thumb">\uD83C\uDFAC</div><div class="video-body">'
      + '<div class="video-topic">' + (job.topic || 'Unknown') + '</div>'
      + '<div class="video-stats"><span>\uD83D\uDC41 <b>' + fmt(r.youtube_views || 0) + '</b></span>'
      + '<span>\u2764 <b>' + fmt(r.youtube_likes || 0) + '</b></span></div>'
      + '<div style="font-family:var(--mono);font-size:.68rem;font-weight:600;color:var(--yellow)">' + fmt(r.score || 0) + '</div>'
      + (hasYt ? '<a class="video-link" href="https://youtube.com/watch?v=' + job.youtube_id + '" target="_blank">&#9654; Watch</a>' : '')
      + '</div></div>';
  }).join('');
  function perfRow(r) {
    var j = analyticsJobs.find(function(x) { return x.id === r.video_id; }) || {};
    return '<div class="perf-row"><div class="perf-topic">' + (j.topic || '-') + '</div>'
      + '<div class="perf-num pn-views">' + fmt(r.youtube_views || 0) + '</div>'
      + '<div class="perf-num pn-likes">' + fmt(r.youtube_likes || 0) + '</div>'
      + '<div class="perf-num pn-score">' + fmt(r.score || 0) + '</div></div>';
  }
  var pl = document.getElementById('perf-list');
  if (pl) pl.innerHTML = sorted.slice(0, 5).map(perfRow).join('') || '<div class="empty" style="padding:14px">No data</div>';
  var withV = rows.filter(function(r) { return r.youtube_views > 0; });
  var fl = document.getElementById('flop-list');
  if (fl) fl.innerHTML = withV.sort(function(a, b) { return a.score - b.score; }).slice(0, 5).map(perfRow).join('') || '<div class="empty" style="padding:14px">No data</div>';
}

// ── CALENDAR ────────────────────────────────────────────────────
async function loadCalendar() {
  try {
    var r = await fetch(API_BASE + '/calendar');
    calEvents = await r.json();
    if (currentPage === 'calendar') renderCalendar();
  } catch(e) {}
}

function renderCalendar() {
  var el = document.getElementById('cal-grid'); if (!el) return;
  var y = calDate.getFullYear(), m = calDate.getMonth();
  setText('cal-lbl', calDate.toLocaleDateString('en-IN', { month: 'long', year: 'numeric' }));
  var first = new Date(y, m, 1).getDay(), days = new Date(y, m + 1, 0).getDate();
  var today = new Date(), html = '';
  for (var i = 0; i < first; i++) html += '<div class="cal-cell" style="opacity:.08"></div>';
  for (var d = 1; d <= days; d++) {
    var isToday = today.getDate() === d && today.getMonth() === m && today.getFullYear() === y;
    var evts = calEvents.filter(function(e) {
      var ed = new Date(e.scheduled_at || e.created_at);
      return ed.getFullYear() === y && ed.getMonth() === m && ed.getDate() === d;
    });
    var evHtml = evts.map(function(e) {
      var cat = CATS[e.cluster] || { color: 'var(--accent)' };
      var t = e.scheduled_at ? new Date(e.scheduled_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : '';
      return '<div class="cal-evt" style="background:' + cat.color + '22;color:' + cat.color + '">' + (t ? t + ' ' : '') + (e.topic || '').slice(0, 14) + '</div>';
    }).join('');
    html += '<div class="cal-cell ' + (isToday ? 'today' : '') + '"><div class="cal-dn" style="' + (isToday ? 'color:var(--accent);font-weight:700' : '') + '">' + d + '</div>' + evHtml + '</div>';
  }
  el.innerHTML = html;
}
function calPrev() { calDate = new Date(calDate.getFullYear(), calDate.getMonth() - 1, 1); renderCalendar(); }
function calNext() { calDate = new Date(calDate.getFullYear(), calDate.getMonth() + 1, 1); renderCalendar(); }
function calToday() { calDate = new Date(); renderCalendar(); }

// ── INIT ────────────────────────────────────────────────────────
buildCatStrips();

async function loadAll() {
  await Promise.all([
    loadJobs(),
    loadTopicsCount(),
    loadAnalytics(),
    loadSettings(),
    loadHealth(),
    loadStaging(),
    loadCBDP(),
    loadManualJobs(),
    loadCalendar(),
  ]);
  updateQueueBadge();
}
loadAll();

// Auto-refresh every 8 seconds
setInterval(function() {
  loadJobs();
  loadTopicsCount();
  loadStaging();
  loadCBDP();
  loadManualJobs();
  updateQueueBadge();
  if (currentPage === 'analytics') loadAnalytics();
  if (currentPage === 'calendar')  renderCalendar();
}, 8000);

// Health refresh every 60 seconds
setInterval(loadHealth, 60000);

// ============================================================
// KILL JOB
// ============================================================

async function killJob(jobId) {
  if (!confirm('Kill this job? Credits spent so far are lost.')) return;
  var row = document.getElementById('jrow-' + jobId);
  if (row) row.style.opacity = '0.4';
  try {
    var r = await fetch(API_BASE + '/kill-job', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ job_id: jobId })
    });
    var d = await r.json();
    if (d.killed) {
      if (row) row.remove();
      setTimeout(function() { loadJobs(); loadTopicsCount(); }, 500);
    } else {
      if (row) row.style.opacity = '';
      alert('Kill failed: ' + (d.error || JSON.stringify(d)));
    }
  } catch(e) {
    if (row) row.style.opacity = '';
    alert('Error: ' + e.message);
  }
}

// ============================================================
// LOGS OVERLAY
// ============================================================

var logsTab = 'failed';

function openLogs() {
  document.getElementById('logs-overlay').style.display = 'block';
  document.body.style.overflow = 'hidden';
  loadLogs();
}

function closeLogs() {
  document.getElementById('logs-overlay').style.display = 'none';
  document.body.style.overflow = '';
}

function switchLogsTab(name) {
  logsTab = name;
  ['failed','history'].forEach(function(n) {
    var t = document.getElementById('ltab-' + n);
    if (t) t.classList.toggle('active', n === name);
  });
  renderLogs();
}

async function loadLogs() {
  var el = document.getElementById('logs-list');
  if (el) el.innerHTML = '<div style="text-align:center;padding:30px;color:var(--muted);font-family:var(--mono);font-size:.75rem">Loading...</div>';
  try {
    var r = await fetch(API_BASE + '/logs');
    var d = await r.json();
    // merge into allJobs-like structure for renderLogs
    window._logsFailed  = d.failed  || [];
    window._logsHistory = d.complete || [];
    renderLogs();
  } catch(e) {
    if (el) el.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:10px">Error: ' + e.message + '</div>';
  }
}

function renderLogs() {
  var el = document.getElementById('logs-list');
  if (!el) return;
  var jobs = logsTab === 'failed'
    ? (window._logsFailed  || [])
    : (window._logsHistory || []);
  if (!jobs.length) {
    el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);font-family:var(--mono);font-size:.75rem">'
      + (logsTab === 'failed' ? '&#10003; No failed jobs' : 'No completed jobs yet') + '</div>';
    return;
  }
  el.innerHTML = jobs.map(function(j) {
    var cat    = CATS[j.cluster] || null;
    var catStr = cat ? cat.emoji + ' ' + j.cluster : j.cluster || '';
    var yt     = j.youtube_id && j.youtube_id !== 'TEST_MODE'
      ? '<a href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank" style="color:var(--red);text-decoration:none;font-size:.7rem">&#9654; Watch</a>'
      : '';
    var errStr = j.error
      ? '<div style="font-family:var(--mono);font-size:.65rem;color:var(--red);margin-top:4px;word-break:break-all">'
        + esc(j.error.slice(0,120)) + (j.error.length>120?'…':'') + '</div>'
      : '';
    var retryBtn = j.status === 'cbdp'
      ? '<button class="btn btn-sm" style="font-size:.68rem" onclick="retryUpload(\''+j.id+'\')">&#8635; Retry</button>'
      : '';
    return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:8px">'
      + '<div style="display:flex;align-items:flex-start;gap:8px">'
      + '<div style="flex:1;min-width:0">'
      + '<div style="font-size:.82rem;font-weight:600;margin-bottom:2px">' + esc(j.topic||'Untitled') + '</div>'
      + '<div style="font-family:var(--mono);font-size:.62rem;color:var(--muted)">'
      + catStr + ' &bull; ' + (j.updated_at ? ago(j.updated_at)+' ago' : '-')
      + '</div>' + errStr + '</div>'
      + '<div style="display:flex;align-items:center;gap:6px;flex-shrink:0">'
      + (yt ? yt : '') + retryBtn + badge(j.status)
      + '</div></div></div>';
  }).join('');
}

// ============================================================
// LONG-FORM VIDEO
// ============================================================

var lfDurMins      = 7;
var lfCurrentJobId = null;
var lfCurrentSegIdx= null;
var lfJobData      = null;

// Long-form image count table per duration
var LF_IMG_COUNT = {
  3:  { Hook:1, Context:1, 'Deep Dive':2, 'What It Means':1, Challenge:1, Payoff:1, total:7,  render:8  },
  7:  { Hook:1, Context:2, 'Deep Dive':3, 'What It Means':2, Challenge:1, Payoff:1, total:10, render:12 },
  10: { Hook:1, Context:2, 'Deep Dive':4, 'What It Means':2, Challenge:2, Payoff:1, total:12, render:16 },
  12: { Hook:1, Context:3, 'Deep Dive':5, 'What It Means':3, Challenge:2, Payoff:1, total:15, render:20 },
};

function setLfDur(mins, btn) {
  lfDurMins = mins;
  ['3','7','10','12'].forEach(function(d) {
    var b = document.getElementById('lf-dur-' + d);
    if (b) b.className = 'btn' + (d == mins ? ' btn-primary' : '');
  });
  updateLfPrecursor();
}

function updateLfPrecursor() {
  var data      = LF_IMG_COUNT[lfDurMins] || LF_IMG_COUNT[7];
  var imgEng    = (currentSettings && currentSettings.image_engine) || 'inbuilt';
  var voxEng    = (currentSettings && currentSettings.voice_engine) || 'inbuilt';
  var imgLabel  = imgEng === 'inbuilt' ? '&#9889; FLUX &mdash; free' : '&#9729; External APIs';
  var voxLabel  = voxEng === 'inbuilt' ? '&#9889; Chatterbox &mdash; free' : '&#9729; ElevenLabs';
  var imgColor  = imgEng === 'inbuilt' ? 'var(--green)' : 'var(--yellow)';
  var voxColor  = voxEng === 'inbuilt' ? 'var(--green)' : 'var(--yellow)';

  setText('lf-pre-imgs',   data.total);
  setText('lf-pre-voice',  6);  // always 6 segments
  setText('lf-pre-render', '~' + data.render + ' min');

  var imgEl = document.getElementById('lf-pre-engine-img');
  var voxEl = document.getElementById('lf-pre-engine-voice');
  if (imgEl) { imgEl.innerHTML = imgLabel; imgEl.style.color = imgColor; }
  if (voxEl) { voxEl.innerHTML = voxLabel; voxEl.style.color = voxColor; }
}

async function createLongformJob() {
  var topic   = (document.getElementById('lf-topic').value || '').trim();
  var cluster = document.getElementById('lf-cluster').value || 'Space';
  if (!topic) { alert('Enter a topic first'); return; }
  var st = document.getElementById('lf-create-status');
  st.textContent = 'Creating job...';
  st.style.color = 'var(--muted)';
  try {
    var r = await fetch(API_BASE + '/longform/create', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ topic: topic, cluster: cluster, target_duration: lfDurMins * 60 })
    });
    var d = await r.json();
    if (d.job_id) {
      st.textContent = 'Job created — script generating (~30s)';
      st.style.color = 'var(--accent)';
      document.getElementById('lf-topic').value = '';
      setTimeout(function() { loadLongformJobs(); st.textContent = ''; }, 4000);
    } else {
      st.textContent = 'Error: ' + (d.error || 'unknown');
      st.style.color = 'var(--red)';
    }
  } catch(e) {
    st.textContent = 'Error: ' + e.message;
    st.style.color = 'var(--red)';
  }
}

async function loadLongformJobs() {
  var el = document.getElementById('lf-jobs-list');
  if (!el) return;
  try {
    var r = await fetch(API_BASE + '/longform/jobs');
    var jobs = await r.json();
    if (!Array.isArray(jobs) || !jobs.length) {
      el.innerHTML = '<div style="text-align:center;padding:30px;color:var(--muted);font-family:var(--mono);font-size:.75rem">No long-form jobs yet.</div>';
      return;
    }
    // Show only non-complete jobs in Create panel; complete ones are in logs
    var active = jobs.filter(function(j) { return j.status !== 'complete'; });
    var completed = jobs.filter(function(j) { return j.status === 'complete'; });
    el.innerHTML = (active.length ? active : jobs).map(function(j) {
      var statusColor = {draft:'#888',scripting:'var(--yellow)',media_collecting:'#4fc3f7',
        ready_to_render:'var(--purple)',rendering:'var(--accent)',complete:'#69f0ae',failed:'var(--red)'}[j.status]||'#888';
      var durStr = j.target_duration ? Math.round(j.target_duration/60)+'m' : '?';
      var ytLink = j.youtube_id
        ? '<a href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank" style="color:var(--red);text-decoration:none;font-size:.7rem">&#9654; Watch</a>' : '';
      return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:8px;display:flex;align-items:center;gap:10px">'
        + '<div style="flex:1;min-width:0">'
        + '<div style="font-size:.82rem;font-weight:600;margin-bottom:2px">' + esc(j.topic) + '</div>'
        + '<div style="font-family:var(--mono);font-size:.62rem;color:var(--muted)">' + j.cluster + ' &bull; ' + durStr + ' &bull; ' + new Date(j.created_at).toLocaleDateString() + '</div>'
        + '</div>'
        + (ytLink ? '<div>' + ytLink + '</div>' : '')
        + '<div style="font-family:var(--mono);font-size:.68rem;color:' + statusColor + ';font-weight:600">' + j.status + '</div>'
        + '<button class="btn btn-sm" style="font-size:.7rem" onclick="openLfStudio(\'' + j.id + '\')">Open Studio</button>'
        + '</div>';
    }).join('');
    if (completed.length && active.length) {
      el.innerHTML += '<div style="font-family:var(--mono);font-size:.65rem;color:var(--muted);text-align:center;padding:6px">'
        + completed.length + ' completed job(s) — see Logs for history</div>';
    }
  } catch(e) {
    el.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:10px">Error: ' + e.message + '</div>';
  }
}

async function openLfStudio(jobId) {
  lfCurrentJobId  = jobId;
  lfCurrentSegIdx = null;
  document.getElementById('lf-studio').style.display = 'block';
  document.body.style.overflow = 'hidden';
  await refreshLfStudio();
}

function closeLfStudio() {
  document.getElementById('lf-studio').style.display = 'none';
  document.body.style.overflow = '';
  lfCurrentJobId = null;
  loadLongformJobs();
}

async function refreshLfStudio() {
  if (!lfCurrentJobId) return;
  try {
    var r   = await fetch(API_BASE + '/longform/' + lfCurrentJobId);
    var job = await r.json();
    lfJobData = job;
    document.getElementById('lf-studio-topic').textContent = job.topic || 'Long-form Video';
    document.getElementById('lf-studio-status').textContent =
      'Status: ' + job.status + (job.mood ? ' · Mood: ' + job.mood : '') +
      ' · Target: ' + Math.round((job.target_duration||420)/60) + ' min';
    var segs     = job.segments || [];
    var allReady = segs.length > 0 && segs.every(function(s) { return s.status === 'ready'; });
    var renderBtn = document.getElementById('lf-render-btn');
    if (renderBtn) renderBtn.disabled = !allReady;
    renderLfTimeline(segs);
    if (lfCurrentSegIdx !== null) {
      var seg = segs.find(function(s) { return s.segment_idx === lfCurrentSegIdx; });
      if (seg) renderLfEditor(seg);
    }
  } catch(e) {
    document.getElementById('lf-studio-status').textContent = 'Error: ' + e.message;
  }
}

function renderLfTimeline(segments) {
  var el = document.getElementById('lf-seg-timeline');
  if (!el) return;
  if (!segments.length) {
    el.innerHTML = '<div style="font-family:var(--mono);font-size:.7rem;color:var(--muted);padding:8px">Generating script...</div>';
    setTimeout(refreshLfStudio, 5000);
    return;
  }
  el.innerHTML = segments.map(function(seg) {
    var dot = seg.status === 'ready'         ? '#69f0ae'
            : (seg.status === 'has_media' || seg.status === 'generating_voice') ? '#4fc3f7'
            : seg.status === 'has_script'    ? 'var(--yellow)'
            : '#888';
    var active = seg.segment_idx === lfCurrentSegIdx;
    return '<div onclick="selectLfSeg(' + seg.segment_idx + ')" style="'
      + 'display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;cursor:pointer;margin-bottom:4px;'
      + 'background:' + (active ? 'var(--surface2)' : 'transparent') + ';'
      + 'border:1px solid ' + (active ? 'var(--border2)' : 'transparent') + '">'
      + '<span style="color:' + dot + ';font-size:1rem">&#9899;</span>'
      + '<div>'
      + '<div style="font-size:.78rem;font-weight:' + (active?'700':'500') + '">' + esc(seg.label||seg.type) + '</div>'
      + '<div style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">' + Math.round(seg.duration_target||60) + 's</div>'
      + '</div>'
      + '</div>';
  }).join('');
}

function selectLfSeg(idx) {
  lfCurrentSegIdx = idx;
  var segs = (lfJobData && lfJobData.segments) || [];
  var seg  = segs.find(function(s) { return s.segment_idx === idx; });
  renderLfTimeline(segs);
  if (seg) renderLfEditor(seg);
}

function renderLfEditor(seg) {
  var el = document.getElementById('lf-seg-editor');
  if (!el) return;
  var mediaItems = (seg.media || []).map(function(m,i) {
    return '<div style="display:flex;align-items:center;gap:6px;padding:6px;background:var(--bg);border-radius:6px;margin-bottom:4px">'
      + '<span style="font-size:.7rem;color:var(--muted)">' + (m.type==='video'?'&#127909;':'&#128444;') + ' ' + (i+1) + '</span>'
      + '<span style="font-family:var(--mono);font-size:.6rem;color:var(--accent);flex:1">' + esc((m.r2_url||'').split('/').pop()) + '</span>'
      + '</div>';
  }).join('');
  var voiceStatus = seg.voice_r2_url
    ? '<span style="color:#69f0ae">&#10003; ' + esc(seg.voice_source||'') + ' voice</span>'
    : '<span style="color:var(--muted)">No voice yet</span>';
  el.innerHTML =
    '<div style="display:flex;align-items:center;gap:8px;margin-bottom:14px">'
    + '<div style="font-size:.95rem;font-weight:700">' + esc(seg.label||seg.type) + '</div>'
    + '<div style="font-family:var(--mono);font-size:.65rem;color:var(--muted)">' + Math.round(seg.duration_target||60) + 's target</div>'
    + '</div>'
    + '<div style="margin-bottom:14px">'
    + '<div style="font-size:.72rem;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:6px">Script</div>'
    + '<textarea id="lf-seg-script" rows="5" style="width:100%;background:var(--bg);border:1px solid var(--border);'
    + 'border-radius:8px;padding:8px;color:var(--text);font-size:.8rem;resize:vertical;box-sizing:border-box">'
    + esc(seg.script||'') + '</textarea>'
    + '<button class="btn" style="margin-top:5px;font-size:.72rem" onclick="saveLfScript(' + seg.segment_idx + ')">Save Script</button>'
    + '</div>'
    + '<div style="margin-bottom:14px">'
    + '<div style="font-size:.72rem;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:6px">Media</div>'
    + (mediaItems || '<div style="font-family:var(--mono);font-size:.7rem;color:var(--muted);margin-bottom:6px">No media yet</div>')
    + '<div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">'
    + '<button class="btn" style="font-size:.72rem" onclick="lfAutoImages(' + seg.segment_idx + ')">&#9889; Auto-gen</button>'
    + '<label class="btn" style="font-size:.72rem;cursor:pointer">&#128228; Image<input type="file" accept="image/*" style="display:none" onchange="lfUploadMedia(event,' + seg.segment_idx + ',\'image\')"></label>'
    + '<label class="btn" style="font-size:.72rem;cursor:pointer">&#127909; Video<input type="file" accept="video/*" style="display:none" onchange="lfUploadMedia(event,' + seg.segment_idx + ',\'video\')"></label>'
    + '</div></div>'
    + '<div style="margin-bottom:14px">'
    + '<div style="font-size:.72rem;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:6px">Voice</div>'
    + '<div style="font-family:var(--mono);font-size:.7rem;margin-bottom:8px">' + voiceStatus + '</div>'
    + '<div style="display:flex;gap:6px;flex-wrap:wrap">'
    + '<button class="btn btn-primary" style="font-size:.72rem" onclick="lfGenVoice(' + seg.segment_idx + ')">&#9889; AI Voice</button>'
    + '<label class="btn" style="font-size:.72rem;cursor:pointer">&#127908; Upload<input type="file" accept="audio/*" style="display:none" onchange="lfUploadVoice(event,' + seg.segment_idx + ')"></label>'
    + '</div></div>'
    + '<div id="lf-seg-msg" style="font-family:var(--mono);font-size:.7rem;color:var(--muted);margin-top:6px"></div>';
}

function lfMsg(msg, color) {
  var el = document.getElementById('lf-seg-msg');
  if (el) { el.textContent = msg; el.style.color = color || 'var(--muted)'; }
}

async function saveLfScript(segIdx) {
  var script = (document.getElementById('lf-seg-script').value || '').trim();
  if (!script) { lfMsg('Script is empty', 'var(--red)'); return; }
  lfMsg('Saving...', 'var(--muted)');
  try {
    var r = await fetch(API_BASE + '/longform/segment/script', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ job_id: lfCurrentJobId, segment_idx: segIdx, script: script })
    });
    var d = await r.json();
    if (d.status === 'updated') { lfMsg('Saved', '#69f0ae'); refreshLfStudio(); }
    else lfMsg('Error: ' + (d.error||'unknown'), 'var(--red)');
  } catch(e) { lfMsg('Error: ' + e.message, 'var(--red)'); }
}

async function lfAutoImages(segIdx) {
  lfMsg('Triggering image generation...', 'var(--yellow)');
  try {
    var r = await fetch(API_BASE + '/longform/segment/generate-images', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ job_id: lfCurrentJobId, segment_idx: segIdx })
    });
    var d = await r.json();
    if (d.status === 'generating') { lfMsg('Generating... check back in ~60s', 'var(--accent)'); setTimeout(refreshLfStudio, 60000); }
    else lfMsg('Error: ' + (d.error||'unknown'), 'var(--red)');
  } catch(e) { lfMsg('Error: ' + e.message, 'var(--red)'); }
}

async function lfUploadMedia(event, segIdx, mediaType) {
  var file = event.target.files[0];
  if (!file) return;
  lfMsg('Uploading ' + mediaType + '...', 'var(--yellow)');
  var currentMedia = ((lfJobData && lfJobData.segments || []).find(function(s) { return s.segment_idx===segIdx; }) || {}).media || [];
  var mediaIdx = currentMedia.length;
  try {
    var r = await fetch(
      API_BASE + '/longform/segment/upload-media?job_id=' + lfCurrentJobId +
      '&segment_idx=' + segIdx + '&media_idx=' + mediaIdx + '&media_type=' + mediaType,
      { method: 'POST', headers: { 'content-type': file.type }, body: file }
    );
    var d = await r.json();
    if (d.status === 'uploaded') { lfMsg('Uploaded', '#69f0ae'); refreshLfStudio(); }
    else lfMsg('Error: ' + (d.error||'unknown'), 'var(--red)');
  } catch(e) { lfMsg('Error: ' + e.message, 'var(--red)'); }
}

async function lfGenVoice(segIdx) {
  lfMsg('Triggering AI voice...', 'var(--yellow)');
  try {
    var r = await fetch(API_BASE + '/longform/segment/generate-voice', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ job_id: lfCurrentJobId, segment_idx: segIdx })
    });
    var d = await r.json();
    if (d.status === 'generating') { lfMsg('Generating voice... ~30s', 'var(--accent)'); setTimeout(refreshLfStudio, 30000); }
    else lfMsg('Error: ' + (d.error||'unknown'), 'var(--red)');
  } catch(e) { lfMsg('Error: ' + e.message, 'var(--red)'); }
}

async function lfUploadVoice(event, segIdx) {
  var file = event.target.files[0];
  if (!file) return;
  lfMsg('Uploading voice recording...', 'var(--yellow)');
  try {
    var r = await fetch(
      API_BASE + '/longform/segment/upload-voice?job_id=' + lfCurrentJobId + '&segment_idx=' + segIdx,
      { method: 'POST', headers: { 'content-type': file.type }, body: file }
    );
    var d = await r.json();
    if (d.status === 'uploaded') { lfMsg('Voice uploaded — segment ready!', '#69f0ae'); refreshLfStudio(); }
    else lfMsg('Error: ' + (d.error||'unknown'), 'var(--red)');
  } catch(e) { lfMsg('Error: ' + e.message, 'var(--red)'); }
}

async function triggerLfRender() {
  if (!lfCurrentJobId) return;
  if (!confirm('Render and publish this long-form video? This takes 5-15 minutes.')) return;
  try {
    var r = await fetch(API_BASE + '/longform/render', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ job_id: lfCurrentJobId })
    });
    var d = await r.json();
    if (d.status === 'rendering') {
      document.getElementById('lf-studio-status').textContent = 'Rendering... check back in 5-15 minutes';
      var btn = document.getElementById('lf-render-btn');
      btn.disabled = true; btn.textContent = '\u231B Rendering...';
    } else {
      alert('Error: ' + (d.error || JSON.stringify(d)));
    }
  } catch(e) { alert('Error: ' + e.message); }
}
