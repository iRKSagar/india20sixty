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
function showToast(msg, duration) {
  var t = document.getElementById('toast-msg');
  if (!t) {
    t = document.createElement('div');
    t.id = 'toast-msg';
    t.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:var(--surface);border:1px solid var(--border);color:var(--text);padding:10px 20px;border-radius:8px;font-family:var(--mono);font-size:.75rem;z-index:9999;pointer-events:none;transition:opacity .3s';
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.style.opacity = '1';
  clearTimeout(t._timer);
  t._timer = setTimeout(function() { t.style.opacity = '0'; }, duration || 2500);
}

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
  if (name === 'analytics') { loadAnalytics(); }
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
  if (name === 'manual')   { initManualPanel(); updateManualSummary(); }
  if (name === 'freeimg')  { initFreeImagesPanel(); }
  if (name === 'longform') { loadLongformJobs(); loadLfTopicIdeas(); }
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
    var data = await r.json();
    allJobs = Array.isArray(data) ? data : [];
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
  topicSrc:    'queue',
  topicId:     null,
  topic:       '',
  cluster:     'AI',
  script:      '',
  scriptPkg:   null,
  visualMode:  localStorage.getItem('m_visual') || 'images',
  voiceMode:   localStorage.getItem('m_voice')  || 'ai',
  imgUrls:     [null, null, null],
  libPicks:    [null, null, null],   // library image picks per slot {url, key}
  videoUrl:    null,
  voiceUrl:    null,
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
    // Fetch if allTopics not loaded yet
    if (!allTopics.length) await loadTopics();
    var topics = allTopics.filter(function(t) {
      return !t.used && t.council_score >= 70 && (!cat || t.cluster === cat);
    }).sort(function(a,b) { return b.council_score - a.council_score; }).slice(0,20);
    _manualTopicsCache = topics;
    if (!topics.length) {
      container.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-family:var(--mono);font-size:.72rem">No topics in queue. <button class="btn btn-ghost btn-sm" onclick="openReplenishModal()">Replenish</button></div>';
      return;
    }
    container.innerHTML = topics.map(function(t, i) {
      var cat2 = CATS[t.cluster] || null;
      var hasScript = !!(t.script_package && t.script_package.text);
      return '<div onclick="selectManualTopicByIdx(' + i + ')" style="'
        + 'padding:10px 12px;background:var(--bg);border:1px solid var(--border);border-radius:8px;cursor:pointer;transition:border-color .15s"'
        + ' onmouseover="this.style.borderColor=\'var(--accent)\'" onmouseout="this.style.borderColor=\'var(--border)\'">'
        + '<div style="font-size:.78rem;font-weight:600;margin-bottom:3px">' + esc(t.topic) + '</div>'
        + '<div style="display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:.6rem;color:var(--muted)">'
        + (cat2 ? '<span style="color:' + cat2.color + '">' + cat2.emoji + ' ' + t.cluster + '</span>' : '')
        + '<span>Score: ' + t.council_score + '</span>'
        + (hasScript ? '<span style="color:var(--green)">&#10003; Script ready</span>' : '')
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
  var el = document.getElementById('m-selected-topic');
  if (el) el.style.display = 'none';
  ['m-step-script','m-step-visuals','m-step-voice','m-step-actions'].forEach(function(id) {
    var e = document.getElementById(id); if (e) e.style.display = 'none';
  });
  updateManualSummary();
}

// ── FREE IMAGES PANEL ────────────────────────────────────────────
var fiState = { topicSrc:'queue', topicId:null, topic:'', cluster:'AI', scriptPkg:null, picks:[null,null,null], fiCat:null };
var _fiTopicsCache = [];

function initFreeImagesPanel() {
  buildFiCatStrip();
  loadFiQueueTopics(null);
}

function buildFiCatStrip() {
  var strip = document.getElementById('fi-cat-strip');
  if (!strip) return;
  strip.innerHTML = '<div class="cat-pill" onclick="loadFiQueueTopics(null,this)" style="border-color:var(--accent);color:var(--accent)">All</div>'
    + Object.entries(CATS).map(function(e) {
        return '<div class="cat-pill" onclick="loadFiQueueTopics(\'' + e[0] + '\',this)">' + e[1].emoji + ' ' + e[1].label + '</div>';
      }).join('');
}

async function loadFiQueueTopics(cat, el) {
  fiState.fiCat = cat;
  if (el) {
    document.querySelectorAll('#fi-cat-strip .cat-pill').forEach(function(p) { p.style.borderColor=''; p.style.color=''; });
    el.style.borderColor = cat ? (CATS[cat]?.color||'var(--accent)') : 'var(--accent)';
    el.style.color       = cat ? (CATS[cat]?.color||'var(--accent)') : 'var(--accent)';
  }
  var container = document.getElementById('fi-topic-cards');
  if (!container) return;
  container.innerHTML = '<div style="padding:12px;color:var(--muted);font-family:var(--mono);font-size:.72rem">Loading...</div>';
  try {
    if (!allTopics.length) await loadTopics();
    var topics = allTopics.filter(function(t) {
      return !t.used && t.council_score >= 70 && (!cat || t.cluster === cat);
    }).sort(function(a,b) { return b.council_score - a.council_score; }).slice(0,20);
    _fiTopicsCache = topics;
    if (!topics.length) {
      container.innerHTML = '<div style="padding:12px;color:var(--muted);font-family:var(--mono);font-size:.72rem">No topics. <button class="btn btn-ghost btn-sm" onclick="openReplenishModal()">Replenish</button></div>';
      return;
    }
    container.innerHTML = topics.map(function(t, i) {
      var c = CATS[t.cluster]||{};
      return '<div onclick="selectFiTopic(' + i + ')" style="padding:10px 12px;background:var(--bg);border:1px solid var(--border);border-radius:8px;cursor:pointer"'
        + ' onmouseover="this.style.borderColor=\'var(--accent)\'" onmouseout="this.style.borderColor=\'var(--border)\'">'
        + '<div style="font-size:.78rem;font-weight:600;margin-bottom:2px">' + esc(t.topic) + '</div>'
        + '<div style="font-family:var(--mono);font-size:.6rem;color:' + (c.color||'var(--muted)') + '">' + (c.emoji||'') + ' ' + t.cluster + ' · Score: ' + t.council_score + '</div>'
        + '</div>';
    }).join('');
  } catch(e) {
    container.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:8px">' + e.message + '</div>';
  }
}

function setFiTopicSrc(src) {
  fiState.topicSrc = src;
  document.getElementById('fi-src-queue').className  = 'btn btn-sm' + (src==='queue'  ? ' btn-primary' : '');
  document.getElementById('fi-src-custom').className = 'btn btn-sm' + (src==='custom' ? ' btn-primary' : '');
  document.getElementById('fi-queue-panel').style.display  = src==='queue'  ? '' : 'none';
  document.getElementById('fi-custom-panel').style.display = src==='custom' ? '' : 'none';
}

function selectFiTopic(idx) {
  var t = _fiTopicsCache[idx]; if (!t) return;
  fiState.topicId = t.id; fiState.topic = t.topic; fiState.cluster = t.cluster; fiState.scriptPkg = t.script_package;
  var disp = document.getElementById('fi-topic-display');
  if (disp) { disp.style.display=''; document.getElementById('fi-topic-text').textContent=t.topic; document.getElementById('fi-topic-meta').textContent=(CATS[t.cluster]?.emoji||'')+' '+t.cluster+' · Score: '+t.council_score; }
  // Show image picker step
  var step = document.getElementById('fi-step-images'); if (step) step.style.display='';
  loadFiLibrary();
}

function setFiCustomTopic() {
  var val = (document.getElementById('fi-custom-topic').value||'').trim(); if (!val) return;
  fiState.topicId=null; fiState.topic=val; fiState.cluster='AI'; fiState.scriptPkg=null;
  var disp = document.getElementById('fi-topic-display');
  if (disp) { disp.style.display=''; document.getElementById('fi-topic-text').textContent=val; document.getElementById('fi-topic-meta').textContent='Custom topic'; }
  var step = document.getElementById('fi-step-images'); if (step) step.style.display='';
  loadFiLibrary();
}

function clearFiTopic() {
  fiState.topicId=null; fiState.topic=''; fiState.scriptPkg=null; fiState.picks=[null,null,null];
  var disp = document.getElementById('fi-topic-display'); if (disp) disp.style.display='none';
  var step = document.getElementById('fi-step-images'); if (step) step.style.display='none';
  var create = document.getElementById('fi-step-create'); if (create) create.style.display='none';
  resetFiPicks();
}

async function loadFiLibrary() {
  var grid = document.getElementById('fi-lib-grid'); if (!grid) return;
  grid.innerHTML = '<div style="padding:16px;color:var(--muted);font-family:var(--mono);font-size:.72rem">Loading library...</div>';
  try {
    if (!allImages.length) { var r=await fetch(API_BASE+'/image-library'); var d=await r.json(); allImages=d.images||[]; }
    renderFiGrid();
    buildFiFilter();
  } catch(e) { grid.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:8px">'+e.message+'</div>'; }
}

var _fiClusterFilter = 'all';
function buildFiFilter() {
  var strip = document.getElementById('fi-lib-filter'); if (!strip) return;
  var clusters = [...new Set(allImages.map(function(i){return i.cluster||'AI'}))].filter(Boolean);
  strip.innerHTML = '<div class="cat-pill" onclick="setFiFilter(\'all\',this)" style="border-color:var(--accent);color:var(--accent)">All ('+allImages.length+')</div>'
    + clusters.map(function(c){
        var cat=CATS[c]||{}; var cnt=allImages.filter(function(i){return i.cluster===c;}).length;
        return '<div class="cat-pill" onclick="setFiFilter(\''+c+'\',this)">'+( cat.emoji||'')+ ' '+c+' ('+cnt+')</div>';
      }).join('');
}

function setFiFilter(c, el) {
  _fiClusterFilter=c;
  document.querySelectorAll('#fi-lib-filter .cat-pill').forEach(function(p){p.style.borderColor='';p.style.color='';});
  if(el){el.style.borderColor='var(--accent)';el.style.color='var(--accent)';}
  renderFiGrid();
}

function renderFiGrid() {
  var grid = document.getElementById('fi-lib-grid'); if (!grid) return;
  var imgs = _fiClusterFilter==='all' ? allImages : allImages.filter(function(i){return i.cluster===_fiClusterFilter;});
  if (!imgs.length) { grid.innerHTML='<div style="color:var(--muted);font-family:var(--mono);font-size:.72rem;padding:16px">No images yet — run Full Auto first.</div>'; return; }
  grid.innerHTML = imgs.map(function(img, i) {
    var isPicked = fiState.picks.some(function(p){return p&&p.url===img.url;});
    var pickIdx  = fiState.picks.findIndex(function(p){return p&&p.url===img.url;});
    var labels   = ['H','S','P'];
    return '<div class="fi-img-card" data-imgurl="'+esc(img.url||'')+'" data-imgkey="'+esc(img.key||'')+'" onclick="toggleFiPick(this)"'
      + ' style="cursor:pointer;border-radius:8px;overflow:hidden;border:3px solid '+(isPicked?'var(--accent)':'transparent')+';position:relative">'
      + (img.url ? '<img src="'+esc(img.url)+'" loading="lazy" style="width:100%;aspect-ratio:9/16;object-fit:cover;display:block">' : '<div style="width:100%;aspect-ratio:9/16;background:var(--surface2)"></div>')
      + (isPicked ? '<div style="position:absolute;top:4px;right:4px;width:20px;height:20px;border-radius:50%;background:var(--accent);color:#000;display:flex;align-items:center;justify-content:center;font-size:.65rem;font-weight:800">'+labels[pickIdx]+'</div>' : '')
      + '<div style="padding:3px 5px;font-family:var(--mono);font-size:.52rem;color:var(--muted);background:rgba(0,0,0,.5)">'+ esc((img.topic||'').slice(0,18)) +'</div>'
      + '</div>';
  }).join('');
}

function toggleFiPick(el) {
  var url = el.dataset.imgurl; var key = el.dataset.imgkey;
  var existing = fiState.picks.findIndex(function(p){return p&&p.url===url;});
  if (existing>-1) {
    fiState.picks[existing]=null;
  } else {
    var slot = fiState.picks.findIndex(function(p){return !p;});
    if (slot===-1) { showToast('Already 3 picked. Click one to deselect.'); return; }
    fiState.picks[slot]={url:url,key:key};
  }
  updateFiPicks();
  renderFiGrid();
}

function resetFiPicks() {
  fiState.picks=[null,null,null];
  updateFiPicks();
}

function updateFiPicks() {
  var labels=['Hook','Story','Payoff'];
  var filled=fiState.picks.filter(function(p){return !!p;}).length;
  var cnt=document.getElementById('fi-img-count'); if(cnt) cnt.textContent=filled+' / 3';
  // Update preview slots
  for(var i=0;i<3;i++){
    var slot=document.getElementById('fi-pick-'+i);
    if(!slot) continue;
    if(fiState.picks[i]&&fiState.picks[i].url){
      slot.innerHTML='<img src="'+esc(fiState.picks[i].url)+'" style="width:100%;height:100%;object-fit:cover">'
        +'<div style="position:absolute;bottom:0;left:0;right:0;background:rgba(0,0,0,.6);font-family:var(--mono);font-size:.52rem;color:var(--accent);padding:2px 4px">'+labels[i]+'</div>';
      slot.style.border='2px solid var(--accent)';
    } else {
      slot.innerHTML=labels[i];
      slot.style.border='2px dashed var(--border2)';
    }
  }
  // Show/hide create step
  var createStep=document.getElementById('fi-step-create');
  if(createStep) createStep.style.display = filled===3 ? '' : 'none';
}

async function submitFreeImages() {
  if (!fiState.topic) { showToast('Pick a topic first'); return; }
  if (fiState.picks.filter(function(p){return!!p;}).length!==3) { showToast('Pick exactly 3 images'); return; }
  var btn=document.getElementById('fi-step-create')?.querySelector('button');
  var result=document.getElementById('fi-result');
  if(btn){btn.disabled=true;btn.textContent='Creating...';}
  if(result) result.innerHTML='<span style="color:var(--muted)">Submitting...</span>';
  try {
    var urls=fiState.picks.map(function(p){return p.url;});
    var r=await fetch(API_BASE+'/run-with-images',{
      method:'POST', headers:{'content-type':'application/json'},
      body:JSON.stringify({image_urls:urls, topic:fiState.topic, category:fiState.cluster, topic_id:fiState.topicId||null})
    });
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    if(result) result.innerHTML='<span style="color:var(--green)">&#10003; Job created: '+d.job_id+'<br>Script, voice, render &amp; publish running automatically.</span>';
    // Reset
    fiState.picks=[null,null,null]; updateFiPicks();
    setTimeout(function(){loadJobs();loadTopicsCount();},2000);
  } catch(e) {
    if(result) result.innerHTML='<span style="color:var(--red)">&#10007; '+e.message+'</span>';
  } finally {
    if(btn){btn.disabled=false;btn.textContent='&#9654; Create &amp; Publish';}
  }
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
  var hasLibPicks = manualState.libPicks && manualState.libPicks.every(function(p) { return p && p.url; });
  var pickedCount = manualState.libPicks ? manualState.libPicks.filter(function(p) { return p && p.url; }).length : 0;
  var visual = hasLibPicks
    ? '&#128444; Library images (0 credits — 3/3 selected)'
    : pickedCount > 0
    ? '&#128444; ' + pickedCount + '/3 library images picked — or FLUX auto-gen'
    : manualState.visualMode === 'images'
    ? '&#128444; 3 FLUX images (~$0.045 GPU credits)'
    : '&#127909; Uploaded video (0 credits)';
  var voice  = manualState.voiceMode  === 'ai'     ? '&#129302; AI Voice (Chatterbox — free)'
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
      var hasLibPicks = manualState.libPicks.every(function(p) { return p && p.url; });
      if (hasLibPicks) {
        // Library images — skip image generation
        var libUrls = manualState.libPicks.map(function(p) { return p.url; });
        if (result) result.innerHTML = '<span style="color:var(--muted)">Triggering pipeline with library images...</span>';
        var pr = await fetch(API_BASE + '/run-with-images', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ image_urls: libUrls, topic: topic, script: script, category: manualState.cluster || 'AI', job_id: jobId })
        });
        var pd = await pr.json();
        if (pd.error) throw new Error(pd.error);
        jobId = pd.job_id || jobId;
      } else if (manualState.topicId) {
        // Topic from queue — use run-topic which marks topic as used
        if (result) result.innerHTML = '<span style="color:var(--muted)">Generating images and rendering...</span>';
        var pr = await fetch(API_BASE + '/run-topic', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ topic_id: manualState.topicId, topic: topic, script: script })
        });
        var pd = await pr.json();
        if (pd.error) throw new Error(pd.error);
        jobId = pd.job_id || jobId;
      } else {
        // Custom topic — trigger pipeline directly with the manual job
        if (result) result.innerHTML = '<span style="color:var(--muted)">Generating images and rendering...</span>';
        var pr = await fetch(API_BASE + '/trigger-manual-pipeline', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ job_id: jobId, topic: topic, script: script, cluster: manualState.cluster || 'AI' })
        });
        var pd = await pr.json();
        if (pd.error) throw new Error(pd.error);
      }
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
    manualState.videoFile = null; manualState.voiceFile = null;
    manualState.imgUrls = [null,null,null]; manualState.libPicks = [null,null,null];
    clearManualTopic();
    setTimeout(function() { loadJobs(); loadTopicsCount(); }, 1500);

  } catch(e) {
    if (result) result.innerHTML = '<span style="color:var(--red)">&#10007; ' + e.message + '</span>';
  }
}

 // backward compat alias

// ── QUEUE PAGE ──────────────────────────────────────────────────
function updateQueueBadge() {
  var s = Array.isArray(allStaged) ? allStaged.length : 0;
  var r = Array.isArray(allReview) ? allReview.length : 0;
  var m = Array.isArray(allManual) ? allManual.length : 0;
  var total = s + r + m;
  var el = document.getElementById('queue-badge');
  if (el) el.textContent = total;
  setText('qc-voice',  s);
  setText('qc-review', r);
  setText('qc-manual', m);
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
    var isSilent = j.is_silent || j.status === 'staged' && !(j.error && j.error.includes('publish_failed'));
    var statusColor = j.status === 'staged' ? 'var(--yellow)' : j.status === 'review' ? 'var(--cyan)' : 'var(--red)';
    return '<div class="queue-card" style="cursor:default">'
      + '<div class="queue-card-head">'
      + '<div class="queue-card-topic">' + title + '</div>'
      + '<div class="queue-card-meta">'
      + '<span style="font-size:.63rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>'
      + '<span class="score-pill ' + scClass(j.council_score || 0) + '">' + (j.council_score || 0) + '</span>'
      + badge(j.status)
      + '<span style="font-family:var(--mono);font-size:.57rem;color:var(--muted)">' + (j.created_at ? ago(j.created_at) + ' ago' : '') + '</span>'
      + '</div></div>'
      + '<div style="padding:4px 12px;background:var(--surface2);font-family:var(--mono);font-size:.58rem;color:' + statusColor + ';border-bottom:0.5px solid var(--border)">' + reason + '</div>'
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
  // Theme buttons
  var savedTheme = localStorage.getItem('i20_theme') || 'dark';
  setTheme(savedTheme);

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
  var pending = '<span style="color:var(--yellow)">&#8635; Replenishing topics... (~30-60s)</span>';
  showDebug('debug-topics', pending);
  showDebug('debug-create', pending);
  try {
    var r = await fetch(API_BASE + '/replenish', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ categories: cats, target: target })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error + (d.detail ? ': ' + d.detail : ''));

    // Replenish is fire-and-forget — council runs ~60s in background
    var msg = d.message || 'Topic council running...';
    showDebug('debug-topics', '<span style="color:var(--yellow)">&#8635; ' + msg + '</span>');
    showDebug('debug-create', '<span style="color:var(--yellow)">&#8635; ' + msg + '</span>');

    // Poll every 15s for up to 3 minutes
    var polls = 0;
    var prevCount = allTopics.filter(function(t){return !t.used;}).length;
    var poller = setInterval(async function() {
      polls++;
      await loadTopicsCount();
      await loadTopics();
      var newCount = allTopics.filter(function(t){return !t.used;}).length;
      if (newCount > prevCount) {
        clearInterval(poller);
        var added = newCount - prevCount;
        showDebug('debug-topics', '<span style="color:var(--green)">&#10003; ' + added + ' topics added. Total ready: ' + newCount + '</span>');
        showDebug('debug-create', '<span style="color:var(--green)">&#10003; ' + added + ' topics added.</span>');
      } else if (polls >= 12) {
        clearInterval(poller);
        showDebug('debug-topics', '<span style="color:var(--muted)">&#10003; Replenish complete. Check Topics tab.</span>');
      }
    }, 15000);
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

async function backfillLibrary() {
  var btn = document.querySelector('button[onclick="backfillLibrary()"]');
  if (btn) { btn.disabled = true; btn.textContent = '⟳ Scanning R2...'; }
  try {
    var r = await fetch(API_BASE + '/image-library/backfill', { method: 'POST' });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Backfill done: ' + d.inserted + ' images added, ' + d.skipped + ' already existed');
    loadLibrary();
  } catch(e) {
    showToast('Backfill failed: ' + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '↓ Backfill R2'; }
  }
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
  // Filter by topic if set
  var filtered = libTopicFilter === 'all'
    ? allImages
    : allImages.filter(function(img) { return img.topic === libTopicFilter; });

  var makeCard = function(img, selFn, selArr, cntId, btnId) {
    var sel   = selArr.some(function(s) { return s.url === img.url; });
    var idx   = selArr.findIndex(function(s) { return s.url === img.url; });
    var cat   = CATS[img.cluster] || null;
    var engBadge = img.engine && img.engine !== 'unknown'
      ? '<div style="position:absolute;top:4px;left:4px;font-family:var(--mono);font-size:.52rem;background:rgba(0,0,0,.7);color:'
        + (img.engine.includes('FLUX') ? 'var(--accent)' : 'var(--muted)')
        + ';padding:1px 4px;border-radius:3px">' + img.engine.replace('FLUX-A10G','FLUX').slice(0,10) + '</div>'
      : '';
    var jtBadge = img.job_type === 'longform'
      ? '<div style="position:absolute;bottom:24px;right:4px;font-family:var(--mono);font-size:.52rem;background:rgba(179,136,255,.3);color:var(--purple);padding:1px 4px;border-radius:3px">LF</div>'
      : '';
    var selBadge = sel
      ? '<div style="position:absolute;top:4px;right:4px;width:20px;height:20px;border-radius:50%;background:var(--accent);color:#000;display:flex;align-items:center;justify-content:center;font-size:.65rem;font-weight:700;z-index:2">' + (idx+1) + '</div>'
      : '<div style="position:absolute;top:4px;right:4px;width:20px;height:20px;border-radius:50%;border:2px solid rgba(255,255,255,.4);background:rgba(0,0,0,.25);z-index:2"></div>';
    var clBadge = cat ? '<span style="font-size:.55rem;color:' + cat.color + '">' + cat.emoji + ' ' + img.cluster + '</span>' : '';
    var imgContent = img.url
      ? '<img src="' + img.url + '" loading="lazy" style="width:100%;aspect-ratio:9/16;object-fit:cover;display:block" onerror="this.style.display=\'none\'">'
      : '<div style="width:100%;aspect-ratio:9/16;background:var(--surface2);display:flex;align-items:center;justify-content:center;font-size:1.2rem">🖼</div>';
    return '<div class="lib-img-card ' + (sel ? 'selected' : '') + '" '
      + 'data-imgurl="' + (img.url||'').replace(/"/g,'&quot;') + '" '
      + 'data-imgkey="' + (img.key||'').replace(/"/g,'&quot;') + '" '
      + 'data-imgid="' + (img.id||'') + '" '
      + 'data-selfn="' + selFn + '" '
      + 'onclick="' + selFn + '(this,\'' + cntId + '\',\'' + btnId + '\')" style="position:relative;cursor:pointer">'
      + imgContent + engBadge + jtBadge + selBadge
      + '<div class="lib-topic">' + clBadge + ' <span style="font-size:.58rem">' + esc((img.topic||'').slice(0,20)) + '</span></div>'
      + '</div>';
  };

  var empty = '<div style="grid-column:1/-1;text-align:center;padding:36px;color:var(--muted);font-family:var(--mono);font-size:.75rem">No images yet — run Full Auto first.</div>';

  // Create → Library tab grid (3-select, no delete)
  var g1 = document.getElementById('lib-grid');
  if (g1) {
    g1.innerHTML = filtered.length
      ? '' /* create-tab library grid removed */
      : empty;
  }

  // Library page grid (multi-select + delete)
  var g2 = document.getElementById('lib-grid2');
  if (g2) {
    g2.innerHTML = filtered.length
      ? filtered.map(function(img) { return makeCard(img, 'toggleLib2', libSelectedImages2, 'lib-sel-count2', 'lib-create-btn2'); }).join('')
      : empty;
  }

  buildLibFilter();
  _updateLibDeleteBtn();
}


// ── MANUAL IMAGE LIBRARY PICKER ─────────────────────────────
var _manualLibSlot = null;

async function openManualLibPicker(slotIdx) {
  _manualLibSlot = slotIdx;
  var labels = ['Hook', 'Story', 'Payoff'];

  var overlay = document.getElementById('manual-lib-picker');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'manual-lib-picker';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:500;overflow:auto;padding:20px;box-sizing:border-box';
    document.body.appendChild(overlay);
  }

  overlay.innerHTML = '<div style="max-width:860px;margin:0 auto">'
    + '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">'
    + '<button class="btn" onclick="document.getElementById(\'manual-lib-picker\').style.display=\'none\'">← Back</button>'
    + '<div style="font-size:.9rem;font-weight:700">Pick image for slot ' + (slotIdx+1) + ' — ' + (labels[slotIdx]||'') + '</div>'
    + '</div>'
    + '<div id="manual-lib-picker-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px">'
    + '<div style="color:var(--muted);font-family:var(--mono);font-size:.75rem;padding:20px">Loading...</div>'
    + '</div></div>';
  overlay.style.display = 'block';

  // Load images
  try {
    if (!allImages.length) {
      var r = await fetch(API_BASE + '/image-library');
      var d = await r.json();
      allImages = d.images || [];
    }
    var grid = document.getElementById('manual-lib-picker-grid');
    if (!allImages.length) {
      grid.innerHTML = '<div style="color:var(--muted);font-family:var(--mono);font-size:.75rem;padding:20px">No images yet — generate some videos first.</div>';
      return;
    }
    grid.innerHTML = allImages.map(function(img) {
      var cat = CATS[img.cluster] || { color: 'var(--muted)', emoji: '📷' };
      var current = manualState.libPicks[slotIdx];
      var isCurrent = current && current.url === img.url;
      var border = isCurrent ? 'var(--accent)' : 'transparent';
      var check = isCurrent ? '<div style="position:absolute;top:4px;right:4px;background:var(--accent);color:#000;border-radius:50%;width:18px;height:18px;display:flex;align-items:center;justify-content:center;font-size:.7rem;font-weight:700">&#10003;</div>' : '';
      var imgEl = img.url
        ? '<img src="' + esc(img.url) + '" style="width:100%;aspect-ratio:9/16;object-fit:cover;display:block" onerror="this.parentElement.style.display=\'none\'">'
        : '<div style="width:100%;aspect-ratio:9/16;background:var(--surface2);display:flex;align-items:center;justify-content:center">&#128444;</div>';
      return '<div data-url="' + esc(img.url||'') + '" data-key="' + esc(img.key||'') + '" onclick="pickManualLibImgEl(this)"'
        + ' style="cursor:pointer;border-radius:8px;overflow:hidden;border:3px solid ' + border + ';position:relative">'
        + imgEl + check
        + '<div style="position:absolute;bottom:0;left:0;right:0;background:rgba(0,0,0,.6);padding:3px 5px;font-size:.52rem;color:' + cat.color + '">'
        + cat.emoji + ' ' + esc((img.topic||'').slice(0,16)) + '</div>'
        + '</div>';
    }).join('');
  } catch(e) {
    var grid = document.getElementById('manual-lib-picker-grid');
    if (grid) grid.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.75rem;padding:20px">Error: ' + e.message + '</div>';
  }
}

function pickManualLibImgEl(el) {
  var url = el.dataset.url;
  var key = el.dataset.key;
  pickManualLibImg(url, key);
}

function clearManualImgPick(slotIdx) {
  manualState.libPicks[slotIdx] = null;
  manualState.imgUrls[slotIdx] = null;
  var n = slotIdx + 1;
  var preview = document.getElementById('m-img-p' + n + '-preview');
  if (preview) preview.style.display = 'none';
  updateManualSummary();
}

function _updateLibDeleteBtn() {
  var btn = document.getElementById('lib-delete-btn');
  if (!btn) return;
  var n = libSelectedImages2.length;
  if (n > 0) {
    btn.style.display = 'flex';
    btn.textContent = '\uD83D\uDDD1 Delete ' + n + ' image' + (n > 1 ? 's' : '');
  } else {
    btn.style.display = 'none';
  }
}

async function deleteSelectedImages() {
  var n = libSelectedImages2.length;
  if (!n) return;
  if (!confirm('Delete ' + n + ' image' + (n > 1 ? 's' : '') + ' from R2 and library? This cannot be undone.')) return;
  var btn = document.getElementById('lib-delete-btn');
  btn.textContent = '\u23F3 Deleting...'; btn.disabled = true;
  try {
    var keys = libSelectedImages2.map(function(s) { return s.key; }).filter(Boolean);
    var ids  = libSelectedImages2.map(function(s) { return s.id;  }).filter(Boolean);
    var r = await fetch(API_BASE + '/delete-images', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys: keys, ids: ids })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    // Remove deleted images from allImages
    var deletedUrls = libSelectedImages2.map(function(s) { return s.url; });
    allImages = allImages.filter(function(img) { return deletedUrls.indexOf(img.url) === -1; });
    libSelectedImages2 = [];
    setText('lib-count2', allImages.length);
    renderLibrary();
    showToast('Deleted ' + (d.deleted || n) + ' image' + (n > 1 ? 's' : ''));
  } catch(e) {
    btn.textContent = '\uD83D\uDDD1 Delete ' + n;
    btn.disabled = false;
    alert('Delete failed: ' + e.message);
  }
}

// Unified toggle for Create→Library tab (3 max)
// Unified toggle for Library page (multi-select for delete/use)
// Legacy aliases
function toggleLib2(el) {
  var url=el.dataset.imgurl, key=el.dataset.imgkey, id=el.dataset.imgid;
  if(!url) return;
  var idx=libSelectedImages2.findIndex(function(s){return s.url===url;});
  if(idx>-1){libSelectedImages2.splice(idx,1);}
  else{libSelectedImages2.push({url:url,key:key,id:id});}
  var sc=document.getElementById('lib-sel-count2');
  if(sc) sc.textContent=libSelectedImages2.length+' selected';
  var btn=document.getElementById('lib-create-btn2');
  if(btn){btn.disabled=libSelectedImages2.length!==3;btn.style.opacity=libSelectedImages2.length===3?'1':'.4';}
  _updateLibDeleteBtn();
  renderLibrary();
}


async function createVideoFromLibrary() {
  if (selectedImages.length !== 3) { alert('Select exactly 3 images first.'); return; }
  var urls = selectedImages.map(function(s) { return typeof s === 'string' ? s : s.url; });
  await _createFromImages(urls, 'lib-create-btn', 'lib-sel-count', function() { selectedImages = []; });
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

var _renderingTopics = {}; // topicId → true while pipeline is running

function renderTopicsPage() {
  var topics = allTopics;
  if (topicFilter === 'ready') topics = topics.filter(function(t) { return !t.used && t.council_score >= 70; });
  if (topicFilter === 'used')  topics = topics.filter(function(t) { return t.used; });
  if (topicCat !== 'all')      topics = topics.filter(function(t) { return t.cluster === topicCat; });
  setText('topics-count', topics.length + ' topics');
  var el = document.getElementById('topics-list');
  if (!topics.length) { el.innerHTML = '<div class="empty"><span class="empty-icon">\uD83D\uDCEB</span>No topics. Use Replenish to fetch new ones.</div>'; return; }
  el.innerHTML = topics.map(function(t) {
    var cat    = CATS[t.cluster] || null;
    var canGen = !t.used && t.council_score >= 70;
    var isRendering = !!_renderingTopics[t.id];
    var dateStr = t.created_at ? ago(t.created_at) + ' ago' : '';
    var statusBadge = isRendering
      ? '<span style="font-family:var(--mono);font-size:.56rem;background:rgba(255,215,64,.15);color:var(--yellow);padding:1px 6px;border-radius:3px;animation:pulse 1.5s infinite">&#9654; Rendering...</span>'
      : t.used
      ? '<span style="font-family:var(--mono);font-size:.56rem;background:rgba(0,230,118,.1);color:var(--green);padding:1px 6px;border-radius:3px">&#10003; Used</span>'
      : '<span style="font-family:var(--mono);font-size:.56rem;background:rgba(255,82,82,.1);color:var(--red);padding:1px 6px;border-radius:3px">Ready</span>';
    return '<div class="topic-row" id="trow-' + t.id + '">'
      + '<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px">'
      + '<div style="flex:1;min-width:0">'
      + '<div class="topic-text">' + esc(t.topic) + '</div>'
      + '<div style="display:flex;align-items:center;gap:7px;margin-top:4px;flex-wrap:wrap">'
      + '<span class="score-pill ' + scClass(t.council_score) + '">' + t.council_score + '</span>'
      + statusBadge
      + (cat ? '<span style="font-size:.63rem;color:' + cat.color + '">' + cat.emoji + ' ' + cat.label + '</span>' : '')
      + (dateStr ? '<span style="font-family:var(--mono);font-size:.56rem;color:var(--muted)">&#128337; ' + dateStr + '</span>' : '')
      + '</div></div>'
      + '<div style="display:flex;gap:6px;flex-shrink:0;align-items:center">'
      + (canGen && !isRendering ? '<button class="btn btn-primary btn-sm" data-tid="' + t.id + '" onclick="generateNow(this.dataset.tid,this)">&#9654; Now</button>' : '')
      + (canGen && !isRendering ? '<button class="btn btn-sm" style="background:rgba(179,136,255,.12);color:var(--purple);border:1px solid rgba(179,136,255,.25);font-size:.68rem" data-topic="' + esc(t.topic) + '" data-cluster="' + (t.cluster||'AI') + '" onclick="useLfTopic(this.dataset.topic,this.dataset.cluster)">&#127916; LF</button>' : '')
      + (!t.used && !isRendering ? '<button class="btn btn-sm" style="background:rgba(255,82,82,.12);color:var(--red);border:1px solid rgba(255,82,82,.25);padding:4px 8px;font-size:.7rem" data-tid="' + t.id + '" onclick="killTopic(this.dataset.tid,this)" title="Kill this topic">&#10005;</button>' : '')
      + '</div>'
      + '</div></div>';
  }).join('');
}


async function useLfTopic(topic, cluster) {
  if (!confirm('Create Full Auto long-form on:\n\n"' + topic + '"\n\nScript to YouTube automatically.')) return;
  try {
    var r = await fetch(API_BASE + '/longform/create', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic: topic, cluster: cluster, target_duration: 420, auto: true })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Long-form started — check Create page');
  } catch(e) { alert('Failed: ' + e.message); }
}
async function generateNow(topicId, btn) {
  if (!confirm('Generate a video from this topic right now?')) return;
  btn.disabled = true; btn.textContent = '\u23F3 Starting...';
  _renderingTopics[topicId] = true;
  renderTopicsPage(); // immediately show Rendering... badge
  try {
    var r = await fetch(API_BASE + '/run-topic', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic_id: topicId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Pipeline started — rendering in background (~5 min)');
    // Keep rendering badge for 5 minutes then clear
    setTimeout(function() {
      delete _renderingTopics[topicId];
      loadTopicsCount();
      renderTopicsPage();
    }, 5 * 60 * 1000);
    setTimeout(function() { loadJobs(); }, 2000);
  } catch(e) {
    delete _renderingTopics[topicId];
    renderTopicsPage();
    alert('Failed: ' + e.message);
  }
}

async function killTopic(topicId, btn) {
  btn.disabled = true; btn.textContent = '...';
  try {
    var r = await fetch(API_BASE + '/kill-topic', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic_id: topicId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    // Remove the row from DOM immediately
    var row = btn.closest('.topic-row');
    if (row) { row.style.opacity = '0.3'; row.style.pointerEvents = 'none'; }
    allTopics = allTopics.filter(function(t) { return t.id !== topicId; });
    setTimeout(function() { loadTopicsCount(); renderTopicsPage(); }, 400);
  } catch(e) { btn.disabled = false; btn.textContent = '\u2715'; alert('Failed: ' + e.message); }
}

// ── ANALYTICS ───────────────────────────────────────────────────
async function loadAnalytics() {
  var vg = document.getElementById('video-grid');
  if (vg && currentPage === 'analytics') {
    vg.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);font-family:var(--mono);font-size:.75rem">⟳ Syncing with YouTube...</div>';
  }
  // Trigger a sync first, wait for it, then fetch data
  fetch(API_BASE + '/sync-analytics', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      console.log('Analytics sync result:', d);
      if (d.error) console.error('Sync error:', d.error);
      // Re-fetch after sync completes
      return fetch(API_BASE + '/analytics');
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      allAnalytics = d.analytics || [];
      analyticsJobs = d.jobs || [];
      if (currentPage === 'analytics') renderAnalytics();
    })
    .catch(function(e) { console.error('Analytics sync chain failed:', e); });
  try {
    var r = await fetch(API_BASE + '/analytics');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    allAnalytics = d.analytics || [];
    analyticsJobs = d.jobs || [];
    if (currentPage === 'analytics') renderAnalytics();
    // Re-fetch after 10s to get synced numbers
    if (currentPage === 'analytics') setTimeout(function() {
      fetch(API_BASE + '/analytics').then(function(r) { return r.json(); }).then(function(d) {
        allAnalytics = d.analytics || []; renderAnalytics();
      }).catch(()=>{});
    }, 10000);
  } catch(e) {
    if (vg && currentPage === 'analytics') {
      vg.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.75rem;padding:20px">Analytics error: ' + e.message + '</div>';
    }
  }
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
    var hasYt = r.youtube_id && r.youtube_id !== 'TEST_MODE';
    var cat = CATS[r.cluster] || { color:'var(--muted)', emoji:'📹', label: r.cluster || '' };
    return '<div class="video-card"><div class="video-thumb" style="font-size:1.5rem;display:flex;align-items:center;justify-content:center;background:var(--surface2)">' + cat.emoji + '</div><div class="video-body">'
      + '<div class="video-topic">' + (r.topic || 'Unknown') + '</div>'
      + '<div style="font-size:.6rem;color:' + cat.color + ';margin-bottom:4px">' + cat.label + '</div>'
      + '<div class="video-stats"><span>👁 <b>' + fmt(r.youtube_views || 0) + '</b></span>'
      + '<span>❤ <b>' + fmt(r.youtube_likes || 0) + '</b></span>'
      + '<span>💬 <b>' + fmt(r.comment_count || 0) + '</b></span></div>'
      + '<div style="font-family:var(--mono);font-size:.68rem;font-weight:600;color:var(--yellow)">Score: ' + fmt(r.score || 0) + '</div>'
      + (hasYt ? '<a class="video-link" href="https://youtube.com/watch?v=' + r.youtube_id + '" target="_blank">▶ Watch</a>' : '<span style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">Not published yet</span>')
      + '</div></div>';
  }).join('');
  function perfRow(r) {
    return '<div class="perf-row"><div class="perf-topic">' + (r.topic || '-') + '</div>'
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
  ['failed','history','modal'].forEach(function(n) {
    var t = document.getElementById('ltab-' + n);
    if (t) t.classList.toggle('active', n === name);
  });
  if (name === 'modal') loadModalLogs();
  else renderLogs();
}

async function loadModalLogs(jobId) {
  var el = document.getElementById('logs-list');
  if (el) el.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted);font-family:var(--mono);font-size:.75rem">Loading Modal logs...</div>';
  try {
    var qs = jobId ? '?job_id=' + jobId + '&limit=100' : '?limit=100';
    var r  = await fetch(API_BASE + '/modal-logs' + qs);
    var d  = await r.json();
    var logs = d.logs || [];
    if (!logs.length) {
      if (el) el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);font-family:var(--mono);font-size:.75rem">No logs found.</div>';
      return;
    }
    if (el) el.innerHTML = '<div style="font-family:var(--mono);font-size:.68rem;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;max-height:70vh;overflow-y:auto">'
      + logs.map(function(l) {
          var ts = l.created_at ? new Date(l.created_at).toLocaleTimeString() : '';
          var isErr = /fail|error|exception/i.test(l.message);
          var isOk  = /✓|complete|success|done|published/i.test(l.message);
          var col   = isErr ? 'var(--red)' : isOk ? 'var(--green)' : 'var(--text)';
          return '<div style="padding:2px 0;border-bottom:1px solid var(--border);color:' + col + '">'
            + '<span style="color:var(--muted);margin-right:8px">' + ts + '</span>'
            + (l.job_id ? '<span style="color:var(--accent);margin-right:6px">[' + l.job_id.slice(0,8) + ']</span>' : '')
            + esc(l.message) + '</div>';
        }).join('')
      + '</div>';
  } catch(e) {
    if (el) el.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:10px">Error: ' + e.message + '</div>';
  }
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
  // Newest first
  jobs = jobs.slice().sort(function(a,b) { return new Date(b.updated_at||b.created_at) - new Date(a.updated_at||a.created_at); });
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

function setLfMode(mode) {
  var autoBtn   = document.getElementById('lf-mode-auto');
  var manualBtn = document.getElementById('lf-mode-manual');
  var hidden    = document.getElementById('lf-auto-mode');
  if (!autoBtn || !manualBtn || !hidden) return;
  var isAuto = mode === 'auto';
  hidden.value = isAuto ? 'true' : 'false';
  autoBtn.style.borderColor  = isAuto ? 'var(--accent)' : 'var(--border)';
  autoBtn.style.background   = isAuto ? 'rgba(0,229,255,.08)' : 'transparent';
  manualBtn.style.borderColor= isAuto ? 'var(--border)' : 'var(--green)';
  manualBtn.style.background = isAuto ? 'transparent' : 'rgba(0,230,118,.08)';
}

async function createLongformJob() {
  var topic   = (document.getElementById('lf-topic').value || '').trim();
  var cluster = document.getElementById('lf-cluster').value || 'Space';
  var modeEl  = document.getElementById('lf-auto-mode');
  var autoMode = modeEl ? modeEl.value !== 'false' : true;
  if (!topic) { alert('Enter a topic first'); return; }

  // Confirmation for full auto
  if (autoMode) {
    if (!confirm('Full Auto will generate script, images, voice, render and publish to YouTube automatically.\n\nStart for topic: "' + topic + '"?')) return;
  }

  var st = document.getElementById('lf-create-status');
  st.textContent = autoMode ? '🤖 Full Auto starting...' : '🎨 Creating manual job...';
  st.style.color = 'var(--muted)';
  try {
    var r = await fetch(API_BASE + '/longform/create', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ topic, cluster, target_duration: lfDurMins * 60, auto: autoMode })
    });
    var d = await r.json();
    if (d.job_id) {
      st.textContent = autoMode
        ? '✓ Full Auto running — script → images → voice → render → YouTube (~15 min)'
        : '✓ Job created — open Studio to pick library images per segment';
      st.style.color = 'var(--green)';
      document.getElementById('lf-topic').value = '';
      setTimeout(function() { loadLongformJobs(); st.textContent = ''; }, 6000);
    } else {
      st.textContent = 'Error: ' + (d.error || 'unknown');
      st.style.color = 'var(--red)';
    }
  } catch(e) {
    st.textContent = 'Error: ' + e.message;
    st.style.color = 'var(--red)';
  }
}

var _lfPollTimer = null;

async function loadLfTopicIdeas() {
  var el  = document.getElementById('lf-topic-ideas');
  var st  = document.getElementById('lf-ideas-status');
  var cat = document.getElementById('lf-cluster');
  var cluster = cat ? cat.value : 'Space';
  if (el) el.innerHTML = '<span style="font-family:var(--mono);font-size:.65rem;color:var(--muted)">Generating ideas...</span>';
  if (st) { st.textContent = 'Loading...'; st.style.color = 'var(--muted)'; }
  try {
    var r = await fetch(API_BASE + '/longform/topic-ideas?cluster=' + cluster);
    var d = await r.json();
    var ideas = d.ideas || [];
    if (!ideas.length) {
      if (el) el.innerHTML = '<span style="font-family:var(--mono);font-size:.65rem;color:var(--muted)">No ideas — try a different cluster</span>';
      if (st) st.textContent = '';
      return;
    }
    if (el) el.innerHTML = ideas.map(function(idea) {
      return '<button onclick="useLfIdea(this)" data-topic="' + esc(idea.topic) + '" data-cluster="' + esc(idea.cluster||cluster) + '" '
        + 'style="background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:5px 10px;'
        + 'font-size:.68rem;cursor:pointer;color:var(--text);text-align:left">'
        + esc(idea.topic) + '</button>';
    }).join('');
    if (st) { st.textContent = ideas.length + ' ideas'; st.style.color = 'var(--green)'; }
  } catch(e) {
    if (el) el.innerHTML = '<span style="font-family:var(--mono);font-size:.65rem;color:var(--red)">Error: ' + e.message + '</span>';
    if (st) st.textContent = '';
  }
}

function useLfIdea(btn) {
  var topicEl   = document.getElementById('lf-topic');
  var clusterEl = document.getElementById('lf-cluster');
  if (topicEl)   topicEl.value   = btn.dataset.topic;
  if (clusterEl && btn.dataset.cluster) clusterEl.value = btn.dataset.cluster;
  document.querySelectorAll('#lf-topic-ideas button').forEach(function(b) {
    b.style.borderColor = b === btn ? 'var(--accent)' : 'var(--border)';
    b.style.color = b === btn ? 'var(--accent)' : 'var(--text)';
  });
}

function _lfProgressBar(segs) {
  if (!segs || !segs.length) return '';
  var total    = segs.length;
  var ready    = segs.filter(function(s) { return s.status === 'ready'; }).length;
  var hasVoice = segs.filter(function(s) { return s.voice_r2_url; }).length;
  var hasMedia = segs.filter(function(s) { return (s.media||[]).length > 0; }).length;
  var pct      = Math.round((ready / total) * 100);
  var sColor   = {'ready':'var(--green)','generating_voice':'var(--yellow)','generating_images':'var(--yellow)','has_media':'var(--purple)','has_script':'var(--accent)','empty':'var(--surface2)'};
  return '<div style="margin-top:6px">'
    + '<div style="display:flex;justify-content:space-between;font-family:var(--mono);font-size:.57rem;color:var(--muted);margin-bottom:2px">'
    + '<span>' + ready + '/' + total + ' segments ready</span>'
    + '<span>🎙' + hasVoice + ' 🖼' + hasMedia + '</span>'
    + '</div>'
    + '<div style="height:4px;background:var(--surface3);border-radius:2px;overflow:hidden;margin-bottom:2px">'
    + '<div style="height:100%;width:'+pct+'%;background:var(--accent);border-radius:2px;transition:width .4s"></div>'
    + '</div>'
    + '<div style="display:flex;gap:2px">'
    + segs.map(function(s) {
        var c = sColor[s.status] || 'var(--surface2)';
        return '<div title="'+esc((s.label||s.segment_type||'Seg')+': '+s.status)+'" '
          + 'style="flex:1;height:5px;background:'+c+';border-radius:1px"></div>';
      }).join('')
    + '</div></div>';
}

async function loadLongformJobs() {
  var el = document.getElementById('lf-jobs-list');
  if (!el) return;
  try {
    var r    = await fetch(API_BASE + '/longform/jobs');
    var jobs = await r.json();
    if (!Array.isArray(jobs) || !jobs.length) {
      el.innerHTML = '<div style="text-align:center;padding:30px;color:var(--muted);font-family:var(--mono);font-size:.75rem">No long-form jobs yet.</div>';
      clearTimeout(_lfPollTimer);
      return;
    }

    var active = jobs.filter(function(j) { return !['complete','failed'].includes(j.status); });
    var failed = jobs.filter(function(j) { return j.status === 'failed'; });

    // Fetch segment progress for active jobs
    var segsMap = {};
    await Promise.all(active.map(async function(j) {
      try {
        var sr = await fetch(API_BASE + '/longform/' + j.id);
        var sd = await sr.json();
        segsMap[j.id] = sd.segments || [];
      } catch(e) {}
    }));

    var statusColor = {draft:'#888',scripting:'var(--yellow)',media_collecting:'#4fc3f7',ready_to_render:'var(--purple)',rendering:'var(--accent)',complete:'#69f0ae',failed:'var(--red)'};
    var statusLabel = {scripting:'⏳ Scripting...',media_collecting:'🎬 Building media...',ready_to_render:'✅ Ready to render',rendering:'🎞 Rendering...',complete:'✓ Done',failed:'✗ Failed',draft:'Draft'};

    var clearBtn = failed.length
      ? '<button class="btn btn-sm" style="font-size:.68rem;color:var(--red);margin-bottom:8px" onclick="clearKilledLfJobs()">🗑 Clear '+failed.length+' failed</button><br>'
      : '';

    el.innerHTML = clearBtn + jobs.map(function(j) {
      var col    = statusColor[j.status] || '#888';
      var lbl    = statusLabel[j.status] || j.status;
      var durStr = j.target_duration ? Math.round(j.target_duration/60)+'m' : '?';
      var segs   = segsMap[j.id] || [];
      var prog   = (segs.length && !['complete','failed','draft'].includes(j.status)) ? _lfProgressBar(segs) : '';
      var ytLink = j.youtube_id ? '<a href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank" style="color:#ff0000;font-size:.7rem;text-decoration:none">▶ Watch</a>' : '';
      var errStr = j.error ? '<div style="font-family:var(--mono);font-size:.57rem;color:var(--red);margin-top:2px">'+esc(j.error.slice(0,80))+'</div>' : '';
      return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:8px">'
        + '<div style="display:flex;align-items:center;gap:8px">'
        + '<div style="flex:1;min-width:0">'
        + '<div style="font-size:.82rem;font-weight:600">'+esc(j.topic)+'</div>'
        + '<div style="font-family:var(--mono);font-size:.6rem;color:var(--muted)">'+j.cluster+' &bull; '+durStr+' &bull; '+new Date(j.created_at).toLocaleDateString()+'</div>'
        + errStr + prog
        + '</div>'
        + (ytLink?'<div>'+ytLink+'</div>':'')
        + '<div style="font-family:var(--mono);font-size:.65rem;color:'+col+';font-weight:700;white-space:nowrap">'+lbl+'</div>'
        + '<button class="btn btn-sm" style="font-size:.68rem" data-jid="'+j.id+'" onclick="openLfStudio(this.dataset.jid)">Studio</button>'
        + (['failed','scripting'].includes(j.status)?'<button class="btn btn-sm" style="font-size:.68rem;color:var(--accent)" data-jid="'+j.id+'" data-topic="'+esc(j.topic)+'" data-cluster="'+j.cluster+'" data-dur="'+(j.target_duration||420)+'" onclick="retryLfScript(this)">↺</button>':'')
        + '<button class="btn btn-sm" style="font-size:.68rem;color:var(--muted)" data-jid="'+j.id+'" onclick="viewJobLogs(this.dataset.jid)">📄</button>'
        + '<button class="btn btn-sm" style="font-size:.68rem;color:var(--red)" data-jid="'+j.id+'" onclick="killLfJob(this.dataset.jid,this)">&times;</button>'
        + '</div></div>';
    }).join('');

    // Auto-poll every 10s if jobs are in progress
    clearTimeout(_lfPollTimer);
    if (active.length) _lfPollTimer = setTimeout(loadLongformJobs, 10000);

  } catch(e) {
    el.innerHTML = '<div style="color:var(--red);font-family:var(--mono);font-size:.72rem;padding:10px">Error: '+e.message+'</div>';
  }
}

async function clearKilledLfJobs() {
  if (!confirm('Permanently delete all killed/failed longform jobs?')) return;
  try {
    var r = await fetch(API_BASE + '/longform/clear-failed', { method: 'POST' });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Cleared ' + (d.deleted || 0) + ' failed jobs');
    loadLongformJobs();
  } catch(e) { alert('Failed: ' + e.message); }
}

async function killLfJob(jobId, btn) {
  if (!confirm('Kill this longform job?')) return;
  if (btn) { btn.disabled = true; btn.textContent = '...'; }
  try {
    var r = await fetch(API_BASE + '/longform/kill', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Job killed');
    loadLongformJobs();
  } catch(e) {
    if (btn) { btn.disabled = false; btn.textContent = '\u00d7 Kill'; }
    alert('Failed: ' + e.message);
  }
}

async function retryLfScript(btn) {
  var jobId  = btn.dataset.jid;
  var topic  = btn.dataset.topic;
  var cluster= btn.dataset.cluster;
  var dur    = parseInt(btn.dataset.dur) || 420;
  btn.disabled = true; btn.textContent = '...';
  try {
    var r = await fetch(API_BASE + '/longform/retry-script', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: jobId, topic, cluster, target_duration: dur })
    });
    var d = await r.json();
    if (d.error) throw new Error(d.error);
    showToast('Script generation restarted');
    setTimeout(loadLongformJobs, 2000);
  } catch(e) {
    btn.disabled = false; btn.textContent = '\u8635 Retry';
    alert('Failed: ' + e.message);
  }
}

function viewJobLogs(jobId) {
  openLogs();
  logsTab = 'modal';
  ['failed','history','modal'].forEach(function(n) {
    var t = document.getElementById('ltab-' + n);
    if (t) t.classList.toggle('active', n === 'modal');
  });
  loadModalLogs(jobId);
}

var _lfLibSegIdx = null;
var _lfLibSelected = [];

// ── THEME ───────────────────────────────────────────────────────
function setTheme(mode) {
  if (mode === 'light') {
    document.body.classList.add('light-mode');
    localStorage.setItem('i20_theme', 'light');
  } else {
    document.body.classList.remove('light-mode');
    localStorage.setItem('i20_theme', 'dark');
  }
  var darkBtn  = document.getElementById('theme-dark-btn');
  var lightBtn = document.getElementById('theme-light-btn');
  if (darkBtn)  darkBtn.className  = 'btn btn-sm ' + (mode === 'dark'  ? 'btn-primary' : 'btn-ghost');
  if (lightBtn) lightBtn.className = 'btn btn-sm ' + (mode === 'light' ? 'btn-primary' : 'btn-ghost');
}

// Init theme from localStorage immediately on script load
(function() {
  var saved = localStorage.getItem('i20_theme') || 'dark';
  if (saved === 'light') document.body.classList.add('light-mode');
})();