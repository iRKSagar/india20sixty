export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return json(null, 204);

    if (url.pathname === "/" || url.pathname === "/dashboard") {
      return new Response(DASHBOARD_HTML, {
        headers: { "content-type": "text/html;charset=UTF-8", "cache-control": "no-store" }
      });
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const body     = await request.json().catch(() => ({}));
        const category = body.category || null;
        const topicData = await pickTopic(env, category);
        const job = await createJob(topicData, env);
        ctx.waitUntil(triggerRender(job, env));
        return json({ status: "job_created", job_id: job.id, topic: topicData.topic,
                      category: topicData.category, council_score: topicData.council_score });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/jobs") {
      try { return json(await supabaseGet(env, "jobs?order=created_at.desc&limit=50")); }
      catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/topics") {
      try { return json(await supabaseGet(env, "topics?order=council_score.desc&limit=100")); }
      catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/analytics") {
      try {
        const analytics = await supabaseGet(env, "analytics?order=score.desc&limit=50");
        const jobs      = await supabaseGet(env, "jobs?status=eq.complete&select=id,topic,council_score,youtube_id,cluster,created_at");
        return json({ analytics, jobs });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/generate-topic" && request.method === "POST") {
      try {
        const body   = await request.json();
        const result = await callTopicCouncil(env, body.topic || "Future of AI in India",
                                              "manual", body.category);
        return json(result);
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/restore-failed" && request.method === "POST") {
      try {
        const failed = await supabaseGet(env, "jobs?status=eq.failed&select=id,topic,council_score,script_package,cluster");
        if (!failed.length) return json({ restored: 0 });
        let restored = 0, already = 0;
        for (const job of failed) {
          if (!job.topic) continue;
          try {
            const ex = await supabaseGet(env, `topics?topic=eq.${encodeURIComponent(job.topic)}&select=id,used`);
            if (ex.length > 0) {
              if (ex[0].used) { await supabasePatch(env, `topics?id=eq.${ex[0].id}`, { used: false, used_at: null }); restored++; }
              else already++;
            } else {
              await supabaseInsert(env, "topics", {
                cluster: job.cluster || "AI", topic: job.topic, used: false,
                council_score: job.council_score || 75,
                script_package: job.script_package || null,
                source: "restored_from_failed", created_at: new Date().toISOString()
              });
              restored++;
            }
          } catch (e) { console.error("Restore:", e.message); }
        }
        return json({ restored, already_in_queue: already, total_failed: failed.length });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/kill-incomplete" && request.method === "POST") {
      try {
        const stuck = await supabaseGet(env,
          "jobs?status=in.(pending,processing,images,voice,render,upload)&select=id,topic,council_score,cluster");
        if (!stuck.length) return json({ killed: 0, topics_restored: 0 });
        let topicsRestored = 0;
        for (const job of stuck) {
          await supabasePatch(env, `jobs?id=eq.${job.id}`, {
            status: "failed", error: "manually_killed", updated_at: new Date().toISOString()
          });
          if (job.topic) {
            try {
              const ex = await supabaseGet(env, `topics?topic=eq.${encodeURIComponent(job.topic)}&select=id,used`);
              if (ex.length > 0 && ex[0].used) {
                await supabasePatch(env, `topics?id=eq.${ex[0].id}`, { used: false, used_at: null });
                topicsRestored++;
              }
            } catch (e) {}
          }
        }
        return json({ killed: stuck.length, topics_restored: topicsRestored });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/test-render") {
      const healthUrl = env.MODAL_HEALTH_URL || "NOT_SET";
      try {
        const r    = await fetch(healthUrl, { signal: AbortSignal.timeout(15000) });
        const text = await r.text();
        return json({ url: healthUrl, status: r.status, response: text.slice(0, 400), ok: r.ok });
      } catch (e) { return json({ url: healthUrl, error: e.message, ok: false }); }
    }

    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const data = await request.json();
        const { job_id, status, youtube_id, error, script } = data;
        if (!job_id) return json({ error: "Missing job_id" }, 400);
        const u = { status: status || "unknown", updated_at: new Date().toISOString() };
        if (youtube_id) u.youtube_id = youtube_id;
        if (error)      u.error = error;
        if (script)     u.script_package = { text: script };
        await supabasePatch(env, `jobs?id=eq.${job_id}`, u);
        if (status === "complete" && youtube_id && youtube_id !== "TEST_MODE")
          ctx.waitUntil(createAnalyticsRecord(job_id, youtube_id, env));
        return json({ received: true, job_id, status });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    if (url.pathname === "/sync-analytics"  && request.method === "POST") { ctx.waitUntil(syncYouTubeAnalytics(env)); return json({ status: "sync_started" }); }
    if (url.pathname === "/replenish"        && request.method === "POST") {
      const body       = await request.json().catch(() => ({}));
      const categories = body.categories || null;
      const target     = body.target || 12;
      ctx.waitUntil(triggerReplenish(env, target, categories));
      return json({ status: "replenish_triggered", categories, target });
    }

    return json({ error: "route_not_found" }, 404);
  },

  async scheduled(event, env, ctx) {
    const cron = event.cron;

    if (cron === "* * * * *") {
      await processQueue(env, ctx);
      if (env.MODAL_HEALTH_URL)     fetch(env.MODAL_HEALTH_URL).catch(() => {});
      if (env.TOPIC_COUNCIL_URL)    fetch(env.TOPIC_COUNCIL_URL + "/health").catch(() => {});
    }

    if (cron === "30 0,6,12 * * *") {
      try {
        const t = await pickTopic(env, null);
        const j = await createJob(t, env);
        ctx.waitUntil(triggerRender(j, env));
        console.log("Scheduled:", j.id, j.topic);
      } catch (e) { console.error("Scheduled failed:", e.message); }
    }

    if (cron === "30 20 * * *") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      try {
        const av = await supabaseGet(env, "topics?used=eq.false&council_score=gte.70&select=id");
        console.log("Queue depth:", av.length);
        if (av.length < 5) ctx.waitUntil(triggerReplenish(env, 12, null));
      } catch (e) { console.error("Queue check:", e.message); }
    }
  }
};

// ── CATEGORIES ────────────────────────────────────────────────────

const CATEGORIES = {
  AI:        { label: "AI & ML",          color: "#00e5ff", emoji: "🤖" },
  Space:     { label: "Space & Defence",  color: "#b388ff", emoji: "🚀" },
  Gadgets:   { label: "Gadgets & Tech",   color: "#ffd740", emoji: "📱" },
  DeepTech:  { label: "Deep Tech",        color: "#ff6b35", emoji: "🔬" },
  GreenTech: { label: "Green & Energy",   color: "#00e676", emoji: "⚡" },
  Startups:  { label: "Startups",         color: "#ff6b9d", emoji: "💡" },
};
const ALL_CATS = Object.keys(CATEGORIES);

// ── TOPIC SELECTION ───────────────────────────────────────────────

async function pickTopic(env, preferCategory = null) {
  let endpoint = "topics?used=eq.false&council_score=gte.70&order=council_score.desc&limit=1";
  if (preferCategory && ALL_CATS.includes(preferCategory))
    endpoint = `topics?used=eq.false&council_score=gte.70&cluster=eq.${preferCategory}&order=council_score.desc&limit=1`;

  const t = await supabaseGet(env, endpoint);
  if (t.length > 0) {
    await supabasePatch(env, `topics?id=eq.${t[0].id}`,
      { used: true, used_at: new Date().toISOString() });
    return { topic: t[0].topic, script_package: t[0].script_package,
             council_score: t[0].council_score, category: t[0].cluster || "AI",
             source: "db_approved" };
  }
  return await generateViaCouncil(env);
}

async function generateViaCouncil(env) {
  const pool = [
    "India's AI healthcare revolution in rural areas",
    "ISRO's next space mission changing everything",
    "India's EV revolution — what's actually happening",
    "AI chips made in India — the semiconductor story",
    "India's solar energy breakthrough",
    "Startups solving India's biggest problems with AI",
  ];
  const topic = pool[Math.floor(Math.random() * pool.length)];
  try {
    const r = await callTopicCouncil(env, topic, "auto_generated", null);
    if (r.status === "approved")
      return { topic: r.topic || topic, script_package: r.script || null,
               council_score: r.evaluation?.council_score || 75,
               category: r.category || "AI", source: "council_generated" };
  } catch (e) { console.error("Council fallback:", e.message); }
  return { topic: "India's AI revolution", script_package: null,
           council_score: 0, category: "AI", source: "fallback" };
}

async function callTopicCouncil(env, topic, source, category) {
  if (!env.TOPIC_COUNCIL_URL) throw new Error("TOPIC_COUNCIL_URL not set");
  const r = await fetch(env.TOPIC_COUNCIL_URL + "/full-pipeline", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ topic, source, category })
  });
  if (!r.ok) throw new Error("Council returned " + r.status);
  return r.json();
}

async function triggerReplenish(env, target = 12, categories = null) {
  if (!env.TOPIC_COUNCIL_URL) return;
  try {
    const r = await fetch(env.TOPIC_COUNCIL_URL + "/replenish", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ target, categories: categories || ALL_CATS })
    });
    console.log("Replenish:", r.status, (await r.text()).slice(0, 100));
  } catch (e) { console.error("Replenish failed:", e.message); }
}

// ── JOB MANAGEMENT ────────────────────────────────────────────────

async function createJob(t, env) {
  return await supabaseInsert(env, "jobs", {
    topic: t.topic, cluster: t.category || "AI", status: "pending",
    script_package: t.script_package || null,
    council_score: t.council_score || 0,
    retries: 0, created_at: new Date().toISOString(), updated_at: new Date().toISOString()
  });
}

async function processQueue(env, ctx) {
  const ago = new Date(Date.now() - 15 * 60000).toISOString();
  try {
    for (const j of await supabaseGet(env, `jobs?status=eq.processing&updated_at=lt.${ago}&retries=lt.3`))
      await supabasePatch(env, `jobs?id=eq.${j.id}`,
        { status: "pending", retries: (j.retries || 0) + 1, updated_at: new Date().toISOString() });
    for (const j of await supabaseGet(env, `jobs?status=eq.processing&updated_at=lt.${ago}&retries=gte.3`))
      await supabasePatch(env, `jobs?id=eq.${j.id}`,
        { status: "failed", error: "max_retries_exceeded", updated_at: new Date().toISOString() });
    const pending = await supabaseGet(env, "jobs?status=eq.pending&order=created_at.asc&limit=1");
    if (!pending.length) return;
    await supabasePatch(env, `jobs?id=eq.${pending[0].id}`,
      { status: "processing", started_at: new Date().toISOString(), updated_at: new Date().toISOString() });
    ctx.waitUntil(triggerRender(pending[0], env));
  } catch (e) { console.error("Queue:", e.message); }
}

async function triggerRender(job, env) {
  if (!env.RENDER_PIPELINE_URL) {
    await supabasePatch(env, `jobs?id=eq.${job.id}`,
      { status: "failed", error: "RENDER_PIPELINE_URL not set", updated_at: new Date().toISOString() });
    return;
  }
  const renderUrl = env.RENDER_PIPELINE_URL.trim().replace(/\/$/, "");
  try {
    const r = await fetch(renderUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        job_id: job.id, topic: job.topic,
        script_package: job.script_package,
        webhook_url: (env.WORKER_URL || "").trim().replace(/\/$/, "") + "/webhook"
      }),
      signal: AbortSignal.timeout(60000)
    });
    if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).slice(0,100)}`);
  } catch (e) {
    console.error("Render trigger:", e.message);
    await supabasePatch(env, `jobs?id=eq.${job.id}`,
      { status: "failed", error: e.message, updated_at: new Date().toISOString() });
  }
}

// ── ANALYTICS ─────────────────────────────────────────────────────

async function createAnalyticsRecord(job_id, youtube_id, env) {
  try { await supabaseInsert(env, "analytics",
    { video_id: job_id, youtube_views: 0, youtube_likes: 0,
      comment_count: 0, score: 0, created_at: new Date().toISOString() }); }
  catch (e) {}
}

async function syncYouTubeAnalytics(env) {
  if (!env.YOUTUBE_CLIENT_ID) return;
  try {
    const jobs = (await supabaseGet(env,
      "jobs?status=eq.complete&youtube_id=not.is.null&order=created_at.desc&limit=50"))
      .filter(j => j.youtube_id && j.youtube_id !== "TEST_MODE");
    if (!jobs.length) return;
    const r = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ client_id: env.YOUTUBE_CLIENT_ID,
        client_secret: env.YOUTUBE_CLIENT_SECRET,
        refresh_token: env.YOUTUBE_REFRESH_TOKEN, grant_type: "refresh_token" })
    });
    if (!r.ok) return;
    const token = (await r.json()).access_token;
    for (let i = 0; i < jobs.length; i += 50) {
      const batch = jobs.slice(i, i+50);
      const res   = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${batch.map(j=>j.youtube_id).join(",")}&access_token=${token}`);
      if (!res.ok) continue;
      for (const item of (await res.json()).items || []) {
        const s = item.statistics || {};
        const views = parseInt(s.viewCount||0), likes = parseInt(s.likeCount||0),
              comments = parseInt(s.commentCount||0), score = views + likes*50 + comments*30;
        const job = batch.find(j => j.youtube_id === item.id);
        if (!job) continue;
        const ex = await supabaseGet(env, `analytics?video_id=eq.${job.id}`);
        if (ex.length > 0) await supabasePatch(env, `analytics?video_id=eq.${job.id}`,
          { youtube_views: views, youtube_likes: likes, comment_count: comments, score,
            updated_at: new Date().toISOString() });
        else await supabaseInsert(env, "analytics",
          { video_id: job.id, youtube_views: views, youtube_likes: likes,
            comment_count: comments, score, created_at: new Date().toISOString() });
      }
    }
  } catch (e) { console.error("Analytics sync:", e.message); }
}

// ── SUPABASE ──────────────────────────────────────────────────────

function sbh(env) { return { apikey: env.SUPABASE_ANON_KEY, Authorization: "Bearer " + (env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_ANON_KEY), "Content-Type": "application/json" }; }
async function supabaseGet(env, ep) { const r = await fetch(env.SUPABASE_URL+"/rest/v1/"+ep, {headers: sbh(env)}); if(!r.ok) throw new Error("GET "+r.status+" "+ep); return r.json(); }
async function supabaseInsert(env, table, data) { const r = await fetch(env.SUPABASE_URL+"/rest/v1/"+table, {method:"POST",headers:{...sbh(env),Prefer:"return=representation"},body:JSON.stringify(data)}); if(!r.ok){const b=await r.text();throw new Error("INSERT "+r.status+" "+b.slice(0,200));} return (await r.json())[0]; }
async function supabasePatch(env, ep, data) { const r = await fetch(env.SUPABASE_URL+"/rest/v1/"+ep, {method:"PATCH",headers:{...sbh(env),Prefer:"return=minimal"},body:JSON.stringify(data)}); return r.ok; }
function json(data, status) { return new Response(JSON.stringify(data,null,2), {status:status||200,headers:{"content-type":"application/json","Access-Control-Allow-Origin":"*","Access-Control-Allow-Headers":"*","Access-Control-Allow-Methods":"GET,POST,OPTIONS,PATCH"}}); }

// ── DASHBOARD ─────────────────────────────────────────────────────

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>India20Sixty — Mission Control</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{--bg:#080c14;--surface:#0d1320;--surface2:#111827;--border:rgba(255,255,255,0.07);--border2:rgba(255,255,255,0.14);--text:#e8eaf0;--muted:#5a6278;--accent:#00e5ff;--accent2:#ff6b35;--green:#00e676;--yellow:#ffd740;--red:#ff5252;--purple:#b388ff;--font:'Syne',sans-serif;--mono:'DM Mono',monospace}
*{margin:0;padding:0;box-sizing:border-box}html,body{height:100%}
body{background:var(--bg);color:var(--text);font-family:var(--font);display:flex;flex-direction:column;overflow:hidden}
body::before{content:'';position:fixed;inset:0;z-index:0;background-image:linear-gradient(rgba(0,229,255,0.02) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,255,0.02) 1px,transparent 1px);background-size:44px 44px;pointer-events:none}
.topbar{position:relative;z-index:10;display:flex;align-items:center;justify-content:space-between;padding:0 28px;height:56px;border-bottom:1px solid var(--border);background:rgba(8,12,20,0.95);backdrop-filter:blur(10px);flex-shrink:0}
.logo-name{font-size:1.15rem;font-weight:800;letter-spacing:-0.02em}.logo-name span{color:var(--accent)}
.logo-sub{font-family:var(--mono);font-size:.6rem;color:var(--muted);letter-spacing:.14em;text-transform:uppercase;margin-top:2px}
.topbar-nav{display:flex;align-items:center;gap:4px}
.nav-btn{display:flex;align-items:center;gap:6px;padding:6px 16px;border-radius:7px;border:none;background:transparent;font-family:var(--font);font-size:.82rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s}
.nav-btn:hover{color:var(--text);background:rgba(255,255,255,.05)}.nav-btn.active{color:var(--text);background:var(--surface2);border:1px solid var(--border2)}
.live-dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}
.live-lbl{font-family:var(--mono);font-size:.68rem;color:var(--green)}
.pages{flex:1;overflow:hidden;position:relative;z-index:1}
.page{display:none;height:100%;overflow-y:auto;padding:28px}.page.active{display:block}
.page::-webkit-scrollbar{width:4px}.page::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:18px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden}
.stat::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:10px 10px 0 0}
.s1::before{background:var(--accent)}.s2::before{background:var(--yellow)}.s3::before{background:var(--green)}.s4::before{background:var(--red)}.s5::before{background:var(--purple)}
.stat-val{font-size:1.9rem;font-weight:800;letter-spacing:-0.04em;line-height:1;margin-bottom:3px}
.s1 .stat-val{color:var(--accent)}.s2 .stat-val{color:var(--yellow)}.s3 .stat-val{color:var(--green)}.s4 .stat-val{color:var(--red)}.s5 .stat-val{color:var(--purple)}
.stat-lbl{font-family:var(--mono);font-size:.63rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}

/* Category pills */
.cat-strip{display:flex;gap:8px;margin-bottom:18px;flex-wrap:wrap}
.cat-pill{display:flex;align-items:center;gap:5px;padding:6px 12px;border-radius:20px;border:1px solid var(--border2);background:var(--surface);font-family:var(--mono);font-size:.68rem;cursor:pointer;transition:all .15s;color:var(--muted)}
.cat-pill:hover{border-color:var(--border2);color:var(--text)}.cat-pill.active{color:#000;font-weight:600}
.cat-count{font-size:.6rem;opacity:.8}

.actions{display:flex;gap:8px;margin-bottom:18px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:6px;padding:9px 18px;border-radius:8px;border:none;font-family:var(--font);font-size:.82rem;font-weight:600;cursor:pointer;transition:all .18s;white-space:nowrap}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn-primary{background:var(--accent);color:#000}.btn-primary:hover{filter:brightness(1.15);transform:translateY(-1px)}
.btn-ghost{background:var(--surface2);color:var(--text);border:1px solid var(--border2)}.btn-ghost:hover{border-color:var(--accent);color:var(--accent);transform:translateY(-1px)}
.btn-purple{background:rgba(179,136,255,.1);color:var(--purple);border:1px solid rgba(179,136,255,.2)}.btn-purple:hover{background:rgba(179,136,255,.18);transform:translateY(-1px)}
.btn-orange{background:rgba(255,107,53,.1);color:var(--accent2);border:1px solid rgba(255,107,53,.2)}.btn-orange:hover{background:rgba(255,107,53,.18);transform:translateY(-1px)}
.btn-red{background:rgba(255,82,82,.08);color:var(--red);border:1px solid rgba(255,82,82,.2)}.btn-red:hover{background:rgba(255,82,82,.18);transform:translateY(-1px)}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:18px}
.panel-head{padding:13px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.panel-title{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.panel-sub{font-family:var(--mono);font-size:.63rem;color:var(--muted)}
.tabs{display:flex;gap:3px;padding:10px 14px;border-bottom:1px solid var(--border)}
.tab{display:flex;align-items:center;gap:6px;padding:6px 12px;border-radius:6px;border:1px solid transparent;background:transparent;font-family:var(--font);font-size:.77rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s}
.tab:hover{color:var(--text);background:var(--surface2)}.tab.active{color:var(--text);background:var(--surface2);border-color:var(--border2)}
.tab-count{font-family:var(--mono);font-size:.62rem;padding:1px 6px;border-radius:20px}
.tc-all{background:rgba(0,229,255,.12);color:var(--accent)}.tc-run{background:rgba(255,215,64,.12);color:var(--yellow)}.tc-ok{background:rgba(0,230,118,.12);color:var(--green)}.tc-fail{background:rgba(255,82,82,.12);color:var(--red)}
.job-item{display:grid;grid-template-columns:1fr 110px 160px 72px;gap:12px;padding:12px 18px;border-bottom:1px solid var(--border);align-items:center;transition:background .12s}
.job-item:hover{background:rgba(255,255,255,.02)}.job-item:last-child{border-bottom:none}
.job-topic{font-size:.84rem;font-weight:600;color:var(--text);line-height:1.3;margin-bottom:3px}
.job-meta{font-family:var(--mono);font-size:.63rem;color:var(--muted);display:flex;gap:7px;align-items:center;flex-wrap:wrap}
.job-err{color:#ff6b6b;max-width:240px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:5px;font-family:var(--mono);font-size:.65rem;font-weight:500;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.bdot{width:5px;height:5px;border-radius:50%;background:currentColor}
.b-pending{background:rgba(255,215,64,.1);color:var(--yellow);border:1px solid rgba(255,215,64,.2)}
.b-processing,.b-upload{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.2)}
.b-images,.b-voice{background:rgba(179,136,255,.1);color:var(--purple);border:1px solid rgba(179,136,255,.2)}
.b-render{background:rgba(255,107,53,.1);color:var(--accent2);border:1px solid rgba(255,107,53,.2)}
.b-complete,.b-test_complete{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.b-failed{background:rgba(255,82,82,.1);color:var(--red);border:1px solid rgba(255,82,82,.2)}
.b-pending .bdot,.b-processing .bdot{animation:blink 1.4s infinite}
.prog-wrap{display:flex;flex-direction:column;gap:4px}
.prog-bar{height:3px;background:rgba(255,255,255,.06);border-radius:2px;overflow:hidden}
.prog-fill{height:100%;border-radius:2px;transition:width .6s}
.prog-pct{font-family:var(--mono);font-size:.6rem;color:var(--muted)}
.time-cell{font-family:var(--mono);font-size:.66rem;color:var(--muted);text-align:right}
.yt-link{color:var(--accent);text-decoration:none;font-family:var(--mono);font-size:.65rem}.yt-link:hover{color:#fff}
.empty{padding:44px 20px;text-align:center;color:var(--muted);font-family:var(--mono);font-size:.74rem;line-height:1.9}
.empty-icon{font-size:1.8rem;display:block;margin-bottom:10px;opacity:.3}
.two-col{display:grid;grid-template-columns:1fr 300px;gap:18px;align-items:start}
.topic-row{padding:13px 18px;border-bottom:1px solid var(--border);transition:background .12s}
.topic-row:hover{background:rgba(255,255,255,.02)}.topic-row:last-child{border-bottom:none}
.topic-text{font-size:.82rem;font-weight:600;color:var(--text);line-height:1.4;margin-bottom:5px}
.topic-foot{display:flex;align-items:center;justify-content:space-between}
.score-pill{font-family:var(--mono);font-size:.63rem;font-weight:500;padding:2px 7px;border-radius:4px}
.sc-hi{background:rgba(0,230,118,.12);color:var(--green)}.sc-med{background:rgba(255,215,64,.12);color:var(--yellow)}.sc-lo{background:rgba(255,82,82,.12);color:var(--red)}
.src-tag{font-family:var(--mono);font-size:.6rem;color:var(--muted)}
.analytics-hero{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.hero-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;text-align:center}
.hero-val{font-size:2.2rem;font-weight:800;letter-spacing:-0.04em;margin-bottom:4px}
.hero-lbl{font-family:var(--mono);font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}
.hv-views{color:#60b4ff}.hv-likes{color:#ff6b9d}.hv-comments{color:var(--yellow)}.hv-score{color:var(--accent)}
.perf-row{display:grid;grid-template-columns:1fr 72px 72px 90px;gap:12px;padding:11px 16px;border-bottom:1px solid var(--border);align-items:center}
.perf-row:last-child{border-bottom:none}
.perf-topic{font-size:.8rem;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.perf-num{font-family:var(--mono);font-size:.7rem;text-align:right}
.pn-views{color:#60b4ff}.pn-likes{color:#ff6b9d}.pn-score{color:var(--yellow);font-weight:600}
.video-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;transition:border-color .2s}
.video-card:hover{border-color:var(--border2)}
.video-thumb{width:100%;aspect-ratio:9/16;background:var(--surface2);display:flex;align-items:center;justify-content:center;font-size:2rem;opacity:.3;max-height:120px}
.video-body{padding:12px}
.video-topic{font-size:.8rem;font-weight:600;color:var(--text);line-height:1.35;margin-bottom:6px}
.video-stats{display:flex;gap:10px;font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-bottom:6px}
.video-stats span b{color:var(--text)}
.video-score{font-family:var(--mono);font-size:.72rem;font-weight:600;color:var(--yellow)}
.video-link{display:inline-flex;align-items:center;gap:4px;color:var(--accent);text-decoration:none;font-family:var(--mono);font-size:.65rem;margin-top:4px}
.used-pill{font-family:var(--mono);font-size:.6rem;padding:1px 6px;border-radius:3px;background:rgba(0,230,118,.1);color:var(--green)}
.used-no{background:rgba(255,82,82,.1);color:var(--red)}
.debug-box{background:rgba(0,0,0,.5);border:1px solid var(--border2);border-radius:8px;padding:13px 16px;font-family:var(--mono);font-size:.7rem;color:var(--muted);line-height:1.9;margin-bottom:16px}
.dk{color:var(--accent)}.dg{color:var(--green)}.dr{color:var(--red)}

/* Replenish modal */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;display:flex;align-items:center;justify-content:center}
.modal-overlay.hidden{display:none}
.modal{background:var(--surface);border:1px solid var(--border2);border-radius:16px;padding:28px;width:420px;max-width:90vw}
.modal-title{font-size:1rem;font-weight:700;margin-bottom:6px}
.modal-sub{font-family:var(--mono);font-size:.68rem;color:var(--muted);margin-bottom:20px}
.modal-cats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:20px}
.cat-check{display:flex;align-items:center;gap:8px;padding:10px 12px;border-radius:8px;border:1px solid var(--border);cursor:pointer;transition:all .15s}
.cat-check:hover{border-color:var(--border2)}.cat-check.selected{border-color:var(--accent);background:rgba(0,229,255,.06)}
.cat-check input{display:none}
.cat-icon{font-size:1.1rem}
.cat-name{font-size:.8rem;font-weight:600}
.modal-actions{display:flex;gap:10px;justify-content:flex-end}

@media(max-width:960px){.stats{grid-template-columns:repeat(3,1fr)}.two-col{grid-template-columns:1fr}.job-item{grid-template-columns:1fr auto}.analytics-hero{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.actions{flex-wrap:wrap}}
</style>
</head>
<body>
<div class="topbar">
  <div>
    <div class="logo-name">&#127470;&#127475; India<span>20Sixty</span></div>
    <div class="logo-sub">Mission Control v4</div>
  </div>
  <nav class="topbar-nav">
    <button class="nav-btn active" onclick="showPage('home',this)">Home</button>
    <button class="nav-btn"        onclick="showPage('analytics',this)">Analytics</button>
    <button class="nav-btn"        onclick="showPage('topics',this)">Topics</button>
  </nav>
  <div style="display:flex;align-items:center;gap:10px">
    <div class="live-dot"></div><span class="live-lbl">Live</span>
  </div>
</div>

<div class="pages">

<!-- HOME -->
<div class="page active" id="page-home">
  <div class="stats">
    <div class="stat s1"><div class="stat-val" id="s-total">-</div><div class="stat-lbl">Total Jobs</div></div>
    <div class="stat s2"><div class="stat-val" id="s-running">-</div><div class="stat-lbl">Running</div></div>
    <div class="stat s3"><div class="stat-val" id="s-complete">-</div><div class="stat-lbl">Complete</div></div>
    <div class="stat s4"><div class="stat-val" id="s-failed">-</div><div class="stat-lbl">Failed</div></div>
    <div class="stat s5"><div class="stat-val" id="s-topics">-</div><div class="stat-lbl">Topics Ready</div></div>
  </div>

  <!-- Category pills -->
  <div class="cat-strip" id="cat-strip">
    <div class="cat-pill active" data-cat="all" onclick="filterByCat('all',this)"
         style="border-color:var(--accent);color:var(--accent)">All</div>
  </div>

  <div class="actions">
    <button class="btn btn-primary"  id="bc" onclick="createJob()">&#9654; Create Video</button>
    <button class="btn btn-ghost"    id="bg" onclick="generateTopic()">&#10022; Generate Topic</button>
    <button class="btn btn-purple"   id="br" onclick="openReplenishModal()">&#8635; Replenish Queue</button>
    <button class="btn btn-red"      id="bk" onclick="killIncomplete()">&#9940; Kill Incomplete</button>
    <button class="btn btn-ghost"    id="bf" onclick="restoreFailed()">&#8617; Restore Failed</button>
    <button class="btn btn-orange"   id="bt" onclick="testRender()">&#9741; Test Render</button>
  </div>
  <div id="debug-home"></div>
  <div class="two-col">
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Jobs</span><span class="panel-sub" id="last-refresh"></span></div>
        <div class="tabs">
          <button class="tab active" data-tab="all"      onclick="switchTab('all')">All <span class="tab-count tc-all" id="tc-all">0</span></button>
          <button class="tab"        data-tab="running"  onclick="switchTab('running')">Running <span class="tab-count tc-run" id="tc-run">0</span></button>
          <button class="tab"        data-tab="complete" onclick="switchTab('complete')">Complete <span class="tab-count tc-ok" id="tc-ok">0</span></button>
          <button class="tab"        data-tab="failed"   onclick="switchTab('failed')">Failed <span class="tab-count tc-fail" id="tc-fail">0</span></button>
        </div>
        <div id="job-list"></div>
      </div>
    </div>
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Council Queue</span></div>
        <div id="queue-list"></div>
      </div>
    </div>
  </div>
</div>

<!-- ANALYTICS -->
<div class="page" id="page-analytics">
  <div class="actions" style="margin-bottom:20px">
    <button class="btn btn-ghost" onclick="syncAnalytics()">&#8635; Sync from YouTube</button>
  </div>
  <div class="analytics-hero">
    <div class="hero-card"><div class="hero-val hv-views" id="a-views">-</div><div class="hero-lbl">Total Views</div></div>
    <div class="hero-card"><div class="hero-val hv-likes" id="a-likes">-</div><div class="hero-lbl">Total Likes</div></div>
    <div class="hero-card"><div class="hero-val hv-comments" id="a-comments">-</div><div class="hero-lbl">Comments</div></div>
    <div class="hero-card"><div class="hero-val hv-score" id="a-avg">-</div><div class="hero-lbl">Avg Score</div></div>
  </div>
  <div class="two-col">
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">All Videos</span><span class="panel-sub" id="a-count"></span></div>
        <div id="video-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;padding:14px"></div>
      </div>
    </div>
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Top Performers</span></div>
        <div class="perf-row" style="opacity:.4;font-family:var(--mono);font-size:.62rem"><div>TOPIC</div><div style="text-align:right">VIEWS</div><div style="text-align:right">LIKES</div><div style="text-align:right">SCORE</div></div>
        <div id="perf-list"></div>
      </div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Needs Attention</span></div>
        <div class="perf-row" style="opacity:.4;font-family:var(--mono);font-size:.62rem"><div>TOPIC</div><div style="text-align:right">VIEWS</div><div style="text-align:right">LIKES</div><div style="text-align:right">SCORE</div></div>
        <div id="flop-list"></div>
      </div>
    </div>
  </div>
</div>

<!-- TOPICS -->
<div class="page" id="page-topics">
  <div class="actions" style="margin-bottom:16px">
    <button class="btn btn-ghost"   id="bt-filter-all"   onclick="filterTopics('all')">All</button>
    <button class="btn btn-primary" id="bt-filter-ready" onclick="filterTopics('ready')">Ready</button>
    <button class="btn btn-ghost"   id="bt-filter-used"  onclick="filterTopics('used')">Used</button>
    <button class="btn btn-purple"  onclick="openReplenishModal()">&#8635; Replenish</button>
  </div>
  <!-- Category filter pills for topics page -->
  <div class="cat-strip" id="topic-cat-strip" style="margin-bottom:16px">
    <div class="cat-pill active" data-cat="all" onclick="filterTopicsByCat('all',this)"
         style="border-color:var(--accent);color:var(--accent)">All</div>
  </div>
  <div class="panel">
    <div class="panel-head"><span class="panel-title">Topics</span><span class="panel-sub" id="topics-count">-</span></div>
    <div id="topics-full-list"></div>
  </div>
</div>

</div><!-- /pages -->

<!-- Replenish Modal -->
<div class="modal-overlay hidden" id="replenish-modal">
  <div class="modal">
    <div class="modal-title">Replenish Topic Queue</div>
    <div class="modal-sub">Select categories to scout real news from</div>
    <div class="modal-cats" id="modal-cats"></div>
    <div style="margin-bottom:16px">
      <div style="font-family:var(--mono);font-size:.68rem;color:var(--muted);margin-bottom:6px">TARGET COUNT</div>
      <input type="range" id="target-slider" min="5" max="30" value="12" oninput="document.getElementById('target-val').textContent=this.value" style="width:100%">
      <div style="font-family:var(--mono);font-size:.7rem;color:var(--accent);margin-top:4px">
        <span id="target-val">12</span> topics
      </div>
    </div>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeReplenishModal()">Cancel</button>
      <button class="btn btn-purple" onclick="runReplenish()">&#8635; Start Replenish</button>
    </div>
  </div>
</div>

<script>
const CATS = {
  AI:        {label:"AI & ML",         color:"#00e5ff", emoji:"🤖"},
  Space:     {label:"Space & Defence", color:"#b388ff", emoji:"🚀"},
  Gadgets:   {label:"Gadgets & Tech",  color:"#ffd740", emoji:"📱"},
  DeepTech:  {label:"Deep Tech",       color:"#ff6b35", emoji:"🔬"},
  GreenTech: {label:"Green & Energy",  color:"#00e676", emoji:"⚡"},
  Startups:  {label:"Startups",        color:"#ff6b9d", emoji:"💡"},
};
const PROG={pending:8,processing:35,images:55,voice:68,render:82,upload:93,complete:100,test_complete:100,failed:0};
const PCOL={pending:'#ffd740',processing:'#00e5ff',images:'#b388ff',voice:'#b388ff',render:'#ff6b35',upload:'#00e5ff',complete:'#00e676',test_complete:'#00e676',failed:'#ff5252'};
const BLBL={pending:'Pending',processing:'Processing',images:'Images',voice:'Voice',render:'Rendering',upload:'Uploading',complete:'Complete',test_complete:'Complete',failed:'Failed'};
let activeTab='all',allJobs=[],allAnalytics=[],allTopics=[],analyticsJobs=[],
    topicFilter='ready',currentPage='home',activeCat='all',topicCat='all';

// Build category strips
function buildCatStrips(){
  const strip = document.getElementById('cat-strip');
  const tstrip = document.getElementById('topic-cat-strip');
  Object.entries(CATS).forEach(([key,cat])=>{
    const p = document.createElement('div');
    p.className = 'cat-pill'; p.dataset.cat = key;
    p.innerHTML = cat.emoji+' '+cat.label+' <span class="cat-count" id="cc-'+key+'">0</span>';
    p.onclick = () => filterByCat(key,p);
    strip.appendChild(p);
    const p2 = p.cloneNode(true);
    p2.onclick = () => filterTopicsByCat(key,p2);
    tstrip.appendChild(p2);
  });
  // Build modal cats
  const mc = document.getElementById('modal-cats');
  Object.entries(CATS).forEach(([key,cat])=>{
    const d = document.createElement('div');
    d.className = 'cat-check selected'; d.dataset.cat = key;
    d.innerHTML = '<span class="cat-icon">'+cat.emoji+'</span><span class="cat-name">'+cat.label+'</span>';
    d.onclick = () => d.classList.toggle('selected');
    mc.appendChild(d);
  });
}

function showPage(name,btn){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  btn.classList.add('active'); currentPage=name;
  if(name==='analytics') renderAnalytics();
  if(name==='topics')    renderTopicsPage();
}

function ago(iso){const s=Math.floor((Date.now()-new Date(iso))/1000);if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';if(s<86400)return Math.floor(s/3600)+'h';return Math.floor(s/86400)+'d';}
function fmt(n){if(!n)return'0';if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1000)return(n/1000).toFixed(1)+'K';return String(n);}
function scClass(s){return s>=80?'sc-hi':s>=60?'sc-med':'sc-lo';}
function badge(status){const s=status||'unknown';const dot=['pending','processing'].includes(s)?'<span class="bdot"></span>':'';return '<span class="badge b-'+s+'">'+dot+(BLBL[s]||s)+'</span>';}
function showDebug(id,html){document.getElementById(id).innerHTML='<div class="debug-box">'+html+'</div>';}

function filterByCat(cat,btn){
  activeCat=cat;
  document.querySelectorAll('#cat-strip .cat-pill').forEach(p=>{
    p.classList.toggle('active',p.dataset.cat===cat);
    if(p.dataset.cat===cat && cat!=='all'){
      p.style.borderColor=CATS[cat]?.color||'var(--accent)';
      p.style.color=CATS[cat]?.color||'var(--accent)';
    } else if(p.dataset.cat===cat){
      p.style.borderColor='var(--accent)'; p.style.color='var(--accent)';
    } else {
      p.style.borderColor=''; p.style.color='';
    }
  });
  renderJobs();
}

function filterTopicsByCat(cat,btn){
  topicCat=cat;
  document.querySelectorAll('#topic-cat-strip .cat-pill').forEach(p=>{
    p.classList.toggle('active',p.dataset.cat===cat);
    p.style.borderColor = p.dataset.cat===cat ? (cat!=='all'?CATS[cat]?.color:'var(--accent)'):'';
    p.style.color       = p.dataset.cat===cat ? (cat!=='all'?CATS[cat]?.color:'var(--accent)'):'';
  });
  renderTopicsPage();
}

function switchTab(tab){activeTab=tab;document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===tab));renderJobs();}

function filterJobs(jobs,tab){
  let j=jobs;
  if(activeCat!=='all') j=j.filter(x=>x.cluster===activeCat);
  if(tab==='running') return j.filter(x=>['pending','processing','images','voice','render','upload'].includes(x.status));
  if(tab==='complete') return j.filter(x=>x.status==='complete'||x.status==='test_complete');
  if(tab==='failed') return j.filter(x=>x.status==='failed');
  return j;
}

function renderJobs(){
  const el=document.getElementById('job-list');
  const jobs=filterJobs(allJobs,activeTab);
  if(!jobs.length){el.innerHTML='<div class="empty"><span class="empty-icon">📭</span>No jobs here.</div>';return;}
  el.innerHTML=jobs.map(j=>{
    const prog=PROG[j.status]||0,col=PCOL[j.status]||'#5a6278';
    const cat=CATS[j.cluster];
    const catBadge=cat?'<span style="font-size:.6rem;color:'+cat.color+'">'+cat.emoji+' '+j.cluster+'</span>':'';
    const yt=j.youtube_id&&j.youtube_id!=='TEST_MODE'?'<a class="yt-link" href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank">▶ Watch</a>':(j.youtube_id==='TEST_MODE'?'<span style="color:var(--muted);font-size:.6rem;font-family:var(--mono)">test</span>':'');
    const err=j.error?'<span class="job-err" title="'+j.error+'">'+j.error.slice(0,40)+'</span>':'';
    return '<div class="job-item"><div><div class="job-topic">'+(j.topic||'Untitled')+'</div><div class="job-meta">'+catBadge+(j.council_score?'<span>'+j.council_score+'</span>':'')+err+(yt?'<span>'+yt+'</span>':'')+'</div></div><div>'+badge(j.status)+'</div><div class="prog-wrap"><div class="prog-bar"><div class="prog-fill" style="width:'+prog+'%;background:'+col+'"></div></div><div class="prog-pct">'+prog+'%</div></div><div class="time-cell">'+(j.updated_at?ago(j.updated_at)+' ago':'–')+'</div></div>';
  }).join('');
}

async function loadJobs(){
  try{
    const r=await fetch('/jobs');allJobs=await r.json();
    const run=allJobs.filter(j=>['pending','processing','images','voice','render','upload'].includes(j.status));
    const ok=allJobs.filter(j=>j.status==='complete'||j.status==='test_complete');
    const fail=allJobs.filter(j=>j.status==='failed');
    document.getElementById('s-total').textContent=allJobs.length;
    document.getElementById('s-running').textContent=run.length;
    document.getElementById('s-complete').textContent=ok.length;
    document.getElementById('s-failed').textContent=fail.length;
    document.getElementById('tc-all').textContent=allJobs.length;
    document.getElementById('tc-run').textContent=run.length;
    document.getElementById('tc-ok').textContent=ok.length;
    document.getElementById('tc-fail').textContent=fail.length;
    document.getElementById('last-refresh').textContent='Updated '+new Date().toLocaleTimeString();
    renderJobs();
  }catch(e){console.error(e);}
}

async function loadQueue(){
  try{
    const r=await fetch('/topics');allTopics=await r.json();
    const ready=allTopics.filter(t=>!t.used&&t.council_score>=70);
    document.getElementById('s-topics').textContent=ready.length;
    // Update category counts
    Object.keys(CATS).forEach(k=>{
      const el=document.getElementById('cc-'+k);
      if(el) el.textContent=ready.filter(t=>t.cluster===k).length;
    });
    const el=document.getElementById('queue-list');
    if(!ready.length){el.innerHTML='<div class="empty"><span class="empty-icon">📭</span>No topics.<br>Click Replenish Queue.</div>';return;}
    el.innerHTML=ready.slice(0,8).map(t=>{
      const cat=CATS[t.cluster];
      return '<div class="topic-row"><div class="topic-text">'+t.topic+'</div><div class="topic-foot"><span class="score-pill '+scClass(t.council_score)+'">'+t.council_score+'/100</span><span style="display:flex;gap:6px;align-items:center">'+(cat?'<span style="font-size:.7rem;color:'+cat.color+'">'+cat.emoji+' '+t.cluster+'</span>':'')+'<span class="src-tag">'+(t.source||'–')+'</span></span></div></div>';
    }).join('');
  }catch(e){console.error(e);}
}

async function loadAnalytics(){
  try{const r=await fetch('/analytics');const d=await r.json();allAnalytics=d.analytics||[];analyticsJobs=d.jobs||[];if(currentPage==='analytics')renderAnalytics();}catch(e){}
}

function renderAnalytics(){
  const rows=allAnalytics;
  if(!rows.length){
    ['a-views','a-likes','a-comments','a-avg','a-count'].forEach(id=>{const e=document.getElementById(id);if(e)e.textContent='–';});
    document.getElementById('video-grid').innerHTML='<div style="grid-column:1/-1"><div class="empty"><span class="empty-icon">📊</span>No analytics yet.</div></div>';
    document.getElementById('perf-list').innerHTML='<div class="empty" style="padding:20px">No data</div>';
    document.getElementById('flop-list').innerHTML='<div class="empty" style="padding:20px">No data</div>';
    return;
  }
  document.getElementById('a-views').textContent=fmt(rows.reduce((s,r)=>s+(r.youtube_views||0),0));
  document.getElementById('a-likes').textContent=fmt(rows.reduce((s,r)=>s+(r.youtube_likes||0),0));
  document.getElementById('a-comments').textContent=fmt(rows.reduce((s,r)=>s+(r.comment_count||0),0));
  document.getElementById('a-avg').textContent=fmt(rows.length?Math.round(rows.reduce((s,r)=>s+(r.score||0),0)/rows.length):0);
  document.getElementById('a-count').textContent=rows.length+' videos';
  const sorted=[...rows].sort((a,b)=>b.score-a.score);
  document.getElementById('video-grid').innerHTML=sorted.map(r=>{
    const job=analyticsJobs.find(j=>j.id===r.video_id)||{};
    const hasYt=job.youtube_id&&job.youtube_id!=='TEST_MODE';
    return '<div class="video-card"><div class="video-thumb">🎬</div><div class="video-body"><div class="video-topic">'+(job.topic||'Unknown')+'</div><div class="video-stats"><span>👁 <b>'+fmt(r.youtube_views||0)+'</b></span><span>❤ <b>'+fmt(r.youtube_likes||0)+'</b></span></div><div class="video-score">'+fmt(r.score||0)+'</div>'+(hasYt?'<a class="video-link" href="https://youtube.com/watch?v='+job.youtube_id+'" target="_blank">▶ Watch</a>':'')+'</div></div>';
  }).join('');
  const ph=(list)=>list.length?list.map(r=>{const j=analyticsJobs.find(x=>x.id===r.video_id)||{};return '<div class="perf-row"><div class="perf-topic">'+(j.topic||'–')+'</div><div class="perf-num pn-views">'+fmt(r.youtube_views||0)+'</div><div class="perf-num pn-likes">'+fmt(r.youtube_likes||0)+'</div><div class="perf-num pn-score">'+fmt(r.score||0)+'</div></div>';}).join(''):'<div class="empty" style="padding:20px">No data</div>';
  const withViews=rows.filter(r=>r.youtube_views>0);
  document.getElementById('perf-list').innerHTML=ph(sorted.slice(0,5));
  document.getElementById('flop-list').innerHTML=ph([...withViews].sort((a,b)=>a.score-b.score).slice(0,5));
}

function filterTopics(f){topicFilter=f;document.querySelectorAll('[id^="bt-filter"]').forEach(b=>b.classList.remove('btn-primary'));document.getElementById('bt-filter-'+f).classList.add('btn-primary');renderTopicsPage();}

function renderTopicsPage(){
  let topics=allTopics;
  if(topicFilter==='ready')  topics=topics.filter(t=>!t.used&&t.council_score>=70);
  if(topicFilter==='used')   topics=topics.filter(t=>t.used);
  if(topicCat!=='all')       topics=topics.filter(t=>t.cluster===topicCat);
  document.getElementById('topics-count').textContent=topics.length+' topics';
  const el=document.getElementById('topics-full-list');
  if(!topics.length){el.innerHTML='<div class="empty"><span class="empty-icon">📭</span>No topics here.</div>';return;}
  el.innerHTML=topics.map(t=>{
    const cat=CATS[t.cluster];
    return '<div class="topic-row"><div class="topic-text">'+t.topic+'</div><div class="topic-foot"><span class="score-pill '+scClass(t.council_score)+'">'+t.council_score+'/100</span><span style="display:flex;gap:8px;align-items:center"><span class="used-pill '+(t.used?'':'used-no')+'">'+(t.used?'Used':'Ready')+'</span>'+(cat?'<span style="font-size:.7rem;color:'+cat.color+'">'+cat.emoji+' '+t.cluster+'</span>':'')+'<span class="src-tag">'+(t.source||'–')+'</span></span></div></div>';
  }).join('');
}

// Replenish modal
function openReplenishModal(){document.getElementById('replenish-modal').classList.remove('hidden');}
function closeReplenishModal(){document.getElementById('replenish-modal').classList.add('hidden');}
async function runReplenish(){
  const selected=[...document.querySelectorAll('#modal-cats .cat-check.selected')].map(d=>d.dataset.cat);
  const target=parseInt(document.getElementById('target-slider').value);
  closeReplenishModal();
  showDebug('debug-home','<span class="dk">Replenishing ['+selected.join(', ')+'] — target '+target+' topics...</span>');
  try{
    const r=await fetch('/replenish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({categories:selected,target})});
    const d=await r.json();
    showDebug('debug-home','<span class="dg">Replenish triggered.</span> Categories: '+selected.join(', ')+' | Target: '+target+'<br>'+JSON.stringify(d));
    setTimeout(()=>loadQueue(),5000);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
}

async function createJob(){
  const btn=document.getElementById('bc');btn.disabled=true;btn.innerHTML='⏳ Creating...';
  try{const r=await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});const d=await r.json();if(d.error)throw new Error(d.error);switchTab('running');loadJobs();loadQueue();}
  catch(e){alert('Error: '+e.message);}
  finally{btn.disabled=false;btn.innerHTML='▶ Create Video';}
}
async function generateTopic(){
  const btn=document.getElementById('bg');btn.disabled=true;btn.innerHTML='⏳ Generating...';
  try{const topic=prompt('Topic idea:','');if(topic===null)return;const r=await fetch('/generate-topic',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:topic||'Future AI India'})});const d=await r.json();if(d.error)throw new Error(d.error);showDebug('debug-home',d.status==='approved'?'<span class="dg">Approved! Score: '+(d.evaluation?.council_score||'?')+'/100 | Category: '+(d.category||'–')+'</span>':'<span class="dr">Rejected.</span>');loadQueue();}
  catch(e){alert(e.message);}
  finally{btn.disabled=false;btn.innerHTML='✦ Generate Topic';}
}
async function killIncomplete(){const r=allJobs.filter(j=>['pending','processing','images','voice','render','upload'].includes(j.status));if(!r.length){showDebug('debug-home','<span class="dg">No incomplete jobs.</span>');return;}if(!confirm('Kill '+r.length+' job(s)?'))return;const btn=document.getElementById('bk');btn.disabled=true;try{const res=await fetch('/kill-incomplete',{method:'POST'});const d=await res.json();showDebug('debug-home','<span class="dg">Killed '+d.killed+'. Restored: '+d.topics_restored+'</span>');setTimeout(()=>{loadJobs();loadQueue();},600);}catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}finally{btn.disabled=false;}}
async function restoreFailed(){const f=allJobs.filter(j=>j.status==='failed');if(!f.length){showDebug('debug-home','<span class="dg">No failed jobs.</span>');return;}if(!confirm('Restore '+f.length+' jobs?'))return;const btn=document.getElementById('bf');btn.disabled=true;try{const r=await fetch('/restore-failed',{method:'POST'});const d=await r.json();showDebug('debug-home','<span class="dg">Restored '+d.restored+'.</span>');setTimeout(()=>{loadJobs();loadQueue();},600);}catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}finally{btn.disabled=false;}}
async function testRender(){const btn=document.getElementById('bt');btn.disabled=true;btn.innerHTML='⏳ Testing...';try{const r=await fetch('/test-render');const d=await r.json();const col=d.ok?'dg':'dr';showDebug('debug-home','<span class="dk">'+d.url+'</span><br><span class="'+col+'">'+d.status+'</span> — '+(d.response||d.error||'–'));}catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}finally{btn.disabled=false;btn.innerHTML='⚡ Test Render';}}
async function syncAnalytics(){try{await fetch('/sync-analytics',{method:'POST'});showDebug('debug-home','<span class="dg">Sync started.</span>');setTimeout(()=>loadAnalytics(),8000);}catch(e){alert(e.message);}}

buildCatStrips();
function loadAll(){loadJobs();loadQueue();loadAnalytics();}
loadAll();
setInterval(()=>{loadJobs();loadQueue();if(currentPage==='analytics')loadAnalytics();},6000);
</script>
</body>
</html>`;
