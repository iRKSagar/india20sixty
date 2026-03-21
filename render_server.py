export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return json(null, 204);
    }

    if (url.pathname === "/" || url.pathname === "/dashboard") {
      return new Response(DASHBOARD_HTML, {
        headers: { "content-type": "text/html;charset=UTF-8", "cache-control": "no-store" }
      });
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const topicData = await pickTopic(env);
        const job = await createJob(topicData, env);
        ctx.waitUntil(triggerRender(job, env));
        return json({ status: "job_created", job_id: job.id, topic: topicData.topic, council_score: topicData.council_score, source: topicData.source });
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    if (url.pathname === "/jobs") {
      try {
        const jobs = await supabaseGet(env, "jobs?order=created_at.desc&limit=50");
        return json(jobs);
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    if (url.pathname === "/topics") {
      try {
        const topics = await supabaseGet(env, "topics?used=eq.false&order=council_score.desc&limit=50");
        return json(topics);
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    if (url.pathname === "/analytics") {
      try {
        const rows = await supabaseGet(env, "analytics?order=score.desc&limit=20");
        return json(rows);
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    if (url.pathname === "/generate-topic" && request.method === "POST") {
      try {
        const body = await request.json();
        const result = await callTopicCouncil(env, body.topic || "Future of AI in India", "manual");
        return json(result);
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    // Debug: test Render connectivity
    if (url.pathname === "/test-render") {
      const renderBase = (env.RENDER_PIPELINE_URL || "NOT_SET").replace(/\/full-pipeline\/?$/, "");
      const healthUrl  = renderBase + "/health";
      try {
        const r    = await fetch(healthUrl, { signal: AbortSignal.timeout(15000) });
        const text = await r.text();
        return json({ render_pipeline_url: renderBase, health_url_called: healthUrl, status: r.status, response: text.slice(0, 300), ok: r.ok });
      } catch (e) {
        return json({ render_pipeline_url: renderBase, health_url_called: healthUrl, error: e.message, ok: false });
      }
    }

    // Debug: show env binding keys
    if (url.pathname === "/debug-env") {
      return json({
        RENDER_PIPELINE_URL:  env.RENDER_PIPELINE_URL  || "NOT_SET",
        WORKER_URL:           env.WORKER_URL            || "NOT_SET",
        TOPIC_COUNCIL_URL:    env.TOPIC_COUNCIL_URL     || "NOT_SET",
        SUPABASE_URL:         env.SUPABASE_URL          ? env.SUPABASE_URL.slice(0, 40) + "..." : "NOT_SET",
        SUPABASE_ANON_KEY:    env.SUPABASE_ANON_KEY     ? "SET" : "NOT_SET",
        YOUTUBE_CLIENT_ID:    env.YOUTUBE_CLIENT_ID     ? "SET" : "NOT_SET",
        YOUTUBE_REFRESH_TOKEN: env.YOUTUBE_REFRESH_TOKEN ? "SET" : "NOT_SET",
      });
    }

    // Webhook from Render — updates job status after pipeline finishes
    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const data = await request.json();
        const { job_id, status, youtube_id, error, script } = data;
        if (!job_id) return json({ error: "Missing job_id" }, 400);
        const updateData = { status: status || "unknown", updated_at: new Date().toISOString() };
        if (youtube_id) updateData.youtube_id = youtube_id;
        if (error)      updateData.error = error;
        if (script)     updateData.script_package = { text: script };
        await supabasePatch(env, `jobs?id=eq.${job_id}`, updateData);
        if ((status === "complete") && youtube_id && youtube_id !== "TEST_MODE") {
          ctx.waitUntil(createAnalyticsRecord(job_id, youtube_id, env));
        }
        console.log(`Webhook: job ${job_id} -> ${status}`);
        return json({ received: true, job_id, status });
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    // Manual analytics sync
    if (url.pathname === "/sync-analytics" && request.method === "POST") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      return json({ status: "sync_started" });
    }

    // Manual replenish trigger
    if (url.pathname === "/replenish" && request.method === "POST") {
      ctx.waitUntil(triggerReplenish(env));
      return json({ status: "replenish_triggered" });
    }

    return json({ error: "route_not_found" }, 404);
  },

  // ==========================================
  // CRON SCHEDULER
  // ==========================================

  async scheduled(event, env, ctx) {
    const cron = event.cron;
    console.log("Cron fired:", cron);

    // Every minute: queue processor + keep Render warm
    if (cron === "* * * * *") {
      await processQueue(env, ctx);
      const renderBase = (env.RENDER_PIPELINE_URL || "").replace(/\/full-pipeline\/?$/, "");
      if (renderBase) fetch(renderBase + "/health").catch(() => {});
    }

    // 3x daily video creation: 6 AM, 12 PM, 6 PM IST = 0:30, 6:30, 12:30 UTC
    if (cron === "30 0,6,12 * * *") {
      try {
        const topicData = await pickTopic(env);
        const job = await createJob(topicData, env);
        ctx.waitUntil(triggerRender(job, env));
        console.log("Scheduled job created:", job.id, job.topic);
      } catch (e) {
        console.error("Scheduled job creation failed:", e.message);
      }
    }

    // Daily at 2 AM IST (20:30 UTC): analytics sync + queue replenishment check
    if (cron === "30 20 * * *") {
      // 1. Sync YouTube analytics
      ctx.waitUntil(syncYouTubeAnalytics(env));

      // 2. Check queue depth — replenish if below threshold
      try {
        const available = await supabaseGet(env,
          "topics?used=eq.false&council_score=gte.70&select=id"
        );
        console.log(`Queue depth: ${available.length}`);

        if (available.length < 5) {
          console.log("Queue low — triggering topic replenishment");
          ctx.waitUntil(triggerReplenish(env, 12));
        }
      } catch (e) {
        console.error("Queue check failed:", e.message);
      }
    }
  }
};

// ==========================================
// TOPIC SELECTION
// ==========================================

async function pickTopic(env) {
  const topics = await supabaseGet(env,
    "topics?used=eq.false&council_score=gte.70&order=council_score.desc&limit=1"
  );

  if (topics.length > 0) {
    const t = topics[0];
    await supabasePatch(env, `topics?id=eq.${t.id}`, {
      used: true,
      used_at: new Date().toISOString()
    });
    return { topic: t.topic, script_package: t.script_package, council_score: t.council_score, source: "db_approved" };
  }

  console.log("No approved topics — generating via Council...");
  return await generateViaCouncil(env);
}

async function generateViaCouncil(env) {
  const pool = [
    "AI doctors diagnosing cancer in Indian villages",
    "ISRO building space station by 2035",
    "Hyperloop connecting Mumbai to Delhi",
    "Vertical farms feeding Indian cities",
    "Quantum computers solving Bangalore traffic",
    "Floating cities for rising sea levels in Kerala",
    "AI teachers in every village school",
    "Solar power satellites beaming energy to India",
    "3D printed organs ending transplant waiting lists"
  ];
  const topic = pool[Math.floor(Math.random() * pool.length)];
  try {
    const result = await callTopicCouncil(env, topic, "auto_generated");
    if (result.status === "approved") {
      return {
        topic:          result.evaluation?.improved_title || topic,
        script_package: result.script || null,
        council_score:  result.evaluation?.council_score || 75,
        source:         "council_generated"
      };
    }
  } catch (e) {
    console.error("Council call failed:", e.message);
  }
  return { topic: "AI hospitals transforming rural India", script_package: null, council_score: 0, source: "fallback" };
}

async function callTopicCouncil(env, topic, source) {
  if (!env.TOPIC_COUNCIL_URL) throw new Error("TOPIC_COUNCIL_URL not set");
  const r = await fetch(env.TOPIC_COUNCIL_URL + "/full-pipeline", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ topic, source })
  });
  if (!r.ok) throw new Error("Topic Council returned " + r.status);
  return await r.json();
}

// ==========================================
// QUEUE REPLENISHMENT
// ==========================================

async function triggerReplenish(env, target = 12) {
  if (!env.TOPIC_COUNCIL_URL) {
    console.error("TOPIC_COUNCIL_URL not set — cannot replenish");
    return;
  }
  try {
    console.log(`Triggering replenishment — target: ${target} topics`);
    const r = await fetch(env.TOPIC_COUNCIL_URL + "/replenish", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ target })
    });
    const text = await r.text();
    console.log("Replenish response:", r.status, text.slice(0, 200));
  } catch (e) {
    console.error("Replenish failed:", e.message);
  }
}

// ==========================================
// JOB MANAGEMENT
// ==========================================

async function createJob(topicData, env) {
  const job = {
    topic:          topicData.topic,
    cluster:        "AI_Future",
    status:         "pending",
    script_package: topicData.script_package || null,
    council_score:  topicData.council_score || 0,
    retries:        0,
    created_at:     new Date().toISOString(),
    updated_at:     new Date().toISOString()
  };
  return await supabaseInsert(env, "jobs", job);
}

async function processQueue(env, ctx) {
  const fifteenMinAgo = new Date(Date.now() - 15 * 60000).toISOString();
  try {
    // Reset stuck jobs (< 3 retries)
    const stuck = await supabaseGet(env,
      `jobs?status=eq.processing&updated_at=lt.${fifteenMinAgo}&retries=lt.3`
    );
    for (const job of stuck) {
      await supabasePatch(env, `jobs?id=eq.${job.id}`, {
        status: "pending", retries: (job.retries || 0) + 1, updated_at: new Date().toISOString()
      });
    }

    // Mark permanently dead jobs (>= 3 retries) as failed
    const dead = await supabaseGet(env,
      `jobs?status=eq.processing&updated_at=lt.${fifteenMinAgo}&retries=gte.3`
    );
    for (const job of dead) {
      await supabasePatch(env, `jobs?id=eq.${job.id}`, {
        status: "failed", error: "max_retries_exceeded", updated_at: new Date().toISOString()
      });
    }

    // Pick up one pending job
    const pending = await supabaseGet(env, "jobs?status=eq.pending&order=created_at.asc&limit=1");
    if (!pending.length) return;

    const job = pending[0];
    await supabasePatch(env, `jobs?id=eq.${job.id}`, {
      status: "processing", started_at: new Date().toISOString(), updated_at: new Date().toISOString()
    });
    ctx.waitUntil(triggerRender(job, env));
  } catch (e) {
    console.error("Queue error:", e.message);
  }
}

async function triggerRender(job, env) {
  if (!env.RENDER_PIPELINE_URL) {
    console.error("RENDER_PIPELINE_URL not set");
    await supabasePatch(env, `jobs?id=eq.${job.id}`, {
      status: "failed", error: "RENDER_PIPELINE_URL not configured", updated_at: new Date().toISOString()
    });
    return;
  }

  const base      = env.RENDER_PIPELINE_URL.trim().replace(/\/$/, "").replace(/\/full-pipeline$/, "");
  const renderUrl = base + "/full-pipeline";

  console.log("Triggering render:", renderUrl, "| Job:", job.id, job.topic);

  try {
    const r = await fetch(renderUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        job_id:      job.id,
        topic:       job.topic,
        script_package: job.script_package,
        webhook_url: (env.WORKER_URL || "").trim().replace(/\/$/, "") + "/webhook"
      }),
      signal: AbortSignal.timeout(60000)
    });
    const text = await r.text();
    console.log("Render response:", r.status, text.slice(0, 200));
    if (!r.ok) throw new Error(`Render returned ${r.status}: ${text.slice(0, 100)}`);
  } catch (e) {
    console.error("Render trigger failed:", e.message);
    await supabasePatch(env, `jobs?id=eq.${job.id}`, {
      status: "failed", error: e.message, updated_at: new Date().toISOString()
    });
  }
}

// ==========================================
// YOUTUBE ANALYTICS SYNC
// ==========================================

async function getYouTubeToken(env) {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id:     env.YOUTUBE_CLIENT_ID,
      client_secret: env.YOUTUBE_CLIENT_SECRET,
      refresh_token: env.YOUTUBE_REFRESH_TOKEN,
      grant_type:    "refresh_token"
    })
  });
  if (!r.ok) throw new Error("YouTube token refresh failed: " + r.status);
  return (await r.json()).access_token;
}

async function createAnalyticsRecord(job_id, youtube_id, env) {
  try {
    await supabaseInsert(env, "analytics", {
      video_id: job_id, youtube_views: 0, youtube_likes: 0,
      comment_count: 0, score: 0, created_at: new Date().toISOString()
    });
  } catch (e) {
    console.error("Analytics placeholder failed:", e.message);
  }
}

async function syncYouTubeAnalytics(env) {
  if (!env.YOUTUBE_CLIENT_ID || !env.YOUTUBE_CLIENT_SECRET || !env.YOUTUBE_REFRESH_TOKEN) {
    console.log("YouTube credentials not set — skipping analytics sync");
    return;
  }
  console.log("Starting YouTube analytics sync...");
  try {
    const jobs = await supabaseGet(env,
      "jobs?status=eq.complete&youtube_id=not.is.null&order=created_at.desc&limit=50"
    );
    const realJobs = jobs.filter(j => j.youtube_id && j.youtube_id !== "TEST_MODE");
    if (!realJobs.length) { console.log("No YouTube videos to sync"); return; }

    const token   = await getYouTubeToken(env);
    const batches = [];
    for (let i = 0; i < realJobs.length; i += 50) batches.push(realJobs.slice(i, i + 50));

    for (const batch of batches) {
      const ids = batch.map(j => j.youtube_id).join(",");
      const r   = await fetch(
        `https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${ids}&access_token=${token}`
      );
      if (!r.ok) { console.error("YouTube API error:", r.status); continue; }

      const items = (await r.json()).items || [];
      for (const item of items) {
        const stats    = item.statistics || {};
        const views    = parseInt(stats.viewCount    || 0);
        const likes    = parseInt(stats.likeCount    || 0);
        const comments = parseInt(stats.commentCount || 0);
        const score    = views + likes * 50 + comments * 30;
        const job      = batch.find(j => j.youtube_id === item.id);
        if (!job) continue;

        const existing = await supabaseGet(env, `analytics?video_id=eq.${job.id}`);
        if (existing.length > 0) {
          await supabasePatch(env, `analytics?video_id=eq.${job.id}`, {
            youtube_views: views, youtube_likes: likes,
            comment_count: comments, score, updated_at: new Date().toISOString()
          });
        } else {
          await supabaseInsert(env, "analytics", {
            video_id: job.id, youtube_views: views, youtube_likes: likes,
            comment_count: comments, score, created_at: new Date().toISOString()
          });
        }
        console.log(`Synced: ${item.id} | views:${views} likes:${likes} score:${score}`);
      }
    }

    await updateCouncilContext(env);
    console.log("Analytics sync complete");
  } catch (e) {
    console.error("Analytics sync failed:", e.message);
  }
}

async function updateCouncilContext(env) {
  try {
    const topRaw   = await supabaseGet(env, "analytics?order=score.desc&limit=5&select=score,video_id");
    const flopRaw  = await supabaseGet(env, "analytics?order=score.asc&youtube_views=gte.100&limit=5&select=score,video_id");
    const allScores = await supabaseGet(env, "analytics?select=score&youtube_views=gte.50");
    const avgScore  = allScores.length
      ? Math.round(allScores.reduce((s, r) => s + (r.score || 0), 0) / allScores.length)
      : 0;

    const topIds  = topRaw.map(r => r.video_id).filter(Boolean);
    const flopIds = flopRaw.map(r => r.video_id).filter(Boolean);
    let topTopics = [], flopTopics = [];

    if (topIds.length) {
      const topJobs = await supabaseGet(env, `jobs?id=in.(${topIds.join(",")})&select=id,topic,council_score`);
      topTopics = topJobs.map(j => ({
        topic: j.topic, council_score: j.council_score,
        analytics_score: (topRaw.find(r => r.video_id === j.id) || {}).score || 0
      }));
    }
    if (flopIds.length) {
      const flopJobs = await supabaseGet(env, `jobs?id=in.(${flopIds.join(",")})&select=id,topic,council_score`);
      flopTopics = flopJobs.map(j => ({
        topic: j.topic, council_score: j.council_score,
        analytics_score: (flopRaw.find(r => r.video_id === j.id) || {}).score || 0
      }));
    }

    await supabasePatch(env, "system_state?id=eq.main", {
      council_context: JSON.stringify({
        avg_score: avgScore, total_videos: allScores.length,
        top_performers: topTopics, worst_performers: flopTopics,
        updated_at: new Date().toISOString()
      }),
      updated_at: new Date().toISOString()
    });
    console.log("Council context updated. Avg score:", avgScore);
  } catch (e) {
    console.error("Council context update failed:", e.message);
  }
}

// ==========================================
// SUPABASE HELPERS
// ==========================================

function supabaseHeaders(env) {
  return {
    apikey:          env.SUPABASE_ANON_KEY,
    Authorization:   "Bearer " + (env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_ANON_KEY),
    "Content-Type":  "application/json"
  };
}

async function supabaseGet(env, endpoint) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + endpoint, { headers: supabaseHeaders(env) });
  if (!r.ok) throw new Error("Supabase GET error: " + r.status + " " + endpoint);
  return r.json();
}

async function supabaseInsert(env, table, data) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + table, {
    method:  "POST",
    headers: { ...supabaseHeaders(env), Prefer: "return=representation" },
    body:    JSON.stringify(data)
  });
  if (!r.ok) { const b = await r.text(); throw new Error("Supabase INSERT error: " + r.status + " " + b.slice(0, 200)); }
  return (await r.json())[0];
}

async function supabasePatch(env, endpoint, data) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + endpoint, {
    method:  "PATCH",
    headers: { ...supabaseHeaders(env), Prefer: "return=minimal" },
    body:    JSON.stringify(data)
  });
  return r.ok;
}

// ==========================================
// UTILS
// ==========================================

function json(data, status) {
  return new Response(JSON.stringify(data, null, 2), {
    status: status || 200,
    headers: {
      "content-type":                "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS,PATCH"
    }
  });
}

// ==========================================
// DASHBOARD
// ==========================================

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>India20Sixty - Mission Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{--bg:#080c14;--surface:#0d1320;--surface2:#111827;--border:rgba(255,255,255,0.07);--border2:rgba(255,255,255,0.12);--text:#e8eaf0;--muted:#5a6278;--accent:#00e5ff;--accent2:#ff6b35;--green:#00e676;--yellow:#ffd740;--red:#ff5252;--purple:#b388ff;--font:'Syne',sans-serif;--mono:'DM Mono',monospace}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:var(--font);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;z-index:0;background-image:linear-gradient(rgba(0,229,255,0.03) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,255,0.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none}
.wrap{position:relative;z-index:1;max-width:1200px;margin:0 auto;padding:24px 20px 60px}
.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;padding-bottom:18px;border-bottom:1px solid var(--border)}
.logo-name{font-size:1.4rem;font-weight:800;letter-spacing:-0.02em}.logo-name span{color:var(--accent)}
.logo-sub{font-family:var(--mono);font-size:.62rem;color:var(--muted);letter-spacing:.12em;text-transform:uppercase;margin-top:2px}
.header-right{display:flex;align-items:center;gap:10px}
.live-dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 7px var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.8)}}
.live-label{font-family:var(--mono);font-size:.68rem;color:var(--green);letter-spacing:.1em}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:22px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px;position:relative;overflow:hidden;transition:border-color .2s}
.stat::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:10px 10px 0 0}
.stat-all::before{background:var(--accent)}.stat-run::before{background:var(--yellow)}.stat-ok::before{background:var(--green)}.stat-fail::before{background:var(--red)}.stat-topic::before{background:var(--purple)}
.stat:hover{border-color:var(--border2)}
.stat-value{font-size:1.8rem;font-weight:800;letter-spacing:-0.03em;line-height:1;margin-bottom:3px}
.stat-all .stat-value{color:var(--accent)}.stat-run .stat-value{color:var(--yellow)}.stat-ok .stat-value{color:var(--green)}.stat-fail .stat-value{color:var(--red)}.stat-topic .stat-value{color:var(--purple)}
.stat-label{font-family:var(--mono);font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}
.actions{display:flex;gap:10px;margin-bottom:22px;flex-wrap:wrap}
.btn{display:flex;align-items:center;gap:7px;padding:10px 20px;border-radius:8px;border:none;font-family:var(--font);font-size:.85rem;font-weight:600;cursor:pointer;transition:all .2s;white-space:nowrap}
.btn-primary{background:var(--accent);color:#000}.btn-primary:hover{background:#33eaff;transform:translateY(-1px);box-shadow:0 8px 24px rgba(0,229,255,.25)}
.btn-secondary{background:var(--surface2);color:var(--text);border:1px solid var(--border2)}.btn-secondary:hover{border-color:var(--accent);color:var(--accent);transform:translateY(-1px)}
.btn-warn{background:rgba(255,107,53,.12);color:var(--accent2);border:1px solid rgba(255,107,53,.25)}.btn-warn:hover{background:rgba(255,107,53,.2);transform:translateY(-1px)}
.btn-purple{background:rgba(179,136,255,.12);color:var(--purple);border:1px solid rgba(179,136,255,.25)}.btn-purple:hover{background:rgba(179,136,255,.2);transform:translateY(-1px)}
.btn:disabled{opacity:.4;cursor:not-allowed;transform:none!important}
.main-grid{display:grid;grid-template-columns:1fr 300px;gap:18px;align-items:start}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:18px}
.panel-head{padding:14px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.panel-title{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.refresh-time{font-family:var(--mono);font-size:.63rem;color:var(--muted)}
.tabs{display:flex;gap:3px;padding:10px 14px;border-bottom:1px solid var(--border)}
.tab{display:flex;align-items:center;gap:6px;padding:6px 12px;border-radius:6px;border:1px solid transparent;background:transparent;font-family:var(--font);font-size:.78rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s}
.tab:hover{color:var(--text);background:var(--surface2)}.tab.active{color:var(--text);background:var(--surface2);border-color:var(--border2)}
.tab-count{font-family:var(--mono);font-size:.63rem;padding:1px 6px;border-radius:20px;font-weight:500}
.tab[data-tab=all] .tab-count{background:rgba(0,229,255,.12);color:var(--accent)}
.tab[data-tab=running] .tab-count{background:rgba(255,215,64,.12);color:var(--yellow)}
.tab[data-tab=complete] .tab-count{background:rgba(0,230,118,.12);color:var(--green)}
.tab[data-tab=failed] .tab-count{background:rgba(255,82,82,.12);color:var(--red)}
.job-item{display:grid;grid-template-columns:1fr 110px 150px 68px;gap:12px;padding:13px 18px;border-bottom:1px solid var(--border);align-items:center;transition:background .15s}
.job-item:hover{background:rgba(255,255,255,.02)}.job-item:last-child{border-bottom:none}
.job-topic{font-size:.85rem;font-weight:600;color:var(--text);line-height:1.35;margin-bottom:4px}
.job-meta{font-family:var(--mono);font-size:.65rem;color:var(--muted);display:flex;gap:7px;flex-wrap:wrap;align-items:center}
.job-err{color:#ff6b6b;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:5px;font-family:var(--mono);font-size:.67rem;font-weight:500;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.badge-dot{width:5px;height:5px;border-radius:50%;background:currentColor}
.b-pending{background:rgba(255,215,64,.1);color:var(--yellow);border:1px solid rgba(255,215,64,.2)}
.b-processing,.b-upload{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.2)}
.b-images,.b-voice{background:rgba(179,136,255,.1);color:var(--purple);border:1px solid rgba(179,136,255,.2)}
.b-render{background:rgba(255,107,53,.1);color:var(--accent2);border:1px solid rgba(255,107,53,.2)}
.b-complete,.b-test_complete{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.b-failed{background:rgba(255,82,82,.1);color:var(--red);border:1px solid rgba(255,82,82,.2)}
.b-pending .badge-dot,.b-processing .badge-dot{animation:pulse 1.4s infinite}
.prog-wrap{display:flex;flex-direction:column;gap:4px}
.prog-bar{height:3px;background:rgba(255,255,255,.06);border-radius:2px;overflow:hidden}
.prog-fill{height:100%;border-radius:2px;transition:width .6s ease}
.prog-pct{font-family:var(--mono);font-size:.62rem;color:var(--muted)}
.yt-link{color:var(--accent);text-decoration:none;font-family:var(--mono);font-size:.67rem}
.yt-link:hover{color:#fff}
.retry-pip{font-family:var(--mono);font-size:.62rem;color:var(--accent2);background:rgba(255,107,53,.1);border:1px solid rgba(255,107,53,.2);padding:1px 5px;border-radius:3px}
.time-cell{font-family:var(--mono);font-size:.68rem;color:var(--muted);text-align:right}
.empty{padding:40px 18px;text-align:center;color:var(--muted);font-family:var(--mono);font-size:.76rem;line-height:1.8}
.empty-icon{font-size:1.8rem;margin-bottom:10px;opacity:.35;display:block}
.topic-item{padding:13px 16px;border-bottom:1px solid var(--border);transition:background .15s}
.topic-item:hover{background:rgba(255,255,255,.02)}.topic-item:last-child{border-bottom:none}
.topic-text{font-size:.8rem;font-weight:600;color:var(--text);line-height:1.4;margin-bottom:5px}
.topic-footer{display:flex;align-items:center;justify-content:space-between}
.score-pill{font-family:var(--mono);font-size:.65rem;font-weight:500;padding:2px 7px;border-radius:4px}
.sc-hi{background:rgba(0,230,118,.12);color:var(--green)}.sc-med{background:rgba(255,215,64,.12);color:var(--yellow)}.sc-lo{background:rgba(255,82,82,.12);color:var(--red)}
.source-tag{font-family:var(--mono);font-size:.62rem;color:var(--muted)}
.analytics-row{display:grid;grid-template-columns:1fr 70px 70px 80px;gap:10px;padding:11px 16px;border-bottom:1px solid var(--border);align-items:center}
.analytics-row:last-child{border-bottom:none}
.analytics-topic{font-weight:600;font-size:.78rem;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.analytics-num{font-family:var(--mono);font-size:.72rem;color:var(--accent);text-align:right}
.analytics-score{font-family:var(--mono);font-size:.72rem;color:var(--yellow);text-align:right;font-weight:600}
.debug-box{background:rgba(0,0,0,.4);border:1px solid var(--border2);border-radius:8px;padding:14px 16px;font-family:var(--mono);font-size:.72rem;color:var(--muted);line-height:1.8;margin:0 0 18px}
.debug-box .ok{color:var(--green)}.debug-box .err{color:var(--red)}.debug-box .key{color:var(--accent)}
::-webkit-scrollbar{width:3px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
@media(max-width:900px){.stats{grid-template-columns:repeat(3,1fr)}.main-grid{grid-template-columns:1fr}.job-item{grid-template-columns:1fr auto}.job-item>.prog-wrap,.job-item>.time-cell{display:none}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.actions{flex-direction:column}}
</style>
</head>
<body>
<div class="wrap">
  <header class="header">
    <div>
      <div class="logo-name">&#127470;&#127475; India<span>20Sixty</span></div>
      <div class="logo-sub">Mission Control</div>
    </div>
    <div class="header-right">
      <div class="live-dot"></div>
      <span class="live-label">Live</span>
    </div>
  </header>

  <div class="stats">
    <div class="stat stat-all"><div class="stat-value" id="s-total">-</div><div class="stat-label">Total Jobs</div></div>
    <div class="stat stat-run"><div class="stat-value" id="s-running">-</div><div class="stat-label">Running</div></div>
    <div class="stat stat-ok"><div class="stat-value" id="s-complete">-</div><div class="stat-label">Complete</div></div>
    <div class="stat stat-fail"><div class="stat-value" id="s-failed">-</div><div class="stat-label">Failed</div></div>
    <div class="stat stat-topic"><div class="stat-value" id="s-topics">-</div><div class="stat-label">Topics Ready</div></div>
  </div>

  <div class="actions">
    <button class="btn btn-primary"   id="btn-create"   onclick="createJob()">&#9654; Create Video</button>
    <button class="btn btn-secondary" id="btn-topic"    onclick="generateTopic()">&#10022; Generate Topic</button>
    <button class="btn btn-purple"    id="btn-replenish" onclick="replenishQueue()">&#8635; Replenish Queue</button>
    <button class="btn btn-warn"      id="btn-test"     onclick="testRender()">&#9741; Test Render</button>
    <button class="btn btn-secondary"                   onclick="syncAnalytics()">&#8635; Sync Analytics</button>
  </div>

  <div id="debug-area"></div>

  <div class="main-grid">
    <div>
      <div class="panel">
        <div class="panel-head">
          <span class="panel-title">Jobs</span>
          <span class="refresh-time" id="last-refresh"></span>
        </div>
        <div class="tabs">
          <button class="tab active" data-tab="all"      onclick="switchTab('all')">All <span class="tab-count" id="tc-all">0</span></button>
          <button class="tab"        data-tab="running"  onclick="switchTab('running')">Running <span class="tab-count" id="tc-running">0</span></button>
          <button class="tab"        data-tab="complete" onclick="switchTab('complete')">Complete <span class="tab-count" id="tc-complete">0</span></button>
          <button class="tab"        data-tab="failed"   onclick="switchTab('failed')">Failed <span class="tab-count" id="tc-failed">0</span></button>
        </div>
        <div id="job-list"></div>
      </div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Analytics — Top Performers</span></div>
        <div id="analytics-list"></div>
      </div>
    </div>
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Council Queue</span></div>
        <div id="topics-list"></div>
      </div>
    </div>
  </div>
</div>

<script>
const PROG={pending:8,processing:35,images:55,voice:68,render:82,upload:93,complete:100,test_complete:100,failed:0};
const PCOL={pending:'#ffd740',processing:'#00e5ff',images:'#b388ff',voice:'#b388ff',render:'#ff6b35',upload:'#00e5ff',complete:'#00e676',test_complete:'#00e676',failed:'#ff5252'};
const BLBL={pending:'Pending',processing:'Processing',images:'Images',voice:'Voice',render:'Rendering',upload:'Uploading',complete:'Complete',test_complete:'Complete',failed:'Failed'};
let activeTab='all', allJobs=[];

function ago(iso){const s=Math.floor((Date.now()-new Date(iso))/1000);if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';if(s<86400)return Math.floor(s/3600)+'h';return Math.floor(s/86400)+'d';}
function fmt(n){if(n>=1000000)return (n/1000000).toFixed(1)+'M';if(n>=1000)return (n/1000).toFixed(1)+'K';return String(n);}
function scClass(s){return s>=80?'sc-hi':s>=60?'sc-med':'sc-lo';}
function badge(status){const s=status||'unknown';const dot=['pending','processing'].includes(s)?'<span class="badge-dot"></span>':'';return '<span class="badge b-'+s+'">'+dot+(BLBL[s]||s)+'</span>';}

function switchTab(tab){
  activeTab=tab;
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===tab));
  renderJobs();
}

function filterJobs(jobs,tab){
  if(tab==='running') return jobs.filter(j=>['pending','processing','images','voice','render','upload'].includes(j.status));
  if(tab==='complete') return jobs.filter(j=>j.status==='complete'||j.status==='test_complete');
  if(tab==='failed') return jobs.filter(j=>j.status==='failed');
  return jobs;
}

function renderJobs(){
  const el=document.getElementById('job-list');
  const jobs=filterJobs(allJobs,activeTab);
  if(!jobs.length){
    const ico={all:'&#128354;',running:'&#9203;',complete:'&#9989;',failed:'&#127881;'}[activeTab]||'';
    const msg={all:'No jobs yet.',running:'No active jobs.',complete:'No completed jobs yet.',failed:'No failures!'}[activeTab]||'';
    el.innerHTML='<div class="empty"><span class="empty-icon">'+ico+'</span>'+msg+'</div>';return;
  }
  el.innerHTML=jobs.map(j=>{
    const prog=PROG[j.status]||0, col=PCOL[j.status]||'#5a6278';
    const yt=j.youtube_id&&j.youtube_id!=='TEST_MODE'
      ?'<a class="yt-link" href="https://youtube.com/watch?v='+j.youtube_id+'" target="_blank">&#9654; Watch</a>'
      :(j.youtube_id==='TEST_MODE'?'<span style="color:var(--muted);font-size:.65rem;font-family:var(--mono)">test</span>':'');
    const err=j.error?'<span class="job-err" title="'+j.error+'">'+j.error.slice(0,45)+(j.error.length>45?'...':'')+'</span>':'';
    const retry=j.retries>0?'<span class="retry-pip">x'+j.retries+'</span>':'';
    return '<div class="job-item"><div><div class="job-topic">'+(j.topic||'Untitled')+'</div>'+
      '<div class="job-meta"><span>'+(j.council_score?'Score '+j.council_score:'Fallback')+'</span>'+err+retry+(yt?'<span>'+yt+'</span>':'')+'</div></div>'+
      '<div>'+badge(j.status)+'</div>'+
      '<div class="prog-wrap"><div class="prog-bar"><div class="prog-fill" style="width:'+prog+'%;background:'+col+'"></div></div>'+
      '<div class="prog-pct">'+prog+'%</div></div>'+
      '<div class="time-cell">'+(j.updated_at?ago(j.updated_at)+' ago':'–')+'</div></div>';
  }).join('');
}

async function loadJobs(){
  try{
    const r=await fetch('/jobs'); allJobs=await r.json();
    const run=allJobs.filter(j=>['pending','processing','images','voice','render','upload'].includes(j.status));
    const ok=allJobs.filter(j=>j.status==='complete'||j.status==='test_complete');
    const fail=allJobs.filter(j=>j.status==='failed');
    document.getElementById('s-total').textContent=allJobs.length;
    document.getElementById('s-running').textContent=run.length;
    document.getElementById('s-complete').textContent=ok.length;
    document.getElementById('s-failed').textContent=fail.length;
    document.getElementById('tc-all').textContent=allJobs.length;
    document.getElementById('tc-running').textContent=run.length;
    document.getElementById('tc-complete').textContent=ok.length;
    document.getElementById('tc-failed').textContent=fail.length;
    document.getElementById('last-refresh').textContent='Updated '+new Date().toLocaleTimeString();
    renderJobs();
  }catch(e){console.error('loadJobs:',e);}
}

async function loadTopics(){
  try{
    const r=await fetch('/topics'); const topics=await r.json();
    const ready=topics.filter(t=>!t.used&&t.council_score>=70);
    document.getElementById('s-topics').textContent=ready.length;
    const el=document.getElementById('topics-list');
    if(!ready.length){el.innerHTML='<div class="empty"><span class="empty-icon">&#128354;</span>No approved topics.<br>Click Replenish Queue.</div>';return;}
    el.innerHTML=ready.slice(0,8).map(t=>
      '<div class="topic-item"><div class="topic-text">'+t.topic+'</div>'+
      '<div class="topic-footer"><span class="score-pill '+scClass(t.council_score)+'">'+t.council_score+'/100</span>'+
      '<span class="source-tag">'+(t.source||'unknown')+'</span></div></div>'
    ).join('');
  }catch(e){console.error('loadTopics:',e);}
}

async function loadAnalytics(){
  try{
    const r=await fetch('/analytics'); const rows=await r.json();
    const el=document.getElementById('analytics-list');
    if(!rows.length||rows.every(r=>!r.youtube_views)){
      el.innerHTML='<div class="empty"><span class="empty-icon">&#128202;</span>No analytics yet.<br>Syncs daily after videos are live.</div>';return;
    }
    const sorted=rows.filter(r=>r.youtube_views>0).sort((a,b)=>b.score-a.score).slice(0,8);
    el.innerHTML='<div class="analytics-row" style="opacity:.4;font-size:.65rem;font-family:var(--mono)"><div>TOPIC</div><div style="text-align:right">VIEWS</div><div style="text-align:right">LIKES</div><div style="text-align:right">SCORE</div></div>'+
    sorted.map(r=>{
      const job=allJobs.find(j=>j.id===r.video_id)||{};
      return '<div class="analytics-row"><div class="analytics-topic">'+(job.topic||r.video_id||'–')+'</div>'+
        '<div class="analytics-num">'+fmt(r.youtube_views||0)+'</div>'+
        '<div class="analytics-num">'+fmt(r.youtube_likes||0)+'</div>'+
        '<div class="analytics-score">'+fmt(r.score||0)+'</div></div>';
    }).join('');
  }catch(e){console.error('loadAnalytics:',e);}
}

function showDebug(html){
  const el=document.getElementById('debug-area');
  el.innerHTML='<div class="debug-box">'+html+'</div>';
}

async function testRender(){
  const btn=document.getElementById('btn-test');
  btn.disabled=true;btn.innerHTML='&#9741; Testing...';
  try{
    const r=await fetch('/test-render'); const d=await r.json();
    const col=d.ok?'var(--green)':'var(--red)';
    showDebug(
      '<span class="key">URL called:</span> '+d.health_url_called+'<br>'+
      '<span class="key">HTTP status:</span> <span style="color:'+col+'">'+d.status+'</span><br>'+
      '<span class="key">Response:</span> '+(d.response||d.error||'–')+'<br>'+
      '<span class="key">RENDER_PIPELINE_URL:</span> '+d.render_pipeline_url
    );
  }catch(e){showDebug('<span class="err">Test failed: '+e.message+'</span>');}
  finally{btn.disabled=false;btn.innerHTML='&#9741; Test Render';}
}

async function replenishQueue(){
  const btn=document.getElementById('btn-replenish');
  btn.disabled=true;btn.innerHTML='&#8635; Replenishing...';
  try{
    const r=await fetch('/replenish',{method:'POST'});
    const d=await r.json();
    showDebug('<span class="key">Replenish triggered.</span> Check topic-council-worker logs for progress.<br>Status: '+JSON.stringify(d));
    setTimeout(()=>{loadTopics();},3000);
  }catch(e){showDebug('<span class="err">Replenish failed: '+e.message+'</span>');}
  finally{btn.disabled=false;btn.innerHTML='&#8635; Replenish Queue';}
}

async function syncAnalytics(){
  try{await fetch('/sync-analytics',{method:'POST'});showDebug('<span class="ok">Analytics sync started. Refresh in 30s.</span>');}
  catch(e){showDebug('<span class="err">Sync failed: '+e.message+'</span>');}
}

async function createJob(){
  const btn=document.getElementById('btn-create');
  btn.disabled=true;btn.innerHTML='&#9711; Creating...';
  try{
    const r=await fetch('/run',{method:'POST'}); const d=await r.json();
    if(d.error)throw new Error(d.error);
    switchTab('running');loadJobs();loadTopics();
  }catch(e){alert('Error: '+e.message);}
  finally{btn.disabled=false;btn.innerHTML='&#9654; Create Video';}
}

async function generateTopic(){
  const btn=document.getElementById('btn-topic');
  btn.disabled=true;btn.innerHTML='&#9711; Generating...';
  try{
    const topic=prompt('Topic idea (blank = auto-generate):','');
    if(topic===null)return;
    const r=await fetch('/generate-topic',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:topic||'Future of AI in India'})});
    const d=await r.json();
    if(d.error)throw new Error(d.error);
    showDebug(d.status==='approved'
      ?'<span class="ok">Council approved! Score: '+(d.evaluation?.council_score||'?')+'/100</span>'
      :'<span class="err">Rejected by Council. Try a different angle.</span>');
    loadTopics();
  }catch(e){alert('Error: '+e.message);}
  finally{btn.disabled=false;btn.innerHTML='&#10022; Generate Topic';}
}

loadJobs();loadTopics();loadAnalytics();
setInterval(()=>{loadJobs();loadTopics();loadAnalytics();},6000);
</script>
</body>
</html>`;
