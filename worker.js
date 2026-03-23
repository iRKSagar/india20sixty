// ============================================================
// India20Sixty — Cloudflare Worker V2
// ============================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return cors(null, 204);

    // ── HEALTH ────────────────────────────────────────────────
    if (url.pathname === "/health") {
      return cors({ status: "ok", version: "v2.0",
        time: new Date().toISOString() });
    }

    // ── DASHBOARD ─────────────────────────────────────────────
    if (url.pathname === "/" || url.pathname === "/dashboard") {
      return new Response(buildDashboard(env), {
        headers: { "content-type": "text/html;charset=UTF-8",
                   "cache-control": "no-store" }
      });
    }

    // ── JOBS ──────────────────────────────────────────────────
    if (url.pathname === "/jobs") {
      try { return cors(await sbGet(env, "jobs?order=created_at.desc&limit=50")); }
      catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── TOPICS ────────────────────────────────────────────────
    if (url.pathname === "/topics") {
      try { return cors(await sbGet(env, "topics?order=council_score.desc&limit=100")); }
      catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── ANALYTICS ─────────────────────────────────────────────
    if (url.pathname === "/analytics") {
      try {
        const analytics = await sbGet(env, "analytics?order=score.desc&limit=50");
        const jobs      = await sbGet(env, "jobs?status=eq.complete&select=id,topic,council_score,youtube_id,cluster,created_at");
        return cors({ analytics, jobs });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── RUN VIDEO ─────────────────────────────────────────────
    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const body  = await request.json().catch(() => ({}));
        const t     = await pickTopic(env, body.category || null);
        const job   = await createJob(t, env);
        ctx.waitUntil(triggerRender(job, env));
        return cors({ status: "job_created", job_id: job.id,
                      topic: t.topic, category: t.category });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── RUN SPECIFIC TOPIC ────────────────────────────────────
    // Called from Topics page "Generate Now" button
    if (url.pathname === "/run-topic" && request.method === "POST") {
      try {
        const body     = await request.json().catch(() => ({}));
        const topic_id = body.topic_id;
        if (!topic_id) return cors({ error: "Missing topic_id" }, 400);
        // Fetch the specific topic
        const topics = await sbGet(env, `topics?id=eq.${topic_id}&select=*`);
        if (!topics.length) return cors({ error: "Topic not found" }, 404);
        const t = topics[0];
        if (t.used) return cors({ error: "Topic already used" }, 400);
        // Mark used
        await sbPatch(env, `topics?id=eq.${topic_id}`,
          { used: true, used_at: new Date().toISOString() });
        // Create job from this specific topic
        const job = await sbInsert(env, "jobs", {
          topic: t.topic, cluster: t.cluster || "AI", status: "pending",
          script_package: t.script_package || null,
          council_score: t.council_score || 0,
          retries: 0,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        });
        ctx.waitUntil(triggerRender(job, env));
        return cors({ status: "job_created", job_id: job.id,
                      topic: t.topic, category: t.cluster });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── REVIEW QUEUE (CBDP) ───────────────────────────────────
    if (url.pathname === "/review") {
      try {
        const rows = await sbGet(env,
          "jobs?status=eq.review&order=updated_at.desc" +
          "&select=id,topic,cluster,script_package,video_r2_url,council_score,updated_at");
        const r2Base = (env.R2_BASE_URL || "").replace(/\/$/, "");
        // Attach full public URL so dashboard can play video directly
        const enriched = rows.map(j => ({
          ...j,
          video_public_url: j.video_r2_url && r2Base
            ? r2Base + "/" + j.video_r2_url
            : null
        }));
        return cors(enriched);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── PUBLISH A CBDP JOB ────────────────────────────────────
    if (url.pathname === "/publish-job" && request.method === "POST") {
      try {
        const { job_id } = await request.json();
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        // Fetch job to get video URL and script
        const jobs = await sbGet(env,
          `jobs?id=eq.${job_id}&select=id,topic,video_r2_url,script_package,cluster`);
        if (!jobs.length) return cors({ error: "Job not found" }, 404);
        const job = jobs[0];
        if (!job.video_r2_url)
          return cors({ error: "No video file for this job" }, 400);

        // Use mixer.py to upload — it handles YouTube upload
        const mixerUrl = env.MIXER_URL || "";
        if (!mixerUrl) return cors({ error: "MIXER_URL not configured" }, 500);

        const r2Base   = (env.R2_BASE_URL || "").replace(/\/$/, "");
        const videoUrl = `${r2Base}/${job.video_r2_url}`;
        const title    = job.script_package?.title || job.topic;

        // Update status to uploading
        await sbPatch(env, `jobs?id=eq.${job_id}`,
          { status: "upload", updated_at: new Date().toISOString() });

        // Trigger mixer with no voice (it's already mixed with voice from AI)
        // We use a special flag to tell mixer to upload-only
        const r = await fetch(mixerUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            job_id,
            video_url:    videoUrl,
            voice_url:    null,        // no voice — video already has audio
            music_track:  null,        // no music — video already has audio
            publish_at:   null,
            upload_only:  true,        // signal to mixer: skip mix, just upload
            title:        title,
          })
        });
        if (!r.ok) throw new Error(`Mixer returned ${r.status}`);
        return cors({ status: "publishing", job_id });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── REJECT A CBDP JOB (send back to queue) ───────────────
    if (url.pathname === "/reject-job" && request.method === "POST") {
      try {
        const { job_id } = await request.json();
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        // Get job topic to restore it
        const jobs = await sbGet(env,
          `jobs?id=eq.${job_id}&select=id,topic,cluster,council_score,script_package`);
        if (!jobs.length) return cors({ error: "Job not found" }, 404);
        const job = jobs[0];
        // Mark job as failed/rejected
        await sbPatch(env, `jobs?id=eq.${job_id}`,
          { status: "failed", error: "Rejected in review",
            updated_at: new Date().toISOString() });
        // Restore topic to queue if it exists
        if (job.topic) {
          try {
            const ex = await sbGet(env,
              `topics?topic=eq.${encodeURIComponent(job.topic)}&select=id,used`);
            if (ex.length > 0) {
              await sbPatch(env, `topics?id=eq.${ex[0].id}`,
                { used: false, used_at: null });
            } else {
              // Re-insert topic so it can be used again
              await sbInsert(env, "topics", {
                topic: job.topic, cluster: job.cluster || "AI",
                council_score: job.council_score || 75,
                script_package: job.script_package || null,
                used: false, source: "restored_from_review",
                created_at: new Date().toISOString()
              });
            }
          } catch(e) { console.error("Topic restore:", e.message); }
        }
        return cors({ status: "rejected", job_id, topic_restored: true });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── GENERATE TOPIC ────────────────────────────────────────
    if (url.pathname === "/generate-topic" && request.method === "POST") {
      try {
        const body   = await request.json().catch(() => ({}));
        const result = await callCouncil(env,
          body.topic || "Future of AI in India", "manual", body.category);
        return cors(result);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── REPLENISH ─────────────────────────────────────────────
    if (url.pathname === "/replenish" && request.method === "POST") {
      const body       = await request.json().catch(() => ({}));
      const categories = body.categories || null;
      const target     = body.target || 12;
      ctx.waitUntil(triggerReplenish(env, target, categories));
      return cors({ status: "replenish_triggered", categories, target });
    }

    // ── PUBLISH STATE ─────────────────────────────────────────
    if (url.pathname === "/publish-state") {
      if (request.method === "GET") {
        try {
          const rows = await sbGet(env, "system_state?id=eq.main&select=publish");
          return cors({ publish: rows[0]?.publish === true });
        } catch (e) { return cors({ publish: false }); }
      }
      if (request.method === "POST") {
        try {
          const { publish } = await request.json();
          await upsertState(env, { publish: !!publish });
          return cors({ publish: !!publish });
        } catch (e) { return cors({ error: e.message }, 500); }
      }
    }

    // ── VOICE MODE ────────────────────────────────────────────
    if (url.pathname === "/voice-mode") {
      if (request.method === "GET") {
        try {
          const rows = await sbGet(env, "system_state?id=eq.main&select=voice_mode");
          return cors({ voice_mode: rows[0]?.voice_mode || "ai" });
        } catch (e) { return cors({ voice_mode: "ai" }); }
      }
      if (request.method === "POST") {
        try {
          const { voice_mode } = await request.json();
          const mode = ["ai", "human"].includes(voice_mode) ? voice_mode : "ai";
          await upsertState(env, { voice_mode: mode });
          return cors({ voice_mode: mode });
        } catch (e) { return cors({ error: e.message }, 500); }
      }
    }

    // ── SCHEDULE ──────────────────────────────────────────────
    if (url.pathname === "/set-schedule" && request.method === "POST") {
      try {
        const { videos_per_day } = await request.json();
        const vpd = Math.min(3, Math.max(1, parseInt(videos_per_day) || 1));
        await upsertState(env, { videos_per_day: vpd });
        return cors({ videos_per_day: vpd });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    if (url.pathname === "/get-schedule") {
      try {
        const rows = await sbGet(env, "system_state?id=eq.main&select=videos_per_day");
        const vpd  = rows[0]?.videos_per_day || 1;
        return cors({ videos_per_day: vpd });
      } catch (e) { return cors({ videos_per_day: 1 }); }
    }

    // ── KILL INCOMPLETE ───────────────────────────────────────
    if (url.pathname === "/kill-incomplete" && request.method === "POST") {
      try {
        const stuck = await sbGet(env,
          "jobs?status=in.(pending,processing,images,voice,render,upload)&select=id,topic,cluster");
        let topicsRestored = 0;
        for (const job of stuck) {
          await sbPatch(env, "jobs?id=eq." + job.id, {
            status: "failed", error: "manually_killed",
            updated_at: new Date().toISOString()
          });
          if (job.topic) {
            try {
              const ex = await sbGet(env, "topics?topic=eq." +
                encodeURIComponent(job.topic) + "&select=id,used");
              if (ex.length > 0 && ex[0].used) {
                await sbPatch(env, "topics?id=eq." + ex[0].id,
                  { used: false, used_at: null });
                topicsRestored++;
              }
            } catch (e) {}
          }
        }
        return cors({ killed: stuck.length, topics_restored: topicsRestored });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── RESTORE FAILED ────────────────────────────────────────
    if (url.pathname === "/restore-failed" && request.method === "POST") {
      try {
        const failed = await sbGet(env,
          "jobs?status=eq.failed&select=id,topic,council_score,script_package,cluster");
        let restored = 0, already = 0;
        for (const job of failed) {
          if (!job.topic) continue;
          try {
            const ex = await sbGet(env, "topics?topic=eq." +
              encodeURIComponent(job.topic) + "&select=id,used");
            if (ex.length > 0) {
              if (ex[0].used) {
                await sbPatch(env, "topics?id=eq." + ex[0].id,
                  { used: false, used_at: null });
                restored++;
              } else { already++; }
            } else {
              await sbInsert(env, "topics", {
                cluster: job.cluster || "AI", topic: job.topic, used: false,
                council_score: job.council_score || 75,
                script_package: job.script_package || null,
                source: "restored_from_failed",
                created_at: new Date().toISOString()
              });
              restored++;
            }
          } catch (e) {}
        }
        return cors({ restored, already_in_queue: already,
                      total_failed: failed.length });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── TEST RENDER ───────────────────────────────────────────
    if (url.pathname === "/test-render") {
      const healthUrl = env.MODAL_HEALTH_URL || "NOT_SET";
      try {
        const r    = await fetch(healthUrl,
          { signal: AbortSignal.timeout(15000) });
        const text = await r.text();
        return cors({ url: healthUrl, status: r.status,
                      response: text.slice(0, 400), ok: r.ok });
      } catch (e) {
        return cors({ url: healthUrl, error: e.message, ok: false });
      }
    }

    // ── WEBHOOK ───────────────────────────────────────────────
    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const data = await request.json();
        const { job_id, status, youtube_id, error, script } = data;
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        const u = { status: status || "unknown",
                    updated_at: new Date().toISOString() };
        if (youtube_id) u.youtube_id = youtube_id;
        if (error)      u.error      = error;
        if (script)     u.script_package = { text: script };
        await sbPatch(env, "jobs?id=eq." + job_id, u);
        if (status === "complete" && youtube_id && youtube_id !== "TEST_MODE")
          ctx.waitUntil(createAnalyticsRecord(job_id, youtube_id, env));
        return cors({ received: true, job_id, status });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── SYNC ANALYTICS ────────────────────────────────────────
    if (url.pathname === "/sync-analytics" && request.method === "POST") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      return cors({ status: "sync_started" });
    }

    // ── STAGING QUEUE ─────────────────────────────────────────
    if (url.pathname === "/staging") {
      try {
        const staged = await sbGet(env,
          "jobs?status=eq.staged&order=created_at.asc" +
          "&select=id,topic,cluster,script_package,video_r2_url,created_at,council_score");
        return cors(staged);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── CBDP QUEUE ────────────────────────────────────────────
    // "Completed But Didn't Publish" — rendered OK, YouTube upload failed
    if (url.pathname === "/cbdp") {
      try {
        const cbdp = await sbGet(env,
          "jobs?status=eq.cbdp&order=created_at.desc" +
          "&select=id,topic,cluster,script_package,video_r2_url,created_at,council_score,error");
        return cors(cbdp);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── RETRY CBDP UPLOAD ─────────────────────────────────────
    if (url.pathname === "/retry-upload" && request.method === "POST") {
      try {
        const { job_id } = await request.json();
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        const retryUrl = env.RETRY_UPLOAD_URL || "";
        if (!retryUrl) return cors({ error: "RETRY_UPLOAD_URL not set in Cloudflare bindings" }, 500);
        // Mark as retrying so UI updates
        await sbPatch(env, "jobs?id=eq." + job_id,
          { status: "upload", error: null, updated_at: new Date().toISOString() });
        // Fire and forget — Modal handles it async
        ctx.waitUntil(
          fetch(retryUrl, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ job_id })
          }).catch(e => console.error("retry-upload trigger:", e.message))
        );
        return cors({ status: "retry_triggered", job_id });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── MARK FAILED AS CBDP (bulk) ────────────────────────────
    // Run once to convert existing failed upload jobs to cbdp status
    if (url.pathname === "/mark-cbdp" && request.method === "POST") {
      try {
        const failed = await sbGet(env,
          "jobs?status=eq.failed&select=id,error,script_package");
        const uploadKeywords = ["400", "401", "403", "youtube", "upload",
                                "quota", "invaliddescription", "bad request"];
        let marked = 0;
        for (const job of failed) {
          const err = (job.error || "").toLowerCase();
          const hasScript = !!(job.script_package);
          const isUploadFail = uploadKeywords.some(k => err.includes(k));
          if (isUploadFail && hasScript) {
            await sbPatch(env, "jobs?id=eq." + job.id,
              { status: "cbdp", updated_at: new Date().toISOString() });
            marked++;
          }
        }
        return cors({ marked, total_failed: failed.length });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── UPLOAD VOICE ──────────────────────────────────────────
    if (url.pathname === "/upload-voice" && request.method === "POST") {
      try {
        const jobId = url.searchParams.get("job_id");
        if (!jobId) return cors({ error: "Missing job_id" }, 400);
        const blob  = await request.arrayBuffer();
        const r2Key = "voices/" + jobId + "/voice.webm";
        if (env.R2) {
          await env.R2.put(r2Key, blob,
            { httpMetadata: { contentType: "audio/webm" } });
        }
        await sbPatch(env, "jobs?id=eq." + jobId,
          { voice_r2_url: r2Key, updated_at: new Date().toISOString() });
        return cors({ status: "uploaded", r2_key: r2Key, job_id: jobId });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── MUSIC LIBRARY ─────────────────────────────────────────
    if (url.pathname === "/music-library") {
      return cors({ tracks: [
        { id: "epic_01",      label: "Epic Rise",         category: "Epic",      duration: 45 },
        { id: "hopeful_01",   label: "Hopeful Morning",   category: "Hopeful",   duration: 52 },
        { id: "tech_01",      label: "Digital Pulse",     category: "Tech",      duration: 38 },
        { id: "emotional_01", label: "Stirring Moment",   category: "Emotional", duration: 60 },
        { id: "neutral_01",   label: "Subtle Background", category: "Neutral",   duration: 44 },
      ]});
    }

    // ── MIX TRIGGER ───────────────────────────────────────────
    if (url.pathname === "/mix" && request.method === "POST") {
      try {
        const body = await request.json();
        const { job_id, music_track, music_volume,
                publish_at, voice_offset_ms } = body;
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        const jobs = await sbGet(env,
          "jobs?id=eq." + job_id +
          "&select=id,topic,video_r2_url,voice_r2_url");
        if (!jobs.length) return cors({ error: "Job not found" }, 404);
        const job = jobs[0];
        if (!job.voice_r2_url)
          return cors({ error: "No voice recording for this job" }, 400);
        const mixerUrl = env.MIXER_URL || "";
        if (!mixerUrl) return cors({ error: "MIXER_URL not set" }, 500);
        const r2Base   = (env.R2_BASE_URL || "").replace(/\/$/, "");
        await sbPatch(env, "jobs?id=eq." + job_id,
          { status: "mixing", updated_at: new Date().toISOString() });
        const r = await fetch(mixerUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            job_id,
            video_url:       r2Base + "/" + job.video_r2_url,
            voice_url:       r2Base + "/" + job.voice_r2_url,
            music_track:     music_track     || "neutral_01",
            music_volume:    music_volume    || 0.08,
            publish_at:      publish_at      || null,
            voice_offset_ms: voice_offset_ms || 0,
          })
        });
        if (!r.ok) throw new Error("Mixer returned " + r.status);
        return cors({ status: "mixing_started", job_id });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── CALENDAR ──────────────────────────────────────────────
    if (url.pathname === "/calendar") {
      try {
        const rows = await sbGet(env,
          "jobs?status=in.(staged,mixing,complete)" +
          "&order=scheduled_at.asc.nullslast,created_at.desc" +
          "&select=id,topic,cluster,status,youtube_id,scheduled_at,created_at");
        return cors(rows);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    return cors({ error: "route_not_found" }, 404);
  },

  // ── SCHEDULED CRON ──────────────────────────────────────────
  async scheduled(event, env, ctx) {
    const cron = event.cron;

    if (cron === "* * * * *") {
      await processQueue(env, ctx);
      if (env.MODAL_HEALTH_URL)
        fetch(env.MODAL_HEALTH_URL).catch(() => {});
      if (env.TOPIC_COUNCIL_URL)
        fetch(env.TOPIC_COUNCIL_URL + "/health").catch(() => {});
    }

    if (cron === "30 0,6,12 * * *") {
      try {
        const rows = await sbGet(env,
          "system_state?id=eq.main&select=videos_per_day");
        const vpd  = rows[0]?.videos_per_day || 1;
        const utcH = new Date().getUTCHours();
        const fire =
          vpd === 3 ||
          (vpd === 2 && (utcH === 0 || utcH === 12)) ||
          (vpd === 1 && utcH === 6);
        if (fire) {
          const t = await pickTopic(env, null);
          const j = await createJob(t, env);
          ctx.waitUntil(triggerRender(j, env));
          console.log("Scheduled:", j.id, t.topic);
        }
      } catch (e) { console.error("Scheduled:", e.message); }
    }

    if (cron === "30 20 * * *") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      try {
        const av = await sbGet(env,
          "topics?used=eq.false&council_score=gte.70&select=id");
        if (av.length < 5) ctx.waitUntil(triggerReplenish(env, 12, null));
      } catch (e) { console.error("Queue check:", e.message); }
    }
  }
};

// ============================================================
// HELPERS
// ============================================================

// ── CATEGORIES ────────────────────────────────────────────────
const CATEGORIES = {
  AI:        { label: "AI & ML",          color: "#00e5ff", emoji: "🤖" },
  Space:     { label: "Space & Defence",  color: "#b388ff", emoji: "🚀" },
  Gadgets:   { label: "Gadgets & Tech",   color: "#ffd740", emoji: "📱" },
  DeepTech:  { label: "Deep Tech",        color: "#ff6b35", emoji: "🔬" },
  GreenTech: { label: "Green & Energy",   color: "#00e676", emoji: "⚡" },
  Startups:  { label: "Startups",         color: "#ff6b9d", emoji: "💡" },
};
const ALL_CATS = Object.keys(CATEGORIES);

const VPD_SCHEDULES = {
  1: ["12:00 PM IST"],
  2: ["6:00 AM IST", "6:00 PM IST"],
  3: ["6:00 AM IST", "12:00 PM IST", "6:00 PM IST"],
};

// ── SUPABASE ──────────────────────────────────────────────────
function sbh(env) {
  return {
    apikey: env.SUPABASE_ANON_KEY,
    Authorization: "Bearer " +
      (env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_ANON_KEY),
    "Content-Type": "application/json"
  };
}
async function sbGet(env, ep) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + ep,
    { headers: sbh(env) });
  if (!r.ok) throw new Error("GET " + r.status + " " + ep);
  return r.json();
}
async function sbInsert(env, table, data) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + table, {
    method: "POST",
    headers: { ...sbh(env), Prefer: "return=representation" },
    body: JSON.stringify(data)
  });
  if (!r.ok) {
    const b = await r.text();
    throw new Error("INSERT " + r.status + " " + b.slice(0, 200));
  }
  return (await r.json())[0];
}
async function sbPatch(env, ep, data) {
  const r = await fetch(env.SUPABASE_URL + "/rest/v1/" + ep, {
    method: "PATCH",
    headers: { ...sbh(env), Prefer: "return=minimal" },
    body: JSON.stringify(data)
  });
  return r.ok;
}
async function upsertState(env, data) {
  // Try patch first, insert if missing
  try {
    const rows = await sbGet(env, "system_state?id=eq.main&select=id");
    if (rows.length > 0) {
      await sbPatch(env, "system_state?id=eq.main",
        { ...data, updated_at: new Date().toISOString() });
    } else {
      await sbInsert(env, "system_state",
        { id: "main", ...data });
    }
  } catch (e) {
    console.error("upsertState:", e.message);
  }
}

// ── RESPONSE ──────────────────────────────────────────────────
function cors(data, status) {
  return new Response(JSON.stringify(data, null, 2), {
    status: status || 200,
    headers: {
      "content-type": "application/json",
      "Access-Control-Allow-Origin":  "*",
      "Access-Control-Allow-Headers": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS,PATCH"
    }
  });
}

// ── TOPIC SELECTION ───────────────────────────────────────────
async function pickTopic(env, preferCategory) {
  let ep = "topics?used=eq.false&council_score=gte.70" +
           "&order=council_score.desc&limit=1";
  if (preferCategory && ALL_CATS.includes(preferCategory))
    ep = "topics?used=eq.false&council_score=gte.70&cluster=eq." +
         preferCategory + "&order=council_score.desc&limit=1";
  const t = await sbGet(env, ep);
  if (t.length > 0) {
    await sbPatch(env, "topics?id=eq." + t[0].id,
      { used: true, used_at: new Date().toISOString() });
    return { topic: t[0].topic, script_package: t[0].script_package,
             council_score: t[0].council_score,
             category: t[0].cluster || "AI", source: "db_approved" };
  }
  // Fallback pool
  const pool = [
    "India AI healthcare revolution in rural areas",
    "ISRO next space mission changing everything",
    "India EV revolution what is actually happening",
    "AI chips made in India semiconductor story",
    "India solar energy breakthrough",
  ];
  const topic = pool[Math.floor(Math.random() * pool.length)];
  return { topic, script_package: null, council_score: 0,
           category: "AI", source: "fallback" };
}

async function callCouncil(env, topic, source, category) {
  if (!env.TOPIC_COUNCIL_URL) throw new Error("TOPIC_COUNCIL_URL not set");
  const r = await fetch(env.TOPIC_COUNCIL_URL + "/full-pipeline", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ topic, source, category })
  });
  if (!r.ok) throw new Error("Council returned " + r.status);
  return r.json();
}

async function triggerReplenish(env, target, categories) {
  if (!env.TOPIC_COUNCIL_URL) return;
  try {
    await fetch(env.TOPIC_COUNCIL_URL + "/replenish", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ target, categories: categories || ALL_CATS })
    });
  } catch (e) { console.error("Replenish:", e.message); }
}

// ── JOB MANAGEMENT ────────────────────────────────────────────
async function createJob(t, env) {
  return await sbInsert(env, "jobs", {
    topic: t.topic, cluster: t.category || "AI", status: "pending",
    script_package: t.script_package || null,
    council_score: t.council_score || 0,
    retries: 0,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  });
}

async function processQueue(env, ctx) {
  const ago = new Date(Date.now() - 15 * 60000).toISOString();
  try {
    // Reset stuck jobs
    for (const j of await sbGet(env,
      "jobs?status=eq.processing&updated_at=lt." + ago + "&retries=lt.3"))
      await sbPatch(env, "jobs?id=eq." + j.id,
        { status: "pending", retries: (j.retries || 0) + 1,
          updated_at: new Date().toISOString() });
    for (const j of await sbGet(env,
      "jobs?status=eq.processing&updated_at=lt." + ago + "&retries=gte.3"))
      await sbPatch(env, "jobs?id=eq." + j.id,
        { status: "failed", error: "max_retries_exceeded",
          updated_at: new Date().toISOString() });
    // Trigger next pending
    const pending = await sbGet(env,
      "jobs?status=eq.pending&order=created_at.asc&limit=1");
    if (!pending.length) return;
    await sbPatch(env, "jobs?id=eq." + pending[0].id,
      { status: "processing", started_at: new Date().toISOString(),
        updated_at: new Date().toISOString() });
    ctx.waitUntil(triggerRender(pending[0], env));
  } catch (e) { console.error("Queue:", e.message); }
}

async function triggerRender(job, env) {
  if (!env.RENDER_PIPELINE_URL) {
    await sbPatch(env, "jobs?id=eq." + job.id,
      { status: "failed", error: "RENDER_PIPELINE_URL not set",
        updated_at: new Date().toISOString() });
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
    if (!r.ok) throw new Error(r.status + ": " + (await r.text()).slice(0, 100));
  } catch (e) {
    console.error("Render trigger:", e.message);
    await sbPatch(env, "jobs?id=eq." + job.id,
      { status: "failed", error: e.message,
        updated_at: new Date().toISOString() });
  }
}

// ── ANALYTICS ─────────────────────────────────────────────────
async function createAnalyticsRecord(job_id, youtube_id, env) {
  try {
    await sbInsert(env, "analytics", {
      video_id: job_id, youtube_views: 0, youtube_likes: 0,
      comment_count: 0, score: 0,
      created_at: new Date().toISOString()
    });
  } catch (e) {}
}

async function syncYouTubeAnalytics(env) {
  if (!env.YOUTUBE_CLIENT_ID) return;
  try {
    const jobs = (await sbGet(env,
      "jobs?status=eq.complete&youtube_id=not.is.null" +
      "&order=created_at.desc&limit=50"))
      .filter(j => j.youtube_id && j.youtube_id !== "TEST_MODE");
    if (!jobs.length) return;
    const tr = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id:     env.YOUTUBE_CLIENT_ID,
        client_secret: env.YOUTUBE_CLIENT_SECRET,
        refresh_token: env.YOUTUBE_REFRESH_TOKEN,
        grant_type:    "refresh_token"
      })
    });
    if (!tr.ok) return;
    const token = (await tr.json()).access_token;
    for (let i = 0; i < jobs.length; i += 50) {
      const batch = jobs.slice(i, i + 50);
      const res = await fetch(
        "https://www.googleapis.com/youtube/v3/videos?part=statistics&id=" +
        batch.map(j => j.youtube_id).join(",") + "&access_token=" + token
      );
      if (!res.ok) continue;
      for (const item of (await res.json()).items || []) {
        const s = item.statistics || {};
        const views    = parseInt(s.viewCount    || 0);
        const likes    = parseInt(s.likeCount    || 0);
        const comments = parseInt(s.commentCount || 0);
        const score    = views + likes * 50 + comments * 30;
        const job = batch.find(j => j.youtube_id === item.id);
        if (!job) continue;
        const ex = await sbGet(env,
          "analytics?video_id=eq." + job.id);
        if (ex.length > 0)
          await sbPatch(env, "analytics?video_id=eq." + job.id,
            { youtube_views: views, youtube_likes: likes,
              comment_count: comments, score,
              updated_at: new Date().toISOString() });
        else
          await sbInsert(env, "analytics", {
            video_id: job.id, youtube_views: views, youtube_likes: likes,
            comment_count: comments, score,
            created_at: new Date().toISOString()
          });
      }
    }
  } catch (e) { console.error("Analytics sync:", e.message); }
}

// ============================================================
// DASHBOARD HTML
// ============================================================
function buildDashboard(env) {
  const r2Base = (env && env.R2_BASE_URL) ? env.R2_BASE_URL.replace(/\/$/, "") : "";
  return `<!DOCTYPE html>
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
.topbar{position:relative;z-index:10;display:flex;align-items:center;justify-content:space-between;padding:0 24px;height:56px;border-bottom:1px solid var(--border);background:rgba(8,12,20,0.95);backdrop-filter:blur(10px);flex-shrink:0}
.logo-name{font-size:1.1rem;font-weight:800;letter-spacing:-0.02em}.logo-name span{color:var(--accent)}
.logo-sub{font-family:var(--mono);font-size:.58rem;color:var(--muted);letter-spacing:.12em;text-transform:uppercase;margin-top:1px}
.topbar-nav{display:flex;align-items:center;gap:3px}
.nav-btn{display:flex;align-items:center;gap:5px;padding:5px 13px;border-radius:7px;border:none;background:transparent;font-family:var(--font);font-size:.8rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s}
.nav-btn:hover{color:var(--text);background:rgba(255,255,255,.05)}.nav-btn.active{color:var(--text);background:var(--surface2);border:1px solid var(--border2)}
.live-dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}
.live-lbl{font-family:var(--mono);font-size:.65rem;color:var(--green)}
.pages{flex:1;overflow:hidden;position:relative;z-index:1}
.page{display:none;height:100%;overflow-y:auto;padding:24px}.page.active{display:block}
.page::-webkit-scrollbar{width:4px}.page::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden}
.stat::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:10px 10px 0 0}
.s1::before{background:var(--accent)}.s2::before{background:var(--yellow)}.s3::before{background:var(--green)}.s4::before{background:var(--red)}.s5::before{background:var(--purple)}
.stat-val{font-size:1.9rem;font-weight:800;letter-spacing:-0.04em;line-height:1;margin-bottom:3px}
.s1 .stat-val{color:var(--accent)}.s2 .stat-val{color:var(--yellow)}.s3 .stat-val{color:var(--green)}.s4 .stat-val{color:var(--red)}.s5 .stat-val{color:var(--purple)}
.stat-lbl{font-family:var(--mono);font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}
.actions{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:5px;padding:8px 16px;border-radius:8px;border:none;font-family:var(--font);font-size:.8rem;font-weight:600;cursor:pointer;transition:all .18s;white-space:nowrap}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn-primary{background:var(--accent);color:#000}.btn-primary:hover:not(:disabled){filter:brightness(1.15);transform:translateY(-1px)}
.btn-ghost{background:var(--surface2);color:var(--text);border:1px solid var(--border2)}.btn-ghost:hover:not(:disabled){border-color:var(--accent);color:var(--accent)}
.btn-purple{background:rgba(179,136,255,.1);color:var(--purple);border:1px solid rgba(179,136,255,.2)}.btn-purple:hover:not(:disabled){background:rgba(179,136,255,.2)}
.btn-orange{background:rgba(255,107,53,.1);color:var(--accent2);border:1px solid rgba(255,107,53,.2)}.btn-orange:hover:not(:disabled){background:rgba(255,107,53,.2)}
.btn-red{background:rgba(255,82,82,.08);color:var(--red);border:1px solid rgba(255,82,82,.2)}.btn-red:hover:not(:disabled){background:rgba(255,82,82,.18)}
.btn-green{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}.btn-green:hover:not(:disabled){background:rgba(0,230,118,.2)}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:16px}
.panel-head{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.panel-title{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--muted)}
.panel-sub{font-family:var(--mono);font-size:.62rem;color:var(--muted)}
.tabs{display:flex;gap:3px;padding:9px 12px;border-bottom:1px solid var(--border)}
.tab{display:flex;align-items:center;gap:5px;padding:5px 11px;border-radius:6px;border:1px solid transparent;background:transparent;font-family:var(--font);font-size:.75rem;font-weight:600;color:var(--muted);cursor:pointer;transition:all .15s}
.tab:hover{color:var(--text);background:var(--surface2)}.tab.active{color:var(--text);background:var(--surface2);border-color:var(--border2)}
.tab-count{font-family:var(--mono);font-size:.6rem;padding:1px 6px;border-radius:20px}
.tc-all{background:rgba(0,229,255,.12);color:var(--accent)}.tc-run{background:rgba(255,215,64,.12);color:var(--yellow)}.tc-ok{background:rgba(0,230,118,.12);color:var(--green)}.tc-fail{background:rgba(255,82,82,.12);color:var(--red)}
.job-item{display:grid;grid-template-columns:1fr 110px 160px 70px;gap:10px;padding:11px 16px;border-bottom:1px solid var(--border);align-items:center;transition:background .12s}
.job-item:hover{background:rgba(255,255,255,.02)}.job-item:last-child{border-bottom:none}
.job-topic{font-size:.82rem;font-weight:600;color:var(--text);line-height:1.3;margin-bottom:2px}
.job-meta{font-family:var(--mono);font-size:.62rem;color:var(--muted);display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.job-err{color:#ff6b6b;max-width:220px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.badge{display:inline-flex;align-items:center;gap:3px;padding:2px 8px;border-radius:5px;font-family:var(--mono);font-size:.63rem;font-weight:500;text-transform:uppercase;letter-spacing:.03em;white-space:nowrap}
.bdot{width:5px;height:5px;border-radius:50%;background:currentColor}
.b-pending{background:rgba(255,215,64,.1);color:var(--yellow);border:1px solid rgba(255,215,64,.2)}
.b-processing,.b-upload{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.2)}
.b-images,.b-voice{background:rgba(179,136,255,.1);color:var(--purple);border:1px solid rgba(179,136,255,.2)}
.b-render{background:rgba(255,107,53,.1);color:var(--accent2);border:1px solid rgba(255,107,53,.2)}
.b-complete,.b-test_complete{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.b-staged,.b-mixing{background:rgba(255,215,64,.1);color:var(--yellow);border:1px solid rgba(255,215,64,.2)}
.b-failed{background:rgba(255,82,82,.1);color:var(--red);border:1px solid rgba(255,82,82,.2)}
.b-pending .bdot,.b-processing .bdot{animation:blink 1.4s infinite}
.prog-wrap{display:flex;flex-direction:column;gap:4px}
.prog-bar{height:3px;background:rgba(255,255,255,.06);border-radius:2px;overflow:hidden}
.prog-fill{height:100%;border-radius:2px;transition:width .6s}
.prog-pct{font-family:var(--mono);font-size:.58rem;color:var(--muted)}
.time-cell{font-family:var(--mono);font-size:.63rem;color:var(--muted);text-align:right}
.yt-link{color:var(--accent);text-decoration:none;font-family:var(--mono);font-size:.63rem}.yt-link:hover{color:#fff}
.empty{padding:40px 20px;text-align:center;color:var(--muted);font-family:var(--mono);font-size:.72rem;line-height:1.9}
.empty-icon{font-size:1.8rem;display:block;margin-bottom:10px;opacity:.3}
.two-col{display:grid;grid-template-columns:1fr 280px;gap:16px;align-items:start}
.topic-row{padding:12px 16px;border-bottom:1px solid var(--border);transition:background .12s}
.topic-row:hover{background:rgba(255,255,255,.02)}.topic-row:last-child{border-bottom:none}
.topic-text{font-size:.8rem;font-weight:600;color:var(--text);line-height:1.4;margin-bottom:4px}
.topic-foot{display:flex;align-items:center;justify-content:space-between}
.score-pill{font-family:var(--mono);font-size:.62rem;font-weight:500;padding:2px 7px;border-radius:4px}
.sc-hi{background:rgba(0,230,118,.12);color:var(--green)}.sc-med{background:rgba(255,215,64,.12);color:var(--yellow)}.sc-lo{background:rgba(255,82,82,.12);color:var(--red)}
.src-tag{font-family:var(--mono);font-size:.58rem;color:var(--muted)}
.analytics-hero{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.hero-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:18px;text-align:center}
.hero-val{font-size:2rem;font-weight:800;letter-spacing:-0.04em;margin-bottom:4px}
.hero-lbl{font-family:var(--mono);font-size:.63rem;color:var(--muted);text-transform:uppercase;letter-spacing:.07em}
.hv-views{color:#60b4ff}.hv-likes{color:#ff6b9d}.hv-comments{color:var(--yellow)}.hv-score{color:var(--accent)}
.perf-row{display:grid;grid-template-columns:1fr 70px 70px 88px;gap:10px;padding:10px 16px;border-bottom:1px solid var(--border);align-items:center}
.perf-row:last-child{border-bottom:none}
.perf-topic{font-size:.78rem;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.perf-num{font-family:var(--mono);font-size:.68rem;text-align:right}
.pn-views{color:#60b4ff}.pn-likes{color:#ff6b9d}.pn-score{color:var(--yellow);font-weight:600}
.video-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;transition:border-color .2s}
.video-card:hover{border-color:var(--border2)}
.video-thumb{width:100%;aspect-ratio:9/16;background:var(--surface2);display:flex;align-items:center;justify-content:center;font-size:2rem;opacity:.3;max-height:110px}
.video-body{padding:10px}
.video-topic{font-size:.78rem;font-weight:600;color:var(--text);line-height:1.3;margin-bottom:5px}
.video-stats{display:flex;gap:8px;font-family:var(--mono);font-size:.63rem;color:var(--muted);margin-bottom:5px}
.video-stats span b{color:var(--text)}
.video-score{font-family:var(--mono);font-size:.7rem;font-weight:600;color:var(--yellow)}
.video-link{display:inline-flex;align-items:center;gap:4px;color:var(--accent);text-decoration:none;font-family:var(--mono);font-size:.63rem;margin-top:3px}
.used-pill{font-family:var(--mono);font-size:.58rem;padding:1px 6px;border-radius:3px;background:rgba(0,230,118,.1);color:var(--green)}
.used-no{background:rgba(255,82,82,.1);color:var(--red)}
.debug-box{background:rgba(0,0,0,.5);border:1px solid var(--border2);border-radius:8px;padding:11px 14px;font-family:var(--mono);font-size:.68rem;color:var(--muted);line-height:1.9;margin-bottom:14px}
.dk{color:var(--accent)}.dg{color:var(--green)}.dr{color:var(--red)}
.cat-strip{display:flex;gap:6px;margin-bottom:14px;flex-wrap:wrap}
.cat-pill{display:flex;align-items:center;gap:4px;padding:5px 11px;border-radius:20px;border:1px solid var(--border2);background:var(--surface);font-family:var(--mono);font-size:.65rem;cursor:pointer;transition:all .15s;color:var(--muted)}
.cat-pill:hover{color:var(--text)}.cat-count{font-size:.58rem;opacity:.8}
.sched-panel{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;margin-bottom:14px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.vpd-btn{min-width:44px}
.toggle-group{display:flex;align-items:center;gap:8px;padding:5px 12px;border-radius:8px;border:1px solid var(--border2);background:var(--surface2)}
.toggle-switch{width:34px;height:18px;border-radius:9px;cursor:pointer;position:relative;transition:background .2s;border:1px solid rgba(255,255,255,.1);flex-shrink:0}
.toggle-knob{position:absolute;top:2px;left:2px;width:14px;height:14px;border-radius:50%;background:#fff;transition:transform .2s;pointer-events:none}
.toggle-lbl{font-family:var(--mono);font-size:.65rem}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;display:flex;align-items:center;justify-content:center}
.modal-overlay.hidden{display:none}
.modal{background:var(--surface);border:1px solid var(--border2);border-radius:14px;padding:24px;width:400px;max-width:90vw}
.modal-title{font-size:.95rem;font-weight:700;margin-bottom:5px}
.modal-sub{font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-bottom:18px}
.modal-cats{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:18px}
.cat-check{display:flex;align-items:center;gap:7px;padding:9px 11px;border-radius:8px;border:1px solid var(--border);cursor:pointer;transition:all .15s;font-size:.78rem;font-weight:600}
.cat-check:hover{border-color:var(--border2)}.cat-check.selected{border-color:var(--accent);background:rgba(0,229,255,.06);color:var(--accent)}
.modal-actions{display:flex;gap:8px;justify-content:flex-end}
.staged-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px}
.staged-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;cursor:pointer;transition:border-color .2s,transform .15s}
.staged-card:hover{border-color:var(--accent);transform:translateY(-2px)}
.staged-head{padding:12px 14px 9px;border-bottom:1px solid var(--border)}
.staged-topic{font-size:.85rem;font-weight:700;color:var(--text);line-height:1.3;margin-bottom:5px}
.staged-meta{display:flex;gap:7px;align-items:center;flex-wrap:wrap}
.staged-body{padding:10px 14px;font-size:.73rem;color:var(--muted);line-height:1.6;max-height:55px;overflow:hidden}
.staged-foot{padding:9px 14px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.vm-badge{font-family:var(--mono);font-size:.58rem;padding:2px 7px;border-radius:4px}
.vm-ai{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.2)}
.vm-human{background:rgba(0,230,118,.1);color:var(--green);border:1px solid rgba(0,230,118,.2)}
.studio-overlay{position:fixed;inset:0;background:rgba(0,0,0,.92);z-index:200;display:flex;flex-direction:column;overflow:hidden}
.studio-overlay.hidden{display:none}
.studio-top{display:flex;align-items:center;justify-content:space-between;padding:12px 22px;border-bottom:1px solid var(--border);flex-shrink:0}
.studio-body{display:grid;grid-template-columns:1fr 400px;flex:1;overflow:hidden;min-height:0}
.studio-video{background:#000;display:flex;align-items:center;justify-content:center}
.studio-video video{max-height:100%;max-width:100%;object-fit:contain}
.studio-panel{background:var(--surface);border-left:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto}
.studio-sec{padding:14px;border-bottom:1px solid var(--border)}
.studio-sec-title{font-family:var(--mono);font-size:.62rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:10px}
.script-box{font-size:.82rem;line-height:1.8;color:var(--text);background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:12px;max-height:160px;overflow-y:auto}
.rec-row{display:flex;gap:8px;align-items:center;margin-bottom:10px}
.rec-btn{width:40px;height:40px;border-radius:50%;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:1rem;transition:all .18s}
.rec-record{background:#ff3333;color:#fff}.rec-record:hover{background:#ff0000}
.rec-stop{background:var(--surface2);color:var(--text);border:1px solid var(--border2)}.rec-stop:hover{background:var(--red);color:#fff}
.rec-play{background:var(--accent);color:#000}.rec-play:hover{filter:brightness(1.2)}
.rec-reset{background:var(--surface2);color:var(--muted);border:1px solid var(--border)}.rec-reset:hover{color:var(--text)}
.rec-status{font-family:var(--mono);font-size:.68rem;color:var(--muted)}
.rec-status.recording{color:var(--red);animation:blink .8s infinite}
.waveform{width:100%;height:64px;background:var(--surface2);border:1px solid var(--border);border-radius:6px}
.char-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-bottom:6px}
.char-btn{padding:6px 4px;border-radius:7px;border:1px solid var(--border);background:var(--surface2);font-family:var(--mono);font-size:.6rem;color:var(--muted);cursor:pointer;text-align:center;transition:all .15s}
.char-btn:hover{color:var(--text)}.char-btn.active{border-color:var(--accent);color:var(--accent);background:rgba(0,229,255,.06)}
.music-track{display:flex;align-items:center;gap:8px;padding:8px 11px;border-radius:7px;border:1px solid var(--border);background:var(--surface2);cursor:pointer;margin-bottom:5px;transition:all .15s}
.music-track:hover{border-color:var(--border2)}.music-track.selected{border-color:var(--green);background:rgba(0,230,118,.05)}
.music-name{font-size:.76rem;font-weight:600;color:var(--text)}
.music-cat{font-family:var(--mono);font-size:.58rem;color:var(--muted)}
.pub-ctrl{display:flex;flex-direction:column;gap:8px;padding:14px}
.dt-input{width:100%;background:var(--surface2);border:1px solid var(--border2);border-radius:7px;padding:8px 11px;font-family:var(--mono);font-size:.7rem;color:var(--text);outline:none}
.dt-input:focus{border-color:var(--accent)}
.cal-nav{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.cal-month{font-size:.95rem;font-weight:700;color:var(--text)}
.cal-hdr{display:grid;grid-template-columns:repeat(7,1fr);gap:2px;margin-bottom:4px}
.cal-hdr div{text-align:center;font-family:var(--mono);font-size:.58rem;color:var(--muted);padding:3px}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:2px}
.cal-cell{min-height:72px;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:5px}
.cal-cell.today{border-color:var(--accent)}
.cal-dn{font-family:var(--mono);font-size:.62rem;color:var(--muted);margin-bottom:3px}
.cal-evt{font-size:.58rem;font-weight:600;padding:2px 4px;border-radius:3px;margin-bottom:2px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
@media(max-width:900px){.stats{grid-template-columns:repeat(3,1fr)}.two-col{grid-template-columns:1fr}.analytics-hero{grid-template-columns:repeat(2,1fr)}.studio-body{grid-template-columns:1fr}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.actions{flex-wrap:wrap}}
</style>
</head>
<body>
<div class="topbar">
  <div>
    <div class="logo-name">&#127470;&#127475; India<span>20Sixty</span></div>
    <div class="logo-sub">Mission Control V2</div>
  </div>
  <nav class="topbar-nav">
    <button class="nav-btn active" onclick="showPage('home',this)">Home</button>
    <button class="nav-btn"        onclick="showPage('staging',this)">&#127908; Staging <span id="stg-cnt" style="font-family:var(--mono);font-size:.58rem;background:rgba(0,230,118,.15);color:var(--green);padding:1px 5px;border-radius:3px;margin-left:2px">0</span></button>
    <button class="nav-btn"        onclick="showPage('review',this)">&#128250; Review <span id="rev-cnt" style="font-family:var(--mono);font-size:.58rem;background:rgba(255,215,64,.15);color:var(--yellow);padding:1px 5px;border-radius:3px;margin-left:2px">0</span></button>
    <button class="nav-btn"        onclick="showPage('calendar',this)">&#128197; Calendar</button>
    <button class="nav-btn"        onclick="showPage('analytics',this)">Analytics</button>
    <button class="nav-btn"        onclick="showPage('topics',this)">Topics</button>
  </nav>
  <div style="display:flex;align-items:center;gap:10px">
    <div class="toggle-group">
      <span class="toggle-lbl" id="vm-lbl" style="color:var(--accent)">&#129302; AI VOICE</span>
      <div class="toggle-switch" id="vm-tog" onclick="toggleVoiceMode()" style="background:var(--accent)">
        <div class="toggle-knob" id="vm-knob"></div>
      </div>
    </div>
    <div class="toggle-group">
      <span class="toggle-lbl" id="pub-lbl" style="color:var(--red)">PUBLISH OFF</span>
      <div class="toggle-switch" id="pub-tog" onclick="togglePublish()" style="background:var(--red)">
        <div class="toggle-knob" id="pub-knob"></div>
      </div>
    </div>
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
  <div class="sched-panel">
    <span style="font-family:var(--mono);font-size:.68rem;color:var(--muted)">Videos/day:</span>
    <div style="display:flex;gap:6px">
      <button class="btn btn-ghost vpd-btn" id="vpd-1" onclick="setVPD(1)">1</button>
      <button class="btn btn-ghost vpd-btn" id="vpd-2" onclick="setVPD(2)">2</button>
      <button class="btn btn-ghost vpd-btn" id="vpd-3" onclick="setVPD(3)">3</button>
    </div>
    <span style="font-family:var(--mono);font-size:.68rem;color:var(--accent)" id="sched-times"></span>
    <span style="font-family:var(--mono);font-size:.65rem;color:var(--muted)" id="sched-desc"></span>
  </div>
  <div class="cat-strip" id="cat-strip">
    <div class="cat-pill" data-cat="all" onclick="filterByCat('all',this)" style="border-color:var(--accent);color:var(--accent)">All</div>
  </div>
  <div class="actions">
    <button class="btn btn-primary"  id="bc" onclick="doCreateJob()">&#9654; Create Video</button>
    <button class="btn btn-ghost"    id="bg" onclick="doGenerateTopic()">&#10022; Generate Topic</button>
    <button class="btn btn-purple"   id="br" onclick="openReplenishModal()">&#8635; Replenish Queue</button>
    <button class="btn btn-red"      id="bk" onclick="doKillIncomplete()">&#9940; Kill Incomplete</button>
    <button class="btn btn-ghost"    id="bf" onclick="doRestoreFailed()">&#8617; Restore Failed</button>
    <button class="btn btn-orange"   id="bt" onclick="doTestRender()">&#9741; Test Render</button>
  </div>
  <div id="debug-home"></div>
  <div class="two-col">
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Jobs</span><span class="panel-sub" id="last-ref"></span></div>
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
        <div class="panel-head"><span class="panel-title">Queue</span></div>
        <div id="queue-list"></div>
      </div>
    </div>
  </div>
</div>

<!-- STAGING -->
<div class="page" id="page-staging">
  <div id="staging-banner" class="debug-box" style="display:none"></div>
  <div class="tabs" style="margin-bottom:14px;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:8px 12px">
    <button class="tab active" id="stab-staged" onclick="switchStagingTab('staged')">&#127908; Awaiting Voice <span class="tab-count tc-run" id="stc-staged">0</span></button>
    <button class="tab"        id="stab-cbdp"   onclick="switchStagingTab('cbdp')">&#9888; CBDP <span class="tab-count tc-fail" id="stc-cbdp">0</span></button>
  </div>
  <div id="stab-panel-staged">
    <div class="staged-grid" id="staged-grid"></div>
  </div>
  <div id="stab-panel-cbdp" style="display:none">
    <div style="font-family:var(--mono);font-size:.72rem;color:var(--muted);margin-bottom:12px;padding:10px 14px;background:rgba(255,82,82,.06);border:1px solid rgba(255,82,82,.15);border-radius:8px">
      &#9888; These videos rendered and voiced OK but failed at YouTube upload. Click Retry Upload to re-attempt without re-rendering.
    </div>
    <div class="staged-grid" id="cbdp-grid"></div>
  </div>
</div>

<!-- CALENDAR -->
<div class="page" id="page-calendar">
  <div class="cal-nav">
    <button class="btn btn-ghost" onclick="calPrev()">&#8592;</button>
    <span class="cal-month" id="cal-lbl"></span>
    <button class="btn btn-ghost" onclick="calNext()">&#8594;</button>
    <button class="btn btn-ghost" onclick="calToday()" style="margin-left:6px">Today</button>
  </div>
  <div class="panel" style="padding:12px">
    <div class="cal-hdr">
      <div>SUN</div><div>MON</div><div>TUE</div><div>WED</div><div>THU</div><div>FRI</div><div>SAT</div>
    </div>
    <div class="cal-grid" id="cal-grid"></div>
  </div>
</div>

<!-- ANALYTICS -->
<div class="page" id="page-analytics">
  <div class="actions" style="margin-bottom:18px">
    <button class="btn btn-ghost" onclick="doSyncAnalytics()">&#8635; Sync YouTube</button>
  </div>
  <div class="analytics-hero">
    <div class="hero-card"><div class="hero-val hv-views" id="a-views">-</div><div class="hero-lbl">Views</div></div>
    <div class="hero-card"><div class="hero-val hv-likes" id="a-likes">-</div><div class="hero-lbl">Likes</div></div>
    <div class="hero-card"><div class="hero-val hv-comments" id="a-comments">-</div><div class="hero-lbl">Comments</div></div>
    <div class="hero-card"><div class="hero-val hv-score" id="a-avg">-</div><div class="hero-lbl">Avg Score</div></div>
  </div>
  <div class="two-col">
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Videos</span><span class="panel-sub" id="a-count"></span></div>
        <div id="video-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px;padding:12px"></div>
      </div>
    </div>
    <div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Top Performers</span></div>
        <div class="perf-row" style="opacity:.4;font-family:var(--mono);font-size:.6rem"><div>TOPIC</div><div style="text-align:right">VIEWS</div><div style="text-align:right">LIKES</div><div style="text-align:right">SCORE</div></div>
        <div id="perf-list"></div>
      </div>
      <div class="panel">
        <div class="panel-head"><span class="panel-title">Needs Attention</span></div>
        <div class="perf-row" style="opacity:.4;font-family:var(--mono);font-size:.6rem"><div>TOPIC</div><div style="text-align:right">VIEWS</div><div style="text-align:right">LIKES</div><div style="text-align:right">SCORE</div></div>
        <div id="flop-list"></div>
      </div>
    </div>
  </div>
</div>

<!-- TOPICS -->
<div class="page" id="page-topics">
  <div class="actions" style="margin-bottom:14px">
    <button class="btn btn-ghost"   id="bt-all"   onclick="filterTopics('all')">All</button>
    <button class="btn btn-primary" id="bt-ready" onclick="filterTopics('ready')">Ready</button>
    <button class="btn btn-ghost"   id="bt-used"  onclick="filterTopics('used')">Used</button>
    <button class="btn btn-purple"  onclick="openReplenishModal()">&#8635; Replenish</button>
  </div>
  <div class="cat-strip" id="topic-cat-strip">
    <div class="cat-pill" data-cat="all" onclick="filterTopicsByCat('all',this)" style="border-color:var(--accent);color:var(--accent)">All</div>
  </div>
  <div class="panel">
    <div class="panel-head"><span class="panel-title">Topics</span><span class="panel-sub" id="topics-count">-</span></div>
    <div id="topics-list"></div>
  </div>
</div>

<!-- CBDP REVIEW PAGE -->
<div class="page" id="page-review">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
    <div>
      <div style="font-size:1rem;font-weight:700;color:var(--text)">Review Queue <span id="cbdp-count" style="font-family:var(--mono);font-size:.7rem;background:rgba(255,215,64,.12);color:var(--yellow);padding:2px 8px;border-radius:4px;margin-left:6px">0</span></div>
      <div style="font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-top:2px">Completed But Didn't Publish — watch each video then decide</div>
    </div>
  </div>
  <div id="cbdp-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:14px"></div>
</div>

</div><!-- /pages -->

<!-- REPLENISH MODAL -->
<div class="modal-overlay hidden" id="rep-modal">
  <div class="modal">
    <div class="modal-title">Replenish Topic Queue</div>
    <div class="modal-sub">Select categories to scout real news from</div>
    <div class="modal-cats" id="modal-cats"></div>
    <div style="margin-bottom:14px">
      <div style="font-family:var(--mono);font-size:.65rem;color:var(--muted);margin-bottom:5px">Target count</div>
      <input type="range" id="tgt-slider" min="5" max="30" value="12" oninput="document.getElementById('tgt-val').textContent=this.value" style="width:100%">
      <div style="font-family:var(--mono);font-size:.68rem;color:var(--accent);margin-top:3px"><span id="tgt-val">12</span> topics</div>
    </div>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeReplenishModal()">Cancel</button>
      <button class="btn btn-purple" onclick="doReplenish()">&#8635; Start</button>
    </div>
  </div>
</div>

<!-- STUDIO OVERLAY -->
<div class="studio-overlay hidden" id="studio">
  <div class="studio-top">
    <div>
      <div style="font-size:.95rem;font-weight:700;color:var(--text)" id="stu-title">Studio</div>
      <div style="font-family:var(--mono);font-size:.62rem;color:var(--muted)" id="stu-id"></div>
    </div>
    <div style="display:flex;gap:8px">
      <button class="btn btn-ghost" onclick="previewMix()">&#9654; Preview</button>
      <button class="btn btn-ghost" onclick="closeStudio()">&#10005; Close</button>
    </div>
  </div>
  <div class="studio-body">
    <div class="studio-video">
      <video id="stu-vid" controls style="width:100%;height:100%;max-height:calc(100vh - 56px)"></video>
    </div>
    <div class="studio-panel">
      <div class="studio-sec">
        <div class="studio-sec-title">&#128196; Script</div>
        <div class="script-box" id="stu-script">Loading...</div>
      </div>
      <div class="studio-sec">
        <div class="studio-sec-title">&#127908; Voice Recording</div>
        <div class="rec-row">
          <button class="rec-btn rec-record" id="rec-rec" onclick="startRec()" title="Record">&#9210;</button>
          <button class="rec-btn rec-stop"   id="rec-stp" onclick="stopRec()"  title="Stop" disabled>&#9209;</button>
          <button class="rec-btn rec-play"   id="rec-ply" onclick="playRec()"  title="Play" disabled>&#9654;</button>
          <button class="rec-btn rec-reset"  id="rec-rst" onclick="resetRec()" title="Reset" disabled>&#8634;</button>
          <span class="rec-status" id="rec-status">Ready</span>
        </div>
        <canvas class="waveform" id="waveform"></canvas>
        <div style="display:flex;gap:8px;margin-top:8px;font-family:var(--mono);font-size:.65rem;color:var(--muted)">
          <span>Duration: <b id="rec-dur" style="color:var(--text)">0:00</b></span>
          <span style="margin-left:auto">Offset: <input type="number" id="voice-off" value="0" min="-2000" max="2000" step="100" style="width:58px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);font-family:var(--mono);font-size:.65rem;padding:2px 4px"> ms</span>
        </div>
      </div>
      <div class="studio-sec">
        <div class="studio-sec-title">&#127917; Voice Character</div>
        <div class="char-grid">
          <div class="char-btn active" onclick="setChar(this,'natural')">&#128528; Natural</div>
          <div class="char-btn"        onclick="setChar(this,'woman')">&#128105; Woman</div>
          <div class="char-btn"        onclick="setChar(this,'man')">&#128104; Man</div>
          <div class="char-btn"        onclick="setChar(this,'elder')">&#129490; Elder</div>
          <div class="char-btn"        onclick="setChar(this,'child')">&#128102; Child</div>
          <div class="char-btn"        onclick="setChar(this,'radio')">&#128251; Radio</div>
        </div>
        <div style="font-family:var(--mono);font-size:.6rem;color:var(--muted);margin-top:6px" id="char-desc">No pitch shift</div>
      </div>
      <div class="studio-sec">
        <div class="studio-sec-title">&#127925; Music</div>
        <div id="music-list"></div>
        <div style="display:flex;align-items:center;gap:8px;margin-top:8px;font-family:var(--mono);font-size:.65rem;color:var(--muted)">
          <span>Vol:</span>
          <input type="range" id="mus-vol" min="0" max="20" value="8" oninput="document.getElementById('mus-vol-v').textContent=this.value+'%'" style="flex:1">
          <span id="mus-vol-v">8%</span>
        </div>
      </div>
      <div class="pub-ctrl">
        <div class="studio-sec-title">&#128197; Schedule &amp; Publish</div>
        <input type="datetime-local" id="pub-at" class="dt-input">
        <div style="display:flex;gap:7px">
          <button class="btn btn-primary" style="flex:1" id="pub-now" onclick="publishNow()">&#128640; Publish Now</button>
          <button class="btn btn-purple"  style="flex:1" id="pub-sch" onclick="publishScheduled()">&#128197; Schedule</button>
        </div>
        <div style="font-family:var(--mono);font-size:.62rem;color:var(--muted);text-align:center;margin-top:6px" id="pub-status"></div>
      </div>
    </div>
  </div>
</div>

<script>
// ── CONFIG (injected from worker env) ──────────────────────────
var R2_BASE_URL = '` + r2Base + `';

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
    var r=await fetch('/jobs'); allJobs=await r.json();
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
    var r=await fetch('/topics'); allTopics=await r.json();
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
    var r=await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
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
    var r=await fetch('/generate-topic',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({topic:topic||'Future AI India'})});
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
    var r=await fetch('/kill-incomplete',{method:'POST'});
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
    var r=await fetch('/restore-failed',{method:'POST'});
    var d=await r.json();
    showDebug('debug-home','<span class="dg">Restored '+d.restored+'.</span>');
    setTimeout(function(){loadJobs();loadQueue();},600);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;}
}

async function doTestRender(){
  var btn=document.getElementById('bt'); btn.disabled=true; btn.textContent='Testing...';
  try{
    var r=await fetch('/test-render');
    var d=await r.json();
    showDebug('debug-home','<span class="dk">'+d.url+'</span><br><span class="'+(d.ok?'dg':'dr')+'">'+d.status+'</span> - '+(d.response||d.error||'-'));
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
  finally{btn.disabled=false;btn.innerHTML='&#9741; Test Render';}
}

async function doSyncAnalytics(){
  try{await fetch('/sync-analytics',{method:'POST'});showDebug('debug-home','<span class="dg">Sync started.</span>');setTimeout(loadAnalytics,8000);}
  catch(e){alert(e.message);}
}

// ── VOICE MODE TOGGLE ─────────────────────────────────────────
async function loadVoiceMode(){
  try{
    var r=await fetch('/voice-mode'); var d=await r.json();
    currentVoiceMode=d.voice_mode||'ai'; setVoiceModeUI(currentVoiceMode);
  }catch(e){}
}
function setVoiceModeUI(mode){
  var h=mode==='human';
  var tog=document.getElementById('vm-tog'); var knb=document.getElementById('vm-knob'); var lbl=document.getElementById('vm-lbl');
  if(tog)tog.style.background=h?'var(--green)':'var(--accent)';
  if(knb)knb.style.transform=h?'translateX(16px)':'translateX(0)';
  if(lbl){lbl.textContent=h?'\uD83C\uDFA4 HUMAN VOICE':'\uD83E\uDD16 AI VOICE';lbl.style.color=h?'var(--green)':'var(--accent)';}
  var stgCnt=document.getElementById('stg-cnt'); if(stgCnt)stgCnt.style.display=h?'':'none';
  var banner=document.getElementById('staging-banner');
  if(banner){banner.style.display=h?'none':'block';banner.innerHTML='<span class="dk">\uD83E\uDD16 AI Voice Mode - pipeline auto-completes. Switch to Human Voice to use staging.</span>';}
}
async function toggleVoiceMode(){
  var newMode=currentVoiceMode==='ai'?'human':'ai';
  try{
    var r=await fetch('/voice-mode',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({voice_mode:newMode})});
    var d=await r.json(); currentVoiceMode=d.voice_mode; setVoiceModeUI(currentVoiceMode);
    showDebug('debug-home',currentVoiceMode==='human'?'<span class="dg">\uD83C\uDFA4 Human Voice Mode ON</span>':'<span class="dk">\uD83E\uDD16 AI Voice Mode</span>');
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
}

// ── PUBLISH TOGGLE ────────────────────────────────────────────
async function loadPublishState(){
  try{
    var r=await fetch('/publish-state'); var d=await r.json(); setPublishUI(d.publish===true);
  }catch(e){}
}
function setPublishUI(on){
  var tog=document.getElementById('pub-tog'); var knb=document.getElementById('pub-knob'); var lbl=document.getElementById('pub-lbl');
  if(tog)tog.style.background=on?'var(--green)':'var(--red)';
  if(knb)knb.style.transform=on?'translateX(16px)':'translateX(0)';
  if(lbl){lbl.textContent=on?'PUBLISH ON':'PUBLISH OFF';lbl.style.color=on?'var(--green)':'var(--red)';}
}
async function togglePublish(){
  var knb=document.getElementById('pub-knob');
  var isOn=knb.style.transform==='translateX(16px)'; var newState=!isOn; setPublishUI(newState);
  try{await fetch('/publish-state',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({publish:newState})});}
  catch(e){setPublishUI(isOn);alert('Failed: '+e.message);}
}

// ── SCHEDULE ──────────────────────────────────────────────────
async function loadSchedule(){
  try{
    var r=await fetch('/get-schedule'); var d=await r.json(); var vpd=d.videos_per_day||1;
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
    var r=await fetch('/set-schedule',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({videos_per_day:n})});
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
    var r=await fetch('/replenish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({categories:cats,target:target})});
    var d=await r.json();
    showDebug('debug-home','<span class="dg">Replenish triggered.</span> '+JSON.stringify(d).slice(0,80));
    setTimeout(loadQueue,5000);
  }catch(e){showDebug('debug-home','<span class="dr">'+e.message+'</span>');}
}

// ── STAGING ───────────────────────────────────────────────────
var allStaged=[];
async function loadStaging(){
  try{
    var r=await fetch('/staging'); allStaged=await r.json();
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
  var vid=document.getElementById('stu-vid'); if(studioJob.video_r2_url)vid.src=studioJob.video_r2_url;
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
    var r=await fetch('/music-library'); var d=await r.json();
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
  var vid=document.getElementById('stu-vid'); if(vid){vid.currentTime=0;vid.play();}
  if(recordedBlob){if(playbackAudio){playbackAudio.pause();playbackAudio=null;} playbackAudio=new Audio(URL.createObjectURL(recordedBlob)); playbackAudio.play();}
}

// ── PUBLISH ───────────────────────────────────────────────────
async function doPublish(publishAt){
  if(!studioJob){alert('No job open');return;}
  if(!recordedBlob){alert('Please record your voice first');return;}
  var sEl=document.getElementById('pub-status');
  var n=document.getElementById('pub-now'); var s=document.getElementById('pub-sch');
  n.disabled=s.disabled=true; sEl.textContent='\u23F3 Uploading voice...'; sEl.style.color='var(--yellow)';
  try{
    var ur=await fetch('/upload-voice?job_id='+studioJob.id,{method:'POST',body:recordedBlob,headers:{'Content-Type':'audio/webm'}});
    if(!ur.ok)throw new Error('Upload failed: '+ur.status);
    sEl.textContent='\u23F3 Starting mix...';
    var mr=await fetch('/mix',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
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
  try{var r=await fetch('/calendar');calEvents=await r.json();if(currentPage==='calendar')renderCalendar();}catch(e){}
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
  try{var r=await fetch('/analytics');var d=await r.json();allAnalytics=d.analytics||[];analyticsJobs=d.jobs||[];if(currentPage==='analytics')renderAnalytics();}catch(e){}
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
    var r=await fetch('/review'); var data=await r.json();
    allReview=Array.isArray(data)?data:[];
    var rc=document.getElementById('rev-cnt'); if(rc)rc.textContent=allReview.length;
    var rc2=document.getElementById('cbdp-count'); if(rc2)rc2.textContent=allReview.length;
    renderReviewGrid();
  }catch(e){console.error('loadCBDP:',e);}
}

function renderReviewGrid(){
  var el=document.getElementById('cbdp-grid'); if(!el)return;
  if(!allReview||!allReview.length){
    el.innerHTML='<div class="empty" style="grid-column:1/-1"><span class="empty-icon">\uD83C\uDFAC</span>'
      +'No videos awaiting review.<br>'
      +'<span style="color:var(--muted)">Switch PUBLISH OFF to queue rendered videos here for review before publishing.</span>'
      +'</div>';
    return;
  }
  try{
    el.innerHTML=allReview.map(function(j){
      var cat=CATS[j.cluster]||{color:'var(--muted)',emoji:'\uD83D\uDCF9',label:j.cluster||'?'};
      var scr=(j.script_package&&j.script_package.text)||'';
      var title=(j.script_package&&j.script_package.title)||j.topic||'Untitled';
      var age=j.updated_at?ago(j.updated_at)+' ago':'';
      var videoUrl=j.video_public_url||(R2_BASE_URL&&j.video_r2_url?R2_BASE_URL+'/'+j.video_r2_url:'');
      return '<div class="staged-card" style="cursor:default">'
        +'<div class="staged-head">'
        +'<div class="staged-topic">'+(title||'Untitled')+'</div>'
        +'<div class="staged-meta">'
        +'<span style="font-size:.68rem;color:'+cat.color+'">'+cat.emoji+' '+cat.label+'</span>'
        +'<span class="score-pill '+scClass(j.council_score||0)+'">'+(j.council_score||0)+'</span>'
        +'<span style="font-family:var(--mono);font-size:.58rem;color:var(--muted)">'+age+'</span>'
        +'</div></div>'
        +(videoUrl
          ?'<video src="'+videoUrl+'" controls preload="metadata" '
           +'style="width:100%;max-height:220px;background:#000;display:block"></video>'
           +'<div style="text-align:center;padding:5px 0;border-bottom:1px solid var(--border)">'
           +'<a href="'+videoUrl+'" target="_blank" '
           +'style="font-family:var(--mono);font-size:.62rem;color:var(--accent);text-decoration:none">'
           +'\u25B6 Watch in new tab</a></div>'
          :'<div style="background:var(--surface2);height:80px;display:flex;align-items:center;'
           +'justify-content:center;font-size:.72rem;color:var(--muted);flex-direction:column;gap:4px">'
           +'\uD83D\uDCF9 Video not available'
           +(R2_BASE_URL?'':'<br><span style="font-size:.6rem">R2_BASE_URL not set in Cloudflare</span>')
           +'</div>')
        +'<div class="staged-body" style="font-size:.72rem;line-height:1.6">'+scr.slice(0,150)+(scr.length>150?'\u2026':'')+'</div>'
        +'<div class="staged-foot" style="gap:6px">'
        +'<button class="btn btn-primary" style="flex:1;font-size:.72rem" data-jid="'+j.id+'" onclick="publishCBDP(this.dataset.jid,this)">\uD83D\uDE80 Publish</button>'
        +'<button class="btn btn-red"     style="flex:1;font-size:.72rem" data-jid="'+j.id+'" onclick="rejectCBDP(this.dataset.jid,this)">\u2715 Reject</button>'
        +'</div></div>';
    }).join('');
  }catch(err){
    console.error('renderReviewGrid error:',err);
    el.innerHTML='<div class="empty" style="grid-column:1/-1"><span class="empty-icon">\u26A0</span>Render error: '+err.message+'</div>';
  }
}

async function publishCBDP(jobId,btn){
  if(!confirm('Publish this video to YouTube now?'))return;
  btn.disabled=true; btn.textContent='\u23F3 Publishing...';
  try{
    var r=await fetch('/publish-job',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({job_id:jobId})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    btn.textContent='\u2713 Sent!'; btn.style.background='var(--green)';
    allReview=allReview.filter(function(j){return j.id!==jobId;});
    var rc=document.getElementById('rev-cnt'); if(rc)rc.textContent=allReview.length;
    setTimeout(function(){renderReviewGrid();loadJobs();},1500);
  }catch(e){btn.textContent='\uD83D\uDE80 Publish';btn.disabled=false;alert('Publish failed: '+e.message);}
}

async function rejectCBDP(jobId,btn){
  if(!confirm('Reject this video? The topic will return to queue for reuse.'))return;
  btn.disabled=true; btn.textContent='\u23F3...';
  try{
    var r=await fetch('/reject-job',{method:'POST',
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
    var r=await fetch('/run-topic',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({topic_id:topicId})});
    var d=await r.json(); if(d.error)throw new Error(d.error);
    btn.textContent='\u2713 Job created!'; btn.style.color='var(--green)';
    showDebug('debug-home','<span class="dg">Video job created from topic: '+d.topic+'</span>');
    setTimeout(function(){loadJobs();loadQueue();renderTopicsPage();},800);
  }catch(e){btn.textContent='\u25B6 Generate Now';btn.disabled=false;alert('Failed: '+e.message);}
}

// ── INIT ─────────────────────────────────────────────────────
buildCatStrips();
function loadAll(){loadJobs();loadQueue();loadAnalytics();loadPublishState();loadSchedule();loadVoiceMode();loadStaging();loadCBDP();loadCalendar();}
loadAll();
setInterval(function(){loadJobs();loadQueue();loadStaging();loadCBDP();if(currentPage==='analytics')loadAnalytics();if(currentPage==='calendar')renderCalendar();},6000);
</script>
</body>
</html>`;
}