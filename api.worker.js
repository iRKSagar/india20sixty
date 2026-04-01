// ============================================================
// India20Sixty — Cloudflare Worker V2
// ============================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // ── CORS preflight ────────────────────────────────────────
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin":  "*",
          "Access-Control-Allow-Headers": "Content-Type,Authorization",
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS,PATCH,DELETE",
          "Access-Control-Max-Age":       "86400",
        }
      });
    }

    // ── CONFIG ────────────────────────────────────────────────
    if (url.pathname === "/config") {
      try {
        const rows = await sbGet(env, "system_state?id=eq.main&select=mode,voice_mode,publish");
        const state = rows[0] || {};
        return cors({
          r2_base_url: (env.R2_BASE_URL || "").replace(/\/$/, ""),
          mode:        state.mode || "auto",
          version:     "v3.0"
        });
      } catch(e) {
        return cors({
          r2_base_url: (env.R2_BASE_URL || "").replace(/\/$/, ""),
          mode: "auto",
          version: "v3.0"
        });
      }
    }

    // ── SET MODE ──────────────────────────────────────────────
    if (url.pathname === "/set-mode" && request.method === "POST") {
      try {
        const { mode } = await request.json();
        const validMode = ["auto", "stage"].includes(mode) ? mode : "auto";
        // In auto mode: publish=true, voice_mode=ai
        // In stage mode: publish=false (goes to review), voice_mode preserved
        const updates = { mode: validMode, updated_at: new Date().toISOString() };
        if (validMode === "auto") {
          updates.publish   = true;
          updates.voice_mode = "ai";
        } else {
          updates.publish = false;
        }
        await upsertState(env, updates);
        return cors({ mode: validMode });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── HEALTH ────────────────────────────────────────────────
    if (url.pathname === "/health") {
      return cors({ status: "ok", version: "v3.0",
        time: new Date().toISOString() });
    }

        // ── DASHBOARD ─────────────────────────────────────────
    if (url.pathname === "/" || url.pathname === "/dashboard") {
      const dashUrl = (env.DASHBOARD_URL || "").trim();
      if (!dashUrl) return new Response("DASHBOARD_URL not set", { status: 500 });
      return Response.redirect(dashUrl, 302);
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

    // ── REVIEW QUEUE ──────────────────────────────────────────
    // Pulls ALL jobs that are done but not on YouTube:
    // 1. status=review  — PUBLISH OFF jobs, video saved to R2
    // 2. status=cbdp    — old: upload failed after render (legacy)
    // 3. status=failed  — upload failed (YouTube API error) but video exists
    if (url.pathname === "/review") {
      try {
        const r2Base = (env.R2_BASE_URL || "").replace(/\/$/, "");
        const fields = "id,topic,cluster,script_package,video_r2_url,council_score,updated_at,status,error";

        // Fetch all unfinished jobs that have videos
        const [reviewRows, cbdpRows, stagedRows] = await Promise.all([
          sbGet(env, `jobs?status=eq.review&order=updated_at.desc&select=${fields}`),
          sbGet(env, `jobs?status=eq.cbdp&order=updated_at.desc&select=${fields}`),
          sbGet(env, `jobs?status=eq.staged&order=updated_at.desc&select=${fields}`),
        ]);

        // Fetch failed jobs that have a video_r2_url
        const failedRows = await sbGet(env,
          `jobs?status=eq.failed&video_r2_url=not.is.null&order=updated_at.desc&limit=20&select=${fields}`
        ).catch(() => []);

        // Merge and deduplicate by id
        const seen = new Set();
        const all  = [...stagedRows, ...reviewRows, ...cbdpRows, ...failedRows].filter(j => {
          if (seen.has(j.id)) return false;
          seen.add(j.id);
          return true;
        });

        // Enrich with full public URL
        const enriched = all.map(j => {
          const raw = j.video_r2_url || "";
          // If already a full URL use as-is, otherwise prepend r2Base
          const videoUrl = raw.startsWith("http")
            ? raw
            : (raw && r2Base ? r2Base + "/" + raw : null);
          const hasVideo = !!videoUrl;
          const reason = j.status === "staged"
            ? "Silent video — needs AI voice to publish"
            : j.status === "review"
              ? "Publish was OFF when rendered"
              : j.status === "cbdp"
                ? "Upload failed — video ready"
                : "Render complete — not published";
          return {
            ...j,
            video_public_url: videoUrl,
            review_reason:    reason,
            has_video:        hasVideo,
          };
        });

        return cors(enriched);
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── ADD VOICE TO STAGED VIDEO + PUBLISH ──────────────────
    if (url.pathname === "/add-voice-and-publish" && request.method === "POST") {
      try {
        const { job_id } = await request.json();
        if (!job_id) return cors({ error: "Missing job_id" }, 400);
        const baseUrl = (env.RENDER_PIPELINE_URL || "").replace(/\/[^/]+$/, "");
        const endpoint = baseUrl + "/add-voice-and-publish";
        await sbPatch(env, `jobs?id=eq.${job_id}`,
          { status: "voice", error: null, updated_at: new Date().toISOString() });
        ctx.waitUntil(fetch(endpoint, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ job_id }),
          signal: AbortSignal.timeout(60000)
        }).catch(e => console.error("add-voice-and-publish:", e.message)));
        return cors({ status: "started", job_id });
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
        const raw      = job.video_r2_url || "";
        const videoUrl = raw.startsWith("http") ? raw : `${r2Base}/${raw}`;
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

    // ── UPLOAD IMAGE TO LIBRARY ───────────────────────────────
    if (url.pathname === "/upload-image" && request.method === "POST") {
      try {
        if (!env.R2) return cors({ error: "R2 not bound" }, 500);
        const r2Base   = (env.R2_BASE_URL || "").replace(/\/$/, "");
        const topic    = url.searchParams.get("topic") || "uploaded";
        const filename = url.searchParams.get("filename") || ("img_" + Date.now() + ".png");
        const topicSlug = topic.toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 40);
        const key      = `images/${topicSlug}/${filename}`;
        const blob     = await request.arrayBuffer();
        await env.R2.put(key, blob, {
          httpMetadata: { contentType: request.headers.get("content-type") || "image/png" }
        });
        const publicUrl = r2Base + "/" + key;
        // Record in image_cache
        await sbInsert(env, "image_cache", {
          topic:      topic,
          r2_key:     key,
          public_url: publicUrl,
          scene_idx:  0,
          created_at: new Date().toISOString()
        }).catch(() => {});
        return cors({ status: "uploaded", key, url: publicUrl });
      } catch (e) { return cors({ error: e.message }, 500); }
    }

    // ── IMAGE LIBRARY ─────────────────────────────────────────
    // List all images saved to R2 from past pipeline runs
    if (url.pathname === "/image-library") {
      try {
        // R2 binding list — works via Cloudflare Worker R2 binding
        if (!env.R2) return cors({ error: "R2 not bound", images: [] });
        const r2Base  = (env.R2_BASE_URL || "").replace(/\/$/, "");
        const listed  = await env.R2.list({ prefix: "images/", limit: 500 });
        const images  = (listed.objects || [])
          .filter(o => o.key.match(/\.(png|jpg|jpeg)$/i))
          .sort((a, b) => new Date(b.uploaded) - new Date(a.uploaded))
          .map(o => ({
            key:        o.key,
            url:        r2Base + "/" + o.key,
            size:       o.size,
            uploaded:   o.uploaded,
            // Parse topic from key: images/{topic-slug}/{job_id}_{idx}.png
            topic:      o.key.split("/")[1]?.replace(/-/g, " ") || "unknown",
            scene_idx:  parseInt((o.key.split("_").pop() || "0").replace(/\D/g, "")) || 0,
          }));
        return cors({ images, total: images.length, r2_base: r2Base });
      } catch (e) {
        // Fallback: query Supabase image_cache table
        try {
          const r2Base = (env.R2_BASE_URL || "").replace(/\/$/, "");
          const rows   = await sbGet(env,
            "image_cache?order=created_at.desc&limit=200&select=*");
          const images = rows.map(r => ({
            key:       r.r2_key,
            url:       r.public_url || (r2Base + "/" + r.r2_key),
            topic:     r.topic,
            scene_idx: r.scene_idx || 0,
            uploaded:  r.created_at,
            job_id:    r.job_id,
          }));
          return cors({ images, total: images.length, source: "supabase" });
        } catch (e2) {
          return cors({ error: e2.message, images: [] });
        }
      }
    }

    // ── CREATE VIDEO WITH LIBRARY IMAGES ─────────────────────
    if (url.pathname === "/run-with-images" && request.method === "POST") {
      try {
        const body       = await request.json().catch(() => ({}));
        const image_urls = body.image_urls; // array of 3 public R2 URLs
        const category   = body.category || null;
        if (!image_urls || image_urls.length < 3)
          return cors({ error: "Need exactly 3 image URLs" }, 400);
        const t   = await pickTopic(env, category);
        const job = await createJob(t, env);
        // Pass image_urls to Modal trigger
        ctx.waitUntil(triggerRender(job, env, image_urls));
        return cors({ status: "job_created", job_id: job.id,
                      topic: t.topic, images: "library" });
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
        const r2Base  = (env.R2_BASE_URL || "").replace(/\/$/, "");
        const staged  = await sbGet(env,
          "jobs?status=eq.staged&order=created_at.asc" +
          "&select=id,topic,cluster,script_package,video_r2_url,created_at,council_score");
        const enriched = staged.map(j => {
          const raw = j.video_r2_url || "";
          const videoUrl = raw.startsWith("http")
            ? raw
            : (raw && r2Base ? r2Base + "/" + raw : null);
          return { ...j, video_public_url: videoUrl };
        });
        return cors(enriched);
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
        const toUrl    = (v) => !v ? null : v.startsWith("http") ? v : r2Base + "/" + v;
        await sbPatch(env, "jobs?id=eq." + job_id,
          { status: "mixing", updated_at: new Date().toISOString() });
        const r = await fetch(mixerUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            job_id,
            video_url:       toUrl(job.video_r2_url),
            voice_url:       toUrl(job.voice_r2_url),
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

async function triggerRender(job, env, image_urls) {
  if (!env.RENDER_PIPELINE_URL) {
    await sbPatch(env, "jobs?id=eq." + job.id,
      { status: "failed", error: "RENDER_PIPELINE_URL not set",
        updated_at: new Date().toISOString() });
    return;
  }
  const renderUrl = env.RENDER_PIPELINE_URL.trim().replace(/\/$/, "");
  try {
    const body = {
      job_id: job.id, topic: job.topic,
      script_package: job.script_package,
      webhook_url: (env.WORKER_URL || "").trim().replace(/\/$/, "") + "/webhook"
    };
    if (image_urls && image_urls.length >= 3) {
      body.image_urls = image_urls;
    }
    const r = await fetch(renderUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
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

// buildDashboard removed — dashboard is now static files