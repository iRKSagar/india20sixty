// ============================================================
// India20Sixty — Cloudflare Worker v5.0
// Priority A: Space/ISRO bias in pickTopic()
// New: /longform/* routes for long-form video pipeline
// New: /settings priority_a toggle
// New: last_cluster tracking to prevent cluster repetition
// ============================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS,PATCH,DELETE",
        "Access-Control-Max-Age": "86400",
      }});
    }

    if (url.pathname === "/config") {
      try {
        const rows = await sbGet(env, "system_state?id=eq.main&select=mode,voice_mode,publish");
        const s = rows[0] || {};
        return cors({ r2_base_url: (env.R2_BASE_URL||"").replace(/\/$/,""), mode: s.mode||"auto", version:"v5.0" });
      } catch(e) { return cors({ r2_base_url:(env.R2_BASE_URL||"").replace(/\/$/,""), mode:"auto", version:"v5.0" }); }
    }

    if (url.pathname === "/health") return cors({ status:"ok", version:"v5.0", time:new Date().toISOString() });

    if (url.pathname === "/" || url.pathname === "/dashboard") {
      const dashUrl = (env.DASHBOARD_URL||"").trim();
      if (!dashUrl) return new Response("DASHBOARD_URL not set",{status:500});
      return Response.redirect(dashUrl, 302);
    }

    if (url.pathname === "/jobs") {
      try { return cors(await sbGet(env,"jobs?order=created_at.desc&limit=50")); }
      catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/topics") {
      try { return cors(await sbGet(env,"topics?order=council_score.desc&limit=100")); }
      catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/analytics") {
      try {
        const jobs = await sbGet(env,"jobs?status=eq.complete&order=created_at.desc&limit=100&select=id,topic,council_score,youtube_id,cluster,created_at");
        const analyticsRows = await sbGet(env,"analytics?order=created_at.desc&limit=100").catch(()=>[]);
        const analyticsMap = {};
        for (const a of analyticsRows) analyticsMap[a.video_id] = a;
        const merged = jobs.map(j => ({
          video_id:j.id, youtube_id:j.youtube_id||null, topic:j.topic||"",
          cluster:j.cluster||"AI", council_score:j.council_score||0,
          youtube_views:(analyticsMap[j.id]||{}).youtube_views||0,
          youtube_likes:(analyticsMap[j.id]||{}).youtube_likes||0,
          comment_count:(analyticsMap[j.id]||{}).comment_count||0,
          score:(analyticsMap[j.id]||{}).score||0, created_at:j.created_at,
        }));
        return cors({analytics:merged, jobs});
      } catch(e) { return cors({error:e.message,analytics:[],jobs:[]},500); }
    }

    if (url.pathname === "/run" && request.method === "POST") {
      try {
        const body = await request.json().catch(()=>({}));
        const t = await pickTopic(env, body.category||null);
        const job = await createJob(t, env);
        await upsertState(env, { last_cluster: t.category });
        ctx.waitUntil(triggerRender(job, env));
        return cors({ status:"job_created", job_id:job.id, topic:t.topic, category:t.category });
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/run-topic" && request.method === "POST") {
      try {
        const body = await request.json().catch(()=>({}));
        if (!body.topic_id) return cors({error:"Missing topic_id"},400);
        const topics = await sbGet(env,"topics?id=eq."+body.topic_id+"&select=*");
        if (!topics.length) return cors({error:"Topic not found"},404);
        const t = topics[0];
        if (t.used) return cors({error:"Topic already used"},400);
        await sbPatch(env,"topics?id=eq."+body.topic_id,{used:true,used_at:new Date().toISOString()});
        const job = await sbInsert(env,"jobs",{
          topic:t.topic, cluster:t.cluster||"AI", status:"pending",
          script_package:t.script_package||null, council_score:t.council_score||0, retries:0,
          created_at:new Date().toISOString(), updated_at:new Date().toISOString()
        });
        ctx.waitUntil(triggerRender(job, env));
        return cors({status:"job_created",job_id:job.id,topic:t.topic,category:t.cluster});
      } catch(e) { return cors({error:e.message},500); }
    }
    if (url.pathname === "/settings" && request.method === "GET") {
      try {
        const rows = await sbGet(env,"system_state?id=eq.main&select=mode,voice_mode,publish,videos_per_day,subscribe_cta,long_form,cross_post,priority_a,image_engine,voice_engine");
        const s = rows[0]||{};
        return cors({
          mode:           s.mode||"auto",
          voice_mode:     s.voice_mode||"ai",
          publish:        s.publish===true,
          videos_per_day: s.videos_per_day||1,
          subscribe_cta:  s.subscribe_cta===true,
          long_form:      s.long_form===true,
          cross_post:     s.cross_post===true,
          priority_a:     s.priority_a===true,
          image_engine:   s.image_engine||"inbuilt",
          voice_engine:   s.voice_engine||"inbuilt",
        });
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/settings" && request.method === "POST") {
      try {
        const body = await request.json();
        const allowed = ["mode","voice_mode","publish","videos_per_day","subscribe_cta","long_form","cross_post","priority_a","image_engine","voice_engine"];
        const updates = {};
        for (const k of allowed) { if (k in body) updates[k]=body[k]; }
        if (!Object.keys(updates).length) return cors({error:"No valid settings"},400);
        if (updates.mode==="auto") { updates.publish=true; updates.voice_mode="ai"; }
        await upsertState(env, updates);
        return cors({status:"updated", updated:Object.keys(updates)});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/review") {
      try {
        const r2Base = (env.R2_BASE_URL||"").replace(/\/$/,"");
        const fields = "id,topic,cluster,script_package,video_r2_url,council_score,updated_at,created_at,status,error,youtube_id";
        const [rev,cbdp,stagedHuman,stagedFailed] = await Promise.all([
          sbGet(env,"jobs?status=eq.review&order=updated_at.desc&select="+fields),
          sbGet(env,"jobs?status=eq.cbdp&order=updated_at.desc&select="+fields),
          // Human voice mode staged (silent, needs voice)
          sbGet(env,"jobs?status=eq.staged&error=is.null&order=updated_at.desc&select="+fields),
          // Publish-failed staged (has video, publish errored)
          sbGet(env,"jobs?status=eq.staged&error=not.is.null&order=updated_at.desc&select="+fields),
        ]);
        const seen = new Set();
        const all = [...stagedHuman,...stagedFailed,...rev,...cbdp].filter(j=>{ if(seen.has(j.id))return false; seen.add(j.id); return true; });
        return cors(all.map(j=>{
          const raw=j.video_r2_url||"";
          const videoUrl=raw.startsWith("http")?raw:(raw&&r2Base?r2Base+"/"+raw:null);
          let reason="";
          const isSilent = j.status==="staged" && (!j.error || !j.error.includes("publish_failed"));
          if (isSilent) reason="Silent video — needs AI voice or manual recording";
          else if (j.status==="staged" && j.error) reason="Publish failed — "+j.error.replace("publish_failed: ","").slice(0,80);
          else if (j.status==="review") reason="Publish was OFF — ready to publish";
          else if (j.status==="cbdp") reason="Upload failed — ready to retry";
          else reason="Not published";
          return {...j, video_public_url:videoUrl, review_reason:reason, has_video:!!videoUrl,
                  is_silent:isSilent};
        }));
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/add-voice-and-publish" && request.method === "POST") {
      try {
        const {job_id} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const triggerUrl = (env.RENDER_PIPELINE_URL||"").trim().replace(/\/$/,"");
        await sbPatch(env,"jobs?id=eq."+job_id,{status:"voice",error:null,updated_at:new Date().toISOString()});
        ctx.waitUntil(fetch(triggerUrl,{
          method:"POST",headers:{"content-type":"application/json"},
          body:JSON.stringify({action:"add-voice-and-publish",job_id}),
          signal:AbortSignal.timeout(60000)
        }).catch(e=>console.error("add-voice:",e.message)));
        return cors({status:"started",job_id});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/publish-job" && request.method === "POST") {
      try {
        const {job_id} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const jobs = await sbGet(env,"jobs?id=eq."+job_id+"&select=id,topic,video_r2_url,script_package,cluster");
        if (!jobs.length) return cors({error:"Job not found"},404);
        const job = jobs[0];
        if (!job.video_r2_url) return cors({error:"No video file"},400);
        const mixerUrl = env.MIXER_URL||"";
        if (!mixerUrl) return cors({error:"MIXER_URL not configured"},500);
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const raw=job.video_r2_url||"";
        const videoUrl=raw.startsWith("http")?raw:r2Base+"/"+raw;
        const title=(job.script_package&&job.script_package.title)||job.topic;
        await sbPatch(env,"jobs?id=eq."+job_id,{status:"upload",updated_at:new Date().toISOString()});
        const r=await fetch(mixerUrl,{method:"POST",headers:{"content-type":"application/json"},
          body:JSON.stringify({job_id,video_url:videoUrl,voice_url:null,music_track:null,publish_at:null,upload_only:true,title})});
        if (!r.ok) throw new Error("Mixer returned "+r.status);
        return cors({status:"publishing",job_id});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/reject-job" && request.method === "POST") {
      try {
        const {job_id} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const jobs = await sbGet(env,"jobs?id=eq."+job_id+"&select=id,topic,cluster,council_score,script_package");
        if (!jobs.length) return cors({error:"Job not found"},404);
        const job=jobs[0];
        await sbPatch(env,"jobs?id=eq."+job_id,{status:"failed",error:"Rejected in review",updated_at:new Date().toISOString()});
        if (job.topic) {
          try {
            const ex=await sbGet(env,"topics?topic=eq."+encodeURIComponent(job.topic)+"&select=id,used");
            if (ex.length>0) { await sbPatch(env,"topics?id=eq."+ex[0].id,{used:false,used_at:null}); }
            else { await sbInsert(env,"topics",{topic:job.topic,cluster:job.cluster||"AI",council_score:job.council_score||75,script_package:job.script_package||null,used:false,source:"restored_from_review",created_at:new Date().toISOString()}); }
          } catch(e) {}
        }
        return cors({status:"rejected",job_id,topic_restored:true});
      } catch(e) { return cors({error:e.message},500); }
    }
    if (url.pathname === "/image-library/backfill" && request.method === "POST") {
      try {
        if (!env.VIDEOS_BUCKET || typeof env.VIDEOS_BUCKET.list !== "function")
          return cors({error:"R2 bucket binding not configured — add it in Cloudflare Worker Bindings"},500);
        const r2Base = (env.R2_BASE_URL||"").replace(/\/$/,"");
        const listed = await env.VIDEOS_BUCKET.list({prefix:"images/", limit:1000});
        const objects = (listed.objects||[]).filter(o=>o.key.match(/\.(png|jpg|jpeg)$/i));
        let inserted=0, skipped=0, errors=0;
        for (const obj of objects) {
          try {
            // Check if already in image_cache
            const existing = await sbGet(env,"image_cache?r2_key=eq."+encodeURIComponent(obj.key)+"&select=id");
            if (existing.length > 0) { skipped++; continue; }
            // Parse key: images/{cluster}/{job_id}_{scene_idx}_{timestamp}.png
            const parts = obj.key.split("/");
            const cluster = parts[1] || "AI";
            const filename = parts[2] || "";
            const segs = filename.replace(".png","").split("_");
            // job_id is UUID (5 parts with hyphens) then scene_idx then timestamp
            // filename format: {job_id}_{scene_idx}_{timestamp}.png
            const scene_idx = parseInt(segs[segs.length-2]) || 0;
            const job_id = segs.slice(0, segs.length-2).join("_");
            const public_url = r2Base ? r2Base+"/"+obj.key : "";
            await sbInsert(env,"image_cache",{
              r2_key:     obj.key,
              public_url: public_url,
              cluster:    cluster,
              engine:     "FLUX",
              job_type:   "shorts",
              scene_idx:  scene_idx,
              job_id:     job_id,
              created_at: obj.uploaded ? new Date(obj.uploaded).toISOString() : new Date().toISOString(),
            });
            inserted++;
          } catch(e) { errors++; console.error("backfill row error:", obj.key, e.message); }
        }
        return cors({total:objects.length, inserted, skipped, errors});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/image-library") {
      try {
        const cluster = url.searchParams.get("cluster") || "";
        const jobType = url.searchParams.get("job_type") || "";
        const r2Base  = (env.R2_BASE_URL||"").replace(/\/$/,"");
        let images = [];

        // Always scan R2 — it's the source of truth. Paginate to get ALL images.
        if (env.VIDEOS_BUCKET && typeof env.VIDEOS_BUCKET.list === "function") {
          try {
            const prefix = cluster ? `images/${cluster}/` : "images/";
            let allObjects = [];
            let cursor = undefined;
            // Paginate through all R2 objects
            do {
              const listed = await env.VIDEOS_BUCKET.list({prefix, limit:1000, cursor});
              allObjects = allObjects.concat(listed.objects||[]);
              cursor = listed.truncated ? listed.cursor : undefined;
            } while (cursor);

            const r2Images = allObjects
              .filter(o => o.key.match(/\.(png|jpg|jpeg)$/i))
              .sort((a,b) => new Date(b.uploaded) - new Date(a.uploaded))
              .map(o => {
                const parts = o.key.split("/");
                const clust = parts[1] || "AI";
                return {
                  key:      o.key,
                  url:      r2Base + "/" + o.key,
                  cluster:  clust,
                  topic:    "India Tech",
                  engine:   "FLUX",
                  job_type: "shorts",
                  scene_idx: 0,
                  uploaded: o.uploaded,
                };
              });

            // Enrich with topic/cluster from image_cache where available
            try {
              const cacheRows = await sbGet(env,
                "image_cache?select=r2_key,topic,cluster,engine,job_type&limit=2000"
              );
              const cacheMap = {};
              for (const r of cacheRows) { if (r.r2_key) cacheMap[r.r2_key] = r; }
              for (const img of r2Images) {
                const cached = cacheMap[img.key];
                if (cached) {
                  if (cached.topic)    img.topic    = cached.topic;
                  if (cached.cluster)  img.cluster  = cached.cluster;
                  if (cached.engine)   img.engine   = cached.engine;
                  if (cached.job_type) img.job_type = cached.job_type;
                }
              }
            } catch(e) { console.error("cache enrich failed:", e.message); }

            images = r2Images;
            console.log("R2 images:", images.length);
          } catch(e) {
            console.error("R2 scan failed:", e.message);
          }
        }

        // Fallback to image_cache only if R2 binding unavailable
        if (!images.length) {
          try {
            let ep = "image_cache?select=id,r2_key,public_url,topic,scene_idx,created_at,cluster,engine,job_type,job_id&order=created_at.desc&limit=1000";
            if (cluster) ep += "&cluster=eq." + cluster;
            if (jobType) ep += "&job_type=eq." + jobType;
            const rows = await sbGet(env, ep);
            images = rows.map(r => ({
              id: r.id, key: r.r2_key,
              url: r.public_url || (r2Base && r.r2_key ? r2Base+"/"+r.r2_key : ""),
              topic: r.topic || "India Tech", cluster: r.cluster || "AI",
              engine: r.engine || "FLUX", job_type: r.job_type || "shorts",
              scene_idx: r.scene_idx || 0, uploaded: r.created_at,
            })).filter(img => img.url);
            console.log("image_cache fallback:", images.length);
          } catch(e) { console.error("image_cache fallback failed:", e.message); }
        }

        // Apply job_type filter if set (R2 scan can't filter, do it here)
        if (jobType && images.length) images = images.filter(i => i.job_type === jobType);

        return cors({ images, total: images.length,
          r2_bound: !!(env.VIDEOS_BUCKET && typeof env.VIDEOS_BUCKET.list === "function") });
      } catch(e) { return cors({error:e.message, images:[]}); }
    }

    if (url.pathname === "/delete-images" && request.method === "POST") {
      try {
        const body = await request.json();
        const keys = body.keys || [];   // R2 keys to delete
        const ids  = body.ids  || [];   // image_cache row ids to delete
        if (!keys.length && !ids.length) return cors({error:"No keys or ids provided"},400);

        let r2Deleted = 0, dbDeleted = 0, errors = [];

        // Delete from R2 if bucket is bound
        if (env.VIDEOS_BUCKET && keys.length) {
          for (const key of keys) {
            try {
              await env.VIDEOS_BUCKET.delete(key);
              r2Deleted++;
            } catch(e) {
              errors.push("R2 " + key + ": " + e.message);
            }
          }
        }

        // Delete from image_cache by id
        if (ids.length) {
          try {
            // Supabase: delete where id in (...)
            for (const id of ids) {
              await sbDelete(env, "image_cache?id=eq." + id);
              dbDeleted++;
            }
          } catch(e) {
            errors.push("DB: " + e.message);
          }
        } else if (keys.length) {
          // Fallback: delete by r2_key if no ids
          for (const key of keys) {
            try {
              await sbDelete(env, "image_cache?r2_key=eq." + encodeURIComponent(key));
            } catch(e) {}
          }
        }

        return cors({deleted: r2Deleted, db_deleted: dbDeleted, errors, total: keys.length});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/run-with-images" && request.method === "POST") {
      try {
        const body=await request.json().catch(()=>({}));
        if (!body.image_urls||body.image_urls.length<3) return cors({error:"Need 3 image URLs"},400);
        const t=await pickTopic(env,body.category||null);
        const job=await createJob(t,env);
        ctx.waitUntil(triggerRender(job,env,body.image_urls));
        return cors({status:"job_created",job_id:job.id,topic:t.topic});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/generate-topic" && request.method === "POST") {
      try {
        const body=await request.json().catch(()=>({}));
        return cors(await callCouncil(env,body.topic||"Future of AI in India","manual",body.category));
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/replenish" && request.method === "POST") {
      const body=await request.json().catch(()=>({}));
      const cats = (body.categories||ALL_CATS).filter(c=>ALL_CATS.includes(c));
      const target = parseInt(body.target)||12;
      if(!env.OPENAI_API_KEY) return cors({error:"OPENAI_API_KEY not set"},500);

      // Run replenish in background — GPT scoring takes 60-90s for 12 topics
      ctx.waitUntil(_runReplenish(env, cats, target));
      return cors({status:"replenishing", message:"Generating "+target+" topics for: "+(cats.join(", "))+". Refresh in ~90s."});
    }
    if (url.pathname === "/kill-incomplete" && request.method === "POST") {
      try {
        const stuck=await sbGet(env,"jobs?status=in.(pending,processing,images,voice,render,upload)&select=id,topic,cluster");
        let restored=0;
        for (const j of stuck) {
          await sbPatch(env,"jobs?id=eq."+j.id,{status:"failed",error:"manually_killed",updated_at:new Date().toISOString()});
          if (j.topic) { try { const ex=await sbGet(env,"topics?topic=eq."+encodeURIComponent(j.topic)+"&select=id,used"); if(ex.length>0&&ex[0].used){await sbPatch(env,"topics?id=eq."+ex[0].id,{used:false,used_at:null});restored++;} } catch(e){} }
        }
        return cors({killed:stuck.length,topics_restored:restored});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/restore-failed" && request.method === "POST") {
      try {
        const failed=await sbGet(env,"jobs?status=eq.failed&select=id,topic,council_score,script_package,cluster");
        let restored=0,already=0;
        for (const j of failed) {
          if (!j.topic) continue;
          try {
            const ex=await sbGet(env,"topics?topic=eq."+encodeURIComponent(j.topic)+"&select=id,used");
            if (ex.length>0) { if(ex[0].used){await sbPatch(env,"topics?id=eq."+ex[0].id,{used:false,used_at:null});restored++;}else{already++;} }
            else { await sbInsert(env,"topics",{cluster:j.cluster||"AI",topic:j.topic,used:false,council_score:j.council_score||75,script_package:j.script_package||null,source:"restored_from_failed",created_at:new Date().toISOString()});restored++; }
          } catch(e) {}
        }
        return cors({restored,already_in_queue:already,total_failed:failed.length});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/test-render") {
      const healthUrl=env.MODAL_HEALTH_URL||"NOT_SET";
      try { const r=await fetch(healthUrl,{signal:AbortSignal.timeout(15000)}); const t=await r.text(); return cors({url:healthUrl,status:r.status,response:t.slice(0,400),ok:r.ok}); }
      catch(e){return cors({url:healthUrl,error:e.message,ok:false});}
    }

    if (url.pathname === "/webhook" && request.method === "POST") {
      try {
        const data=await request.json();
        const {job_id,status,youtube_id,error,script}=data;
        if (!job_id) return cors({error:"Missing job_id"},400);
        const u={status:status||"unknown",updated_at:new Date().toISOString()};
        if (youtube_id) u.youtube_id=youtube_id;
        if (error) u.error=error;
        if (script) u.script_package={text:script};
        await sbPatch(env,"jobs?id=eq."+job_id,u);
        if (status==="complete"&&youtube_id&&youtube_id!=="TEST_MODE") ctx.waitUntil(createAnalyticsRecord(job_id,youtube_id,env));
        return cors({received:true,job_id,status});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/sync-analytics" && request.method === "POST") {
      try {
        await syncYouTubeAnalytics(env);
        const rows = await sbGet(env,"analytics?select=video_id,youtube_views,youtube_likes,score&order=score.desc&limit=5").catch(()=>[]);
        const jobs = await sbGet(env,"jobs?status=eq.complete&youtube_id=not.is.null&select=id,youtube_id&limit=5").catch(()=>[]);
        return cors({status:"synced", analytics_rows: rows.length, jobs_with_youtube: jobs.length, sample_jobs: jobs.slice(0,3), sample_analytics: rows.slice(0,3)});
      } catch(e) {
        console.error("sync-analytics error:", e.message);
        return cors({status:"error", error: e.message});
      }
    }

    if (url.pathname === "/staging") {
      try {
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const staged=await sbGet(env,"jobs?status=eq.staged&order=created_at.asc&select=id,topic,cluster,script_package,video_r2_url,created_at,council_score");
        return cors(staged.map(j=>{ const raw=j.video_r2_url||""; const v=raw.startsWith("http")?raw:(raw&&r2Base?r2Base+"/"+raw:null); return {...j,video_public_url:v}; }));
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/cbdp") {
      try { return cors(await sbGet(env,"jobs?status=eq.cbdp&order=created_at.desc&select=id,topic,cluster,script_package,video_r2_url,created_at,council_score,error")); }
      catch(e){return cors({error:e.message},500);}
    }
    if (url.pathname === "/upload-voice" && request.method === "POST") {
      try {
        const jobId=url.searchParams.get("job_id");
        if (!jobId) return cors({error:"Missing job_id"},400);
        const blob=await request.arrayBuffer();
        const r2Key="voices/"+jobId+"/voice.webm";
        if (env.VIDEOS_BUCKET) await env.VIDEOS_BUCKET.put(r2Key,blob,{httpMetadata:{contentType:"audio/webm"}});
        await sbPatch(env,"jobs?id=eq."+jobId,{voice_r2_url:r2Key,updated_at:new Date().toISOString()});
        return cors({status:"uploaded",r2_key:r2Key,job_id:jobId});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/music-library") {
      return cors({tracks:[
        {id:"epic_01",label:"Epic Rise",category:"Epic",duration:45},
        {id:"hopeful_01",label:"Hopeful Morning",category:"Hopeful",duration:52},
        {id:"tech_01",label:"Digital Pulse",category:"Tech",duration:38},
        {id:"emotional_01",label:"Stirring Moment",category:"Emotional",duration:60},
        {id:"neutral_01",label:"Subtle Background",category:"Neutral",duration:44},
      ]});
    }

    if (url.pathname === "/mix" && request.method === "POST") {
      try {
        const body=await request.json();
        const {job_id,music_track,music_volume,publish_at,voice_offset_ms}=body;
        if (!job_id) return cors({error:"Missing job_id"},400);
        const jobs=await sbGet(env,"jobs?id=eq."+job_id+"&select=id,topic,video_r2_url,voice_r2_url");
        if (!jobs.length) return cors({error:"Job not found"},404);
        const job=jobs[0];
        if (!job.voice_r2_url) return cors({error:"No voice recording"},400);
        const mixerUrl=env.MIXER_URL||"";
        if (!mixerUrl) return cors({error:"MIXER_URL not set"},500);
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const toUrl=v=>!v?null:v.startsWith("http")?v:r2Base+"/"+v;
        await sbPatch(env,"jobs?id=eq."+job_id,{status:"mixing",updated_at:new Date().toISOString()});
        const r=await fetch(mixerUrl,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({job_id,video_url:toUrl(job.video_r2_url),voice_url:toUrl(job.voice_r2_url),music_track:music_track||"neutral_01",music_volume:music_volume||0.08,publish_at:publish_at||null,voice_offset_ms:voice_offset_ms||0})});
        if (!r.ok) throw new Error("Mixer returned "+r.status);
        return cors({status:"mixing_started",job_id});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/kill-job" && request.method === "POST") {
      try {
        const {job_id} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const jobs = await sbGet(env,"jobs?id=eq."+job_id+"&select=id,topic,cluster,status");
        if (!jobs.length) return cors({error:"Job not found"},404);
        const job = jobs[0];
        const killable = ["pending","processing","images","voice","render","upload"];
        if (!killable.includes(job.status))
          return cors({error:"Job status '"+job.status+"' cannot be killed"},400);
        await sbPatch(env,"jobs?id=eq."+job_id,
          {status:"failed",error:"manually_killed",updated_at:new Date().toISOString()});
        let topicRestored = false;
        if (job.topic) {
          try {
            const ex = await sbGet(env,"topics?topic=eq."+encodeURIComponent(job.topic)+"&select=id,used");
            if (ex.length > 0 && ex[0].used) {
              await sbPatch(env,"topics?id=eq."+ex[0].id,{used:false,used_at:null});
              topicRestored = true;
            }
          } catch(e) {}
        }
        return cors({killed:true,job_id,topic_restored:topicRestored});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/kill-topic" && request.method === "POST") {
      try {
        const {topic_id} = await request.json();
        if (!topic_id) return cors({error:"Missing topic_id"},400);
        await sbPatch(env,"topics?id=eq."+topic_id,
          {used:true, source:"killed", updated_at:new Date().toISOString()});
        return cors({killed:true, topic_id});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/logs") {
      try {
        const fields = "id,topic,cluster,status,error,youtube_id,council_score,created_at,updated_at,script_package";
        const [failed,complete] = await Promise.all([
          sbGet(env,"jobs?status=eq.failed&order=updated_at.desc&limit=50&select="+fields),
          sbGet(env,"jobs?status=in.(complete,test_complete)&order=updated_at.desc&limit=50&select="+fields),
        ]);
        return cors({failed,complete});
      } catch(e) { return cors({error:e.message},500); }
    }
    if (url.pathname === "/calendar") {
      try { return cors(await sbGet(env,"jobs?status=in.(staged,mixing,complete)&order=scheduled_at.asc.nullslast,created_at.desc&select=id,topic,cluster,status,youtube_id,scheduled_at,created_at")); }
      catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/service-health") {
      try {
        const results={};
        try { const r=await fetch(env.MODAL_HEALTH_URL||"",{signal:AbortSignal.timeout(8000)}); const d=await r.json().catch(()=>({})); results.modal={ok:r.ok,version:d.version||"?"}; } catch(e){results.modal={ok:false,error:e.message};}
        try { const tcUrl=env.TOPIC_COUNCIL_URL||""; if(tcUrl){const r=await fetch(tcUrl,{signal:AbortSignal.timeout(8000)}); const d=await r.json().catch(()=>({})); results.topic_council={ok:r.ok,queue_depth:d.queue_depth||0,space_ready:d.space_ready||0};} } catch(e){results.topic_council={ok:false,error:e.message};}
        try { const t=await sbGet(env,"topics?used=eq.false&council_score=gte.70&select=cluster"); results.topics_ready=t.length; results.space_ready=t.filter(x=>x.cluster==="Space").length; } catch(e){}
        try { const j=await sbGet(env,"jobs?order=created_at.desc&limit=100&select=status,created_at"); const today=new Date(); today.setHours(0,0,0,0); const tj=j.filter(x=>new Date(x.created_at)>=today); results.jobs_today=tj.length; results.running=j.filter(x=>["pending","processing","images","voice","render","upload"].includes(x.status)).length; results.complete_today=tj.filter(x=>x.status==="complete").length; } catch(e){}
        results.time=new Date().toISOString();
        return cors(results);
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/create-manual-job" && request.method === "POST") {
      try {
        const {topic,script,cluster}=await request.json().catch(()=>({}));
        if (!topic||!script) return cors({error:"Missing topic or script"},400);
        const words=script.trim().split(/\s+/).filter(Boolean);
        if (words.length>70) return cors({error:"Script too long: "+words.length+" words (max 65)"},400);
        const safeCluster=(cluster&&ALL_CATS.includes(cluster))?cluster:"AI";
        const job=await sbInsert(env,"jobs",{topic:topic.trim().slice(0,200),cluster:safeCluster,status:"manual_pending",script_package:{text:script.trim(),source:"manual",word_count:words.length,generated_at:new Date().toISOString()},council_score:75,retries:0,created_at:new Date().toISOString(),updated_at:new Date().toISOString()});
        return cors({status:"created",job_id:job.id,topic:job.topic,word_count:words.length});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/upload-manual-video" && request.method === "POST") {
      try {
        const jobId=url.searchParams.get("job_id");
        if (!jobId) return cors({error:"Missing job_id"},400);
        if (!env.VIDEOS_BUCKET) return cors({error:"R2 not bound"},500);
        const blob=await request.arrayBuffer();
        if (blob.byteLength<10000) return cors({error:"File too small"},400);
        const r2Key="manual/"+jobId+"/video.mp4";
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        await env.VIDEOS_BUCKET.put(r2Key,blob,{httpMetadata:{contentType:"video/mp4"}});
        const publicUrl=r2Base+"/"+r2Key;
        await sbPatch(env,"jobs?id=eq."+jobId,{video_r2_url:publicUrl,status:"staged",updated_at:new Date().toISOString()});
        return cors({status:"uploaded",r2_key:r2Key,url:publicUrl,job_id:jobId,size_kb:Math.round(blob.byteLength/1024)});
      } catch(e){return cors({error:e.message},500);}
    }
    // ══════════════════════════════════════════════════════════
    // LONG-FORM ROUTES
    // ══════════════════════════════════════════════════════════

    if (url.pathname === "/longform/create" && request.method === "POST") {
      try {
        const {topic,cluster,target_duration,auto}=await request.json().catch(()=>({}));
        if (!topic) return cors({error:"Missing topic"},400);
        const safeCluster=(cluster&&ALL_CATS.includes(cluster))?cluster:"Space";
        const durSecs=Math.min(720,Math.max(180,(parseInt(target_duration)||420)));
        const autoMode = auto !== false; // default true — full auto
        const job=await sbInsert(env,"longform_jobs",{
          topic:topic.trim().slice(0,300),cluster:safeCluster,
          status:"scripting",target_duration:durSecs,
          auto_mode:autoMode,
          created_at:new Date().toISOString(),updated_at:new Date().toISOString(),
        });
        const lfUrl=env.LONGFORM_PIPELINE_URL||"";
        if (lfUrl) ctx.waitUntil(fetch(lfUrl,{method:"POST",headers:{"content-type":"application/json"},
          body:JSON.stringify({action:"generate-script",job_id:job.id,topic,cluster:safeCluster,target_duration:durSecs,auto:autoMode})
        }).catch(e=>console.error("lf script:",e.message)));
        return cors({status:"created",job_id:job.id,topic,cluster:safeCluster,target_duration:durSecs,auto:autoMode});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/jobs") {
      try { return cors(await sbGet(env,"longform_jobs?order=created_at.desc&limit=30")); }
      catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/topic-ideas") {
      try {
        const cluster = url.searchParams.get("cluster") || "Space";
        const today = new Date().toLocaleDateString('en-US',{month:'long',year:'numeric'});
        const OPENAI_KEY = env.OPENAI_API_KEY || "";
        if (!OPENAI_KEY) return cors({error:"OPENAI_API_KEY not set",ideas:[]},500);
        const prompt = `You are a YouTube documentary topic researcher for India20Sixty.
Today is ${today}. Generate 6 compelling story angles for a 7-10 minute documentary video about India's ${cluster} sector.

Each topic must:
- Be a specific story, not a generic subject (not "ISRO overview" but "Why ISRO's Gaganyaan is 2 years late and what that means")
- Have a central question that makes a viewer curious
- Be relevant to India in ${today}
- Frame correctly — past events as past, future as genuinely upcoming

Return ONLY valid JSON array:
[
  {"topic": "specific compelling angle 1", "cluster": "${cluster}"},
  {"topic": "specific compelling angle 2", "cluster": "${cluster}"},
  ...6 total
]`;
        const r = await fetch("https://api.openai.com/v1/chat/completions",{
          method:"POST",
          headers:{"Authorization":"Bearer "+OPENAI_KEY,"Content-Type":"application/json"},
          body:JSON.stringify({model:"gpt-4o-mini",messages:[{role:"user",content:prompt}],
            temperature:0.9,max_tokens:600,response_format:{type:"json_object"}})
        });
        if (!r.ok) return cors({error:"OpenAI error",ideas:[]},500);
        const d = await r.json();
        const raw = d.choices[0].message.content;
        const parsed = JSON.parse(raw);
        const ideas = Array.isArray(parsed) ? parsed : (parsed.ideas || parsed.topics || []);
        return cors({ideas, cluster});
      } catch(e) { return cors({error:e.message, ideas:[]}, 500); }
    }

    if (url.pathname === "/longform/kill" && request.method === "POST") {
      try {
        const {job_id} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        await sbPatch(env,"longform_jobs?id=eq."+job_id,
          {status:"failed",error:"manually_killed",updated_at:new Date().toISOString()});
        return cors({killed:true,job_id});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/clear-failed" && request.method === "POST") {
      try {
        // Get failed job ids first
        const failed = await sbGet(env,"longform_jobs?status=eq.failed&select=id");
        let deleted = 0;
        for (const j of failed) {
          // Delete segments first
          await sbDelete(env,"longform_segments?job_id=eq."+j.id).catch(()=>{});
          // Delete job
          await sbDelete(env,"longform_jobs?id=eq."+j.id).catch(()=>{});
          deleted++;
        }
        return cors({deleted});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/retry-script" && request.method === "POST") {
      try {
        const {job_id,topic,cluster,target_duration} = await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const lfUrl = (env.LONGFORM_PIPELINE_URL||"").trim().replace(/\/$/,"");
        if (!lfUrl) return cors({error:"LONGFORM_PIPELINE_URL not set"},500);
        await sbPatch(env,"longform_jobs?id=eq."+job_id,
          {status:"scripting",error:null,updated_at:new Date().toISOString()});
        ctx.waitUntil(fetch(lfUrl,{
          method:"POST",headers:{"content-type":"application/json"},
          body:JSON.stringify({action:"generate-script",job_id,topic,cluster,target_duration:target_duration||420}),
          signal:AbortSignal.timeout(30000)
        }).catch(e=>console.error("retry-script:",e.message)));
        return cors({status:"started",job_id});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/modal-logs" && request.method === "GET") {
      try {
        const job_id = url.searchParams.get("job_id") || "";
        const limit  = parseInt(url.searchParams.get("limit") || "50");
        let ep = "render_logs?order=created_at.desc&limit="+limit+"&select=job_id,message,created_at";
        if (job_id) ep += "&job_id=eq."+job_id;
        const logs = await sbGet(env,ep);
        return cors({logs: logs.reverse()});
      } catch(e){return cors({error:e.message,logs:[]},500);}
    }

    if (url.pathname.match(/^\/longform\/[a-f0-9-]{36}$/) && request.method === "GET") {
      try {
        const jobId=url.pathname.split("/longform/")[1];
        const [jobs,segments]=await Promise.all([
          sbGet(env,"longform_jobs?id=eq."+jobId),
          sbGet(env,"longform_segments?job_id=eq."+jobId+"&order=segment_idx.asc"),
        ]);
        if (!jobs.length) return cors({error:"Job not found"},404);
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const enrichedSegs=segments.map(s=>({
          ...s,
          media:(s.media||[]).map(m=>({...m,public_url:m.r2_url?(m.r2_url.startsWith("http")?m.r2_url:r2Base+"/"+m.r2_url):null})),
          voice_public_url:s.voice_r2_url?(s.voice_r2_url.startsWith("http")?s.voice_r2_url:r2Base+"/"+s.voice_r2_url):null,
        }));
        return cors({...jobs[0],segments:enrichedSegs});
      } catch(e){return cors({error:e.message},500);}
    }
    if (url.pathname === "/longform/render" && request.method === "POST") {
      try {
        const {job_id,publish_at}=await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const segments=await sbGet(env,"longform_segments?job_id=eq."+job_id+"&select=segment_idx,status");
        const notReady=segments.filter(s=>s.status!=="ready");
        if (notReady.length>0) return cors({error:"Not all segments ready",not_ready:notReady.map(s=>s.segment_idx)},400);
        await sbPatch(env,"longform_jobs?id=eq."+job_id,{status:"rendering",updated_at:new Date().toISOString()});
        const lfUrl=env.LONGFORM_PIPELINE_URL||"";
        if (!lfUrl) return cors({error:"LONGFORM_PIPELINE_URL not set"},500);
        ctx.waitUntil(fetch(lfUrl,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({action:"render-full",job_id,publish_at:publish_at||null})}).catch(e=>console.error("lf render:",e.message)));
        return cors({status:"rendering",job_id});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/webhook" && request.method === "POST") {
      try {
        const {job_id,segment_idx,event,payload}=await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        if (event==="segment_voice_ready"&&segment_idx!=null) { await sbPatch(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx,{voice_r2_url:payload.voice_r2_url,voice_source:"ai",status:"ready",updated_at:new Date().toISOString()}); await _updateLongformStatus(env,job_id); }
        if (event==="segment_images_ready"&&segment_idx!=null) { await sbPatch(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx,{media:payload.media,status:"has_media",updated_at:new Date().toISOString()}); await _updateLongformStatus(env,job_id); }
        if (event==="render_complete") { await sbPatch(env,"longform_jobs?id=eq."+job_id,{status:"complete",youtube_id:payload.youtube_id||null,video_r2_url:payload.video_r2_url||null,updated_at:new Date().toISOString()}); }
        if (event==="render_failed") { await sbPatch(env,"longform_jobs?id=eq."+job_id,{status:"failed",error:payload.error||"render failed",updated_at:new Date().toISOString()}); }
        if (event==="script_ready") {
          // Delete existing segments first — prevents duplicates on retry
          await sbDelete(env,"longform_segments?job_id=eq."+job_id).catch(()=>{});
          for (const seg of (payload.segments||[])) {
            await sbInsert(env,"longform_segments",{
              job_id,
              segment_idx:    seg.segment_idx,
              segment_type:   seg.type || "context",
              label:          seg.label || ("Segment "+seg.segment_idx),
              script:         seg.script || "",
              duration_target:seg.duration_target || 60,
              image_prompts:  seg.image_prompts || [],
              media:          [],
              voice_r2_url:   null,
              voice_source:   null,
              status:         "has_script",
              created_at:     new Date().toISOString(),
            }).catch(e=>console.error("segment insert:",e.message));
          }
          await sbPatch(env,"longform_jobs?id=eq."+job_id,{status:"media_collecting",updated_at:new Date().toISOString()});
        }
        return cors({received:true});
      } catch(e){return cors({error:e.message},500);}
    }

    return cors({error:"route_not_found"},404);
  },

  async scheduled(event, env, ctx) {
    const cron=event.cron;
    console.log("Cron fired:", cron, new Date().toISOString());

    // Every minute — queue processor + health pings
    if (cron==="* * * * *") {
      await processQueue(env,ctx);
      if (env.MODAL_HEALTH_URL) fetch(env.MODAL_HEALTH_URL).catch(()=>{});
      if (env.TOPIC_COUNCIL_HEALTH_URL) fetch(env.TOPIC_COUNCIL_HEALTH_URL).catch(()=>{});
    }

    // Video creation — matches exactly what Cloudflare has registered:
    // 30 2 * * *  = 2:30 AM UTC = 8:00 AM IST
    // 0 8 * * *   = 8:00 AM UTC = 1:30 PM IST
    // 30 15 * * * = 3:30 PM UTC = 9:00 PM IST
    const isVideoCron = cron==="30 2 * * *" || cron==="0 8 * * *" || cron==="30 15 * * *";
    if (isVideoCron) {
      try {
        const rows=await sbGet(env,"system_state?id=eq.main&select=videos_per_day,last_cluster,publish");
        const s=rows[0]||{};
        const vpd=s.videos_per_day||1;
        const last=s.last_cluster||"";
        if (s.publish===false) { console.log("Publish OFF — skipping"); return; }
        // 1 vid/day: only 0 8 (1:30 PM IST)
        // 2 vids/day: 30 2 + 30 15 (8 AM + 9 PM IST)
        // 3 vids/day: all three slots
        const shouldFire =
          vpd===3 ||
          (vpd===2 && (cron==="30 2 * * *" || cron==="30 15 * * *")) ||
          (vpd===1 && cron==="0 8 * * *");
        if (!shouldFire) { console.log("Skip: vpd="+vpd+" cron="+cron); return; }
        const t=await pickTopic(env,null,last);
        const j=await createJob(t,env);
        await upsertState(env,{last_cluster:t.category});
        ctx.waitUntil(triggerRender(j,env));
        console.log("Video scheduled:",j.id,t.topic,t.category);
      } catch(e){console.error("Scheduled error:",e.message);}
    }

    // Analytics sync + replenish — fires on the 2:30 AM UTC slot (quietest time)
    if (cron==="30 2 * * *") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      try {
        const av=await sbGet(env,"topics?used=eq.false&council_score=gte.70&select=id");
        if(av.length<5) ctx.waitUntil(_runReplenish(env,ALL_CATS,12));
        console.log("Topics ready:", av.length);
      } catch(e){}
    }
  }
};

// ── REPLENISH — runs directly in Worker via ctx.waitUntil ─────
const SEED_HEADLINES = {
  AI:       [{t:"India AI startup ecosystem raises record funding 2026",c:"AI"},{t:"IIT researchers develop multilingual AI model for 22 Indian languages",c:"AI"},{t:"India launches national AI compute infrastructure for universities",c:"AI"},{t:"Indian AI firm wins global medical imaging benchmark",c:"AI"},{t:"India's AI regulation framework first draft released",c:"AI"}],
  Space:    [{t:"ISRO Gaganyaan astronaut training completion announced",c:"Space"},{t:"India commercial space sector attracts 10 new startups via IN-SPACe",c:"Space"},{t:"ISRO successfully tests space docking technology",c:"Space"},{t:"India launches earth observation satellite for 600 million farmers",c:"Space"},{t:"India space station 2035 blueprint released by ISRO",c:"Space"}],
  Gadgets:  [{t:"India first homegrown 5G chip enters mass production",c:"Gadgets"},{t:"India EV sales cross 1 million units monthly",c:"Gadgets"},{t:"Made in India drone achieves 200km range record",c:"Gadgets"},{t:"India affordable 5G smartphone launched at Rs 8000",c:"Gadgets"},{t:"India semiconductor fabrication plant gets government approval",c:"Gadgets"}],
  DeepTech: [{t:"IIT Bombay develops room temperature superconductor breakthrough",c:"DeepTech"},{t:"India quantum computing startup achieves 100 qubit milestone",c:"DeepTech"},{t:"Indian biotech firm develops dengue vaccine using AI",c:"DeepTech"},{t:"India 3D printing company manufactures bridge in 72 hours",c:"DeepTech"},{t:"India nanotech research center opens with 500 crore funding",c:"DeepTech"}],
  GreenTech:[{t:"India achieves 200 GW solar capacity milestone",c:"GreenTech"},{t:"India green hydrogen exports begin to Europe",c:"GreenTech"},{t:"India builds world largest floating solar farm",c:"GreenTech"},{t:"India EV battery recycling industry creates 50000 jobs",c:"GreenTech"},{t:"India wind energy capacity doubles in single year",c:"GreenTech"}],
  Startups: [{t:"Indian fintech startup becomes youngest decacorn in Asia",c:"Startups"},{t:"India agritech startup brings AI to 100 million farmers",c:"Startups"},{t:"India edtech pivot to skill training creates new unicorn",c:"Startups"},{t:"India health startup digitises primary care for rural areas",c:"Startups"},{t:"India B2B SaaS exports cross 10 billion dollars annually",c:"Startups"}],
};

async function _runReplenish(env, cats, target) {
  console.log("Replenish: cats="+cats+" target="+target);
  const today = new Date().toLocaleDateString("en-US",{year:"numeric",month:"long",day:"numeric"});
  let added = 0;

  for (const cat of cats) {
    if (added >= target) break;
    const perCat = Math.ceil((target - added) / (cats.length - cats.indexOf(cat)));
    const seeds = SEED_HEADLINES[cat] || [];
    // Shuffle seeds
    const shuffled = seeds.sort(()=>Math.random()-0.5).slice(0, perCat + 2);

    for (const seed of shuffled) {
      if (added >= target) break;
      try {
        const headline = seed.t;
        const prompt = `You are a content council for India20Sixty — YouTube Shorts about India's near future.
Today is ${today}. Evaluate this India tech headline for a 25-second Short.

HEADLINE: ${headline}
CATEGORY: ${cat}

TENSE: Past events before today → past tense. Future events → future tense only if genuinely upcoming.

SCENE PROMPTS — write 3 specific image prompts derived from the headline words themselves.
Extract the PHYSICAL SUBJECT from the headline:
- headline has "satellite" → show satellite hardware or launch pad
- headline has "rocket/ISRO" → show rocket on launch pad Sriharikota
- headline has "EV/electric" → show electric vehicle on Indian highway
- headline has "solar" → show solar panels in Indian field
- headline has "farmer" → show Indian farmer with agricultural technology
- headline has "drone" → show drone in flight over India
- headline has "chip/semiconductor" → show cleanroom with silicon wafers
- headline has "AI/software" → ONLY then show engineers at computers
Each prompt must be under 80 characters. No template text. No brackets.

Return ONLY valid JSON (no markdown):
{
  "video_angle": "compelling specific angle max 100 chars",
  "cluster": "${cat}",
  "key_fact": "most interesting verifiable fact from headline",
  "council_score": 75,
  "script": {
    "text": "50-55 word script. Pure Indian English. Hook with fact. Debate question end. No CTA.",
    "mood": "hopeful_future",
    "scene_prompts": ["[write actual hook shot prompt here — physical subject from headline, India setting]", "[write actual detail shot prompt here — close-up of technology]", "[write actual wide shot prompt here — India scale]"]
  }
}`;

        const r = await fetch("https://api.openai.com/v1/chat/completions",{
          method:"POST",
          headers:{"Authorization":"Bearer "+env.OPENAI_API_KEY,"Content-Type":"application/json"},
          body:JSON.stringify({model:"gpt-4o-mini",messages:[{role:"user",content:prompt}],
            temperature:0.8,max_tokens:700,response_format:{type:"json_object"}}),
          signal: AbortSignal.timeout(30000)
        });
        if(!r.ok){console.error("OpenAI error",r.status);continue;}
        const d = await r.json();
        const raw = d.choices[0].message.content;
        const data = JSON.parse(raw);
        const score = parseInt(data.council_score)||70;
        if(score < 65){console.log("Score too low:",score,data.video_angle?.slice(0,40));continue;}

        const s = data.script||{};
        // Validate scene prompts — strip template placeholders GPT sometimes outputs
        let rawPrompts = s.scene_prompts||[];
        const validated = rawPrompts.slice(0,3).map((sp,i) => {
          sp = String(sp).replace(/^\[|\]$/g,'').replace(/^Hook:|^Detail:|^Wide:/i,'').trim();
          if(sp.length < 20 || sp.includes('[write') || sp.toUpperCase().includes('WRITE ACTUAL'))
            sp = `photorealistic India, ${headline.slice(0,50)}, ${['establishing shot','close-up detail','wide view'][i]}, natural daylight`;
          return sp.slice(0,120);
        });
        while(validated.length < 3)
          validated.push(`photorealistic India, ${headline.slice(0,50)}, natural daylight, no text`);

        const scriptPkg = {
          text: s.text||"", reviewed_script: s.text||"",
          mood: s.mood||"hopeful_future",
          scene_prompts: validated,
          key_fact: data.key_fact||"", source:"council",
          word_count: (s.text||"").split(/\s+/).filter(Boolean).length,
        };

        await sbInsert(env,"topics",{
          topic: data.video_angle||seed.t,
          cluster: data.cluster||cat,
          council_score: score,
          script_package: scriptPkg,
          source: "council",
          used: false,
          created_at: new Date().toISOString(),
        });
        added++;
        console.log("Replenish ✓ "+cat+": "+data.video_angle?.slice(0,50)+" score="+score);
      } catch(e) {
        console.error("Replenish seed error:",seed.t.slice(0,40),e.message);
      }
    }
  }
  console.log("Replenish done: "+added+" topics added");
}

// ── HELPERS ───────────────────────────────────────────────────
const CATEGORIES={AI:{label:"AI & ML",color:"#00e5ff",emoji:"\uD83E\uDD16"},Space:{label:"Space & Defence",color:"#b388ff",emoji:"\uD83D\uDE80"},Gadgets:{label:"Gadgets & Tech",color:"#ffd740",emoji:"\uD83D\uDCF1"},DeepTech:{label:"Deep Tech",color:"#ff6b35",emoji:"\uD83D\uDD2C"},GreenTech:{label:"Green & Energy",color:"#00e676",emoji:"\u26A1"},Startups:{label:"Startups",color:"#ff6b9d",emoji:"\uD83D\uDCA1"}};
const ALL_CATS=Object.keys(CATEGORIES);
function sbh(env){return{apikey:env.SUPABASE_ANON_KEY,Authorization:"Bearer "+(env.SUPABASE_SERVICE_ROLE_KEY||env.SUPABASE_ANON_KEY),"Content-Type":"application/json"};}
async function sbGet(env,ep){const r=await fetch(env.SUPABASE_URL+"/rest/v1/"+ep,{headers:sbh(env)});if(!r.ok)throw new Error("GET "+r.status+" "+ep);return r.json();}
async function sbInsert(env,table,data){const r=await fetch(env.SUPABASE_URL+"/rest/v1/"+table,{method:"POST",headers:{...sbh(env),Prefer:"return=representation"},body:JSON.stringify(data)});if(!r.ok){const b=await r.text();throw new Error("INSERT "+r.status+" "+b.slice(0,200));}return(await r.json())[0];}
async function sbPatch(env,ep,data){const r=await fetch(env.SUPABASE_URL+"/rest/v1/"+ep,{method:"PATCH",headers:{...sbh(env),Prefer:"return=minimal"},body:JSON.stringify(data)});return r.ok;}
async function sbDelete(env,ep){const r=await fetch(env.SUPABASE_URL+"/rest/v1/"+ep,{method:"DELETE",headers:{...sbh(env),Prefer:"return=minimal"}});return r.ok;}
async function upsertState(env,data){try{const rows=await sbGet(env,"system_state?id=eq.main&select=id");if(rows.length>0){await sbPatch(env,"system_state?id=eq.main",{...data,updated_at:new Date().toISOString()});}else{await sbInsert(env,"system_state",{id:"main",...data});}}catch(e){console.error("upsertState:",e.message);}}
function cors(data,status){return new Response(JSON.stringify(data,null,2),{status:status||200,headers:{"content-type":"application/json","Access-Control-Allow-Origin":"*","Access-Control-Allow-Headers":"*","Access-Control-Allow-Methods":"GET,POST,OPTIONS,PATCH"}});}

// ── PRIORITY A TOPIC SELECTION ────────────────────────────────
async function pickTopic(env, preferCategory, lastCluster) {
  let priorityA=false;
  try{const rows=await sbGet(env,"system_state?id=eq.main&select=priority_a");priorityA=rows[0]?.priority_a===true;}catch(e){}
  if (preferCategory&&ALL_CATS.includes(preferCategory)) { const t=await _fetchBestTopic(env,preferCategory); if(t)return t; }
  // Priority A: Space first — but skip if last job was also Space (avoid channel being 100% Space)
  if (priorityA&&lastCluster!=="Space") {
    const spaceTopics=await sbGet(env,"topics?used=eq.false&council_score=gte.70&cluster=eq.Space&order=council_score.desc&limit=1");
    if (spaceTopics.length>0) {
      const t=spaceTopics[0];
      await sbPatch(env,"topics?id=eq."+t.id,{used:true,used_at:new Date().toISOString()});
      console.log("Priority A → Space:",t.topic);
      return {topic:t.topic,script_package:t.script_package,council_score:t.council_score,category:"Space",source:"priority_a"};
    }
  }
  const best=await _fetchBestTopic(env,null);
  if (best) return best;
  const pool=["India AI healthcare revolution","ISRO next space mission","India EV revolution","AI chips made in India","India solar energy breakthrough"];
  return {topic:pool[Math.floor(Math.random()*pool.length)],script_package:null,council_score:0,category:"AI",source:"fallback"};
}
async function _fetchBestTopic(env,cluster){
  let ep="topics?used=eq.false&council_score=gte.70&order=council_score.desc&limit=1";
  if(cluster)ep+="&cluster=eq."+cluster;
  const t=await sbGet(env,ep);if(!t.length)return null;
  await sbPatch(env,"topics?id=eq."+t[0].id,{used:true,used_at:new Date().toISOString()});
  return {topic:t[0].topic,script_package:t[0].script_package,council_score:t[0].council_score,category:t[0].cluster||"AI",source:"db_approved"};
}
async function callCouncil(env,topic,source,category){
  const url=env.TOPIC_COUNCIL_URL||"";
  if(!url)throw new Error("TOPIC_COUNCIL_URL not set");
  const r=await fetch(url,{method:"POST",headers:{"content-type":"application/json"},
    body:JSON.stringify({action:"full-pipeline",topic,source,category})});
  if(!r.ok)throw new Error("Council returned "+r.status);
  return r.json();
}
async function createJob(t,env){return await sbInsert(env,"jobs",{topic:t.topic,cluster:t.category||"AI",status:"pending",script_package:t.script_package||null,council_score:t.council_score||0,retries:0,created_at:new Date().toISOString(),updated_at:new Date().toISOString()});}
async function processQueue(env,ctx){const ago=new Date(Date.now()-15*60000).toISOString();try{for(const j of await sbGet(env,"jobs?status=eq.processing&updated_at=lt."+ago+"&retries=lt.3"))await sbPatch(env,"jobs?id=eq."+j.id,{status:"pending",retries:(j.retries||0)+1,updated_at:new Date().toISOString()});for(const j of await sbGet(env,"jobs?status=eq.processing&updated_at=lt."+ago+"&retries=gte.3"))await sbPatch(env,"jobs?id=eq."+j.id,{status:"failed",error:"max_retries_exceeded",updated_at:new Date().toISOString()});const pending=await sbGet(env,"jobs?status=eq.pending&order=created_at.asc&limit=1");if(!pending.length)return;await sbPatch(env,"jobs?id=eq."+pending[0].id,{status:"processing",started_at:new Date().toISOString(),updated_at:new Date().toISOString()});ctx.waitUntil(triggerRender(pending[0],env));}catch(e){console.error("Queue:",e.message);}}
async function triggerRender(job,env,image_urls){if(!env.RENDER_PIPELINE_URL){await sbPatch(env,"jobs?id=eq."+job.id,{status:"failed",error:"RENDER_PIPELINE_URL not set",updated_at:new Date().toISOString()});return;}const renderUrl=env.RENDER_PIPELINE_URL.trim().replace(/\/$/,"");try{const body={job_id:job.id,topic:job.topic,script_package:job.script_package,webhook_url:(env.WORKER_URL||"").trim().replace(/\/$/,"")+"/webhook"};if(image_urls&&image_urls.length>=3)body.image_urls=image_urls;const r=await fetch(renderUrl,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body),signal:AbortSignal.timeout(60000)});if(!r.ok)throw new Error(r.status+": "+(await r.text()).slice(0,100));}catch(e){console.error("Render trigger:",e.message);await sbPatch(env,"jobs?id=eq."+job.id,{status:"failed",error:e.message,updated_at:new Date().toISOString()});}}
async function createAnalyticsRecord(job_id,youtube_id,env){try{const jobs=await sbGet(env,"jobs?id=eq."+job_id+"&select=topic,cluster,council_score");const j=jobs[0]||{};const ex=await sbGet(env,"analytics?video_id=eq."+job_id);if(!ex.length){await sbInsert(env,"analytics",{video_id:job_id,youtube_id,topic:j.topic||"",cluster:j.cluster||"AI",council_score:j.council_score||0,youtube_views:0,youtube_likes:0,comment_count:0,score:0,created_at:new Date().toISOString()});}else{await sbPatch(env,"analytics?video_id=eq."+job_id,{youtube_id,updated_at:new Date().toISOString()});}}catch(e){console.error("createAnalyticsRecord:",e.message);}}
async function syncYouTubeAnalytics(env){
  if(!env.YOUTUBE_CLIENT_ID||!env.YOUTUBE_CLIENT_SECRET||!env.YOUTUBE_REFRESH_TOKEN){
    throw new Error("YouTube OAuth credentials not set in Cloudflare variables");
  }
  // Get OAuth token
  const tr=await fetch("https://oauth2.googleapis.com/token",{
    method:"POST",headers:{"content-type":"application/x-www-form-urlencoded"},
    body:new URLSearchParams({client_id:env.YOUTUBE_CLIENT_ID,client_secret:env.YOUTUBE_CLIENT_SECRET,refresh_token:env.YOUTUBE_REFRESH_TOKEN,grant_type:"refresh_token"})
  });
  if(!tr.ok){const b=await tr.text();throw new Error("OAuth failed "+tr.status+": "+b.slice(0,150));}
  const tokenData=await tr.json();
  if(!tokenData.access_token) throw new Error("OAuth returned no access_token: "+JSON.stringify(tokenData).slice(0,100));
  const token=tokenData.access_token;

  // Get all completed jobs with a real youtube_id
  const jobs=(await sbGet(env,"jobs?status=eq.complete&youtube_id=not.is.null&order=created_at.desc&limit=50&select=id,topic,cluster,council_score,youtube_id"))
    .filter(j=>j.youtube_id&&j.youtube_id!=="TEST_MODE"&&j.youtube_id.length>5);
  if(!jobs.length){console.log("Analytics: no completed jobs with youtube_id");return;}

  // Fetch stats from YouTube API in one call
  const ids=jobs.map(j=>j.youtube_id).join(",");
  console.log("Analytics: fetching stats for",jobs.length,"videos:",ids.slice(0,80));
  const res=await fetch("https://www.googleapis.com/youtube/v3/videos?part=statistics&id="+ids+"&access_token="+token);
  if(!res.ok){const b=await res.text();throw new Error("YouTube API failed "+res.status+": "+b.slice(0,150));}
  const ytData=await res.json();
  const items=ytData.items||[];
  console.log("Analytics: YouTube returned",items.length,"items");

  // Upsert analytics for each video
  let updated=0;
  for(const item of items){
    const s=item.statistics||{};
    const views=parseInt(s.viewCount||0);
    const likes=parseInt(s.likeCount||0);
    const coms=parseInt(s.commentCount||0);
    const score=views+likes*50+coms*30;
    const job=jobs.find(j=>j.youtube_id===item.id);
    if(!job) continue;
    const ex=await sbGet(env,"analytics?video_id=eq."+job.id);
    if(ex.length>0){
      await sbPatch(env,"analytics?video_id=eq."+job.id,{youtube_id:item.id,youtube_views:views,youtube_likes:likes,comment_count:coms,score,updated_at:new Date().toISOString()});
    } else {
      await sbInsert(env,"analytics",{video_id:job.id,youtube_id:item.id,topic:job.topic||"",cluster:job.cluster||"AI",council_score:job.council_score||0,youtube_views:views,youtube_likes:likes,comment_count:coms,score,created_at:new Date().toISOString()});
    }
    updated++;
    console.log("Analytics:",item.id,"views="+views,"likes="+likes);
  }
  console.log("Analytics sync done: updated",updated,"of",jobs.length,"videos");
}
async function _updateLongformStatus(env,jobId){
  try{
    const segs=await sbGet(env,"longform_segments?job_id=eq."+jobId+"&select=status,voice_r2_url,media");
    if(!segs.length)return;
    const allReady=segs.every(s=>s.status==="ready"||(s.voice_r2_url&&(s.media||[]).length>0));
    const allHaveMedia=segs.every(s=>["has_media","ready","generating_voice","generating_images"].includes(s.status)||s.voice_r2_url||(s.media||[]).length>0);
    const newStatus=allReady?"ready_to_render":allHaveMedia?"media_collecting":"scripting";
    await sbPatch(env,"longform_jobs?id=eq."+jobId,{status:newStatus,updated_at:new Date().toISOString()});
    // Auto-trigger render when all segments ready
    if(allReady){
      const jobs=await sbGet(env,"longform_jobs?id=eq."+jobId+"&select=auto_mode");
      if(jobs.length&&jobs[0].auto_mode!==false){
        const lfUrl=(env.LONGFORM_PIPELINE_URL||"").trim().replace(/\/$/,"");
        if(lfUrl){
          fetch(lfUrl,{method:"POST",headers:{"content-type":"application/json"},
            body:JSON.stringify({action:"render-full",job_id:jobId})
          }).catch(e=>console.error("auto-render:",e.message));
          console.log("Auto-render triggered for",jobId);
        }
      }
    }
  }catch(e){console.error("_updateLongformStatus:",e.message);}
}