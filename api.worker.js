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

    if (url.pathname === "/set-mode" && request.method === "POST") {
      try {
        const {mode} = await request.json();
        const validMode = ["auto","stage"].includes(mode)?mode:"auto";
        const updates = {mode:validMode, updated_at:new Date().toISOString()};
        if (validMode==="auto") { updates.publish=true; updates.voice_mode="ai"; } else { updates.publish=false; }
        await upsertState(env, updates);
        return cors({mode:validMode});
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

    if (url.pathname === "/upload-image" && request.method === "POST") {
      try {
        if (!env.R2) return cors({error:"R2 not bound"},500);
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const topic=url.searchParams.get("topic")||"uploaded";
        const filename=url.searchParams.get("filename")||("img_"+Date.now()+".png");
        const topicSlug=topic.toLowerCase().replace(/[^a-z0-9]+/g,"-").slice(0,40);
        const key="images/"+topicSlug+"/"+filename;
        const blob=await request.arrayBuffer();
        await env.R2.put(key,blob,{httpMetadata:{contentType:request.headers.get("content-type")||"image/png"}});
        const publicUrl=r2Base+"/"+key;
        await sbInsert(env,"image_cache",{topic,r2_key:key,public_url:publicUrl,scene_idx:0,created_at:new Date().toISOString()}).catch(()=>{});
        return cors({status:"uploaded",key,url:publicUrl});
      } catch(e) { return cors({error:e.message},500); }
    }

    if (url.pathname === "/image-library") {
      try {
        const cluster = url.searchParams.get("cluster") || "";
        const jobType = url.searchParams.get("job_type") || "";
        const r2Base  = (env.R2_BASE_URL||"").replace(/\/$/,"");

        // Primary: query image_cache table
        try {
          // Use basic columns first — works even without migration
          let ep = "image_cache?select=id,r2_key,public_url,topic,scene_idx,created_at,cluster,engine,job_type,job_id&order=created_at.desc&limit=500";
          if (cluster) ep += "&cluster=eq." + cluster;
          if (jobType) ep += "&job_type=eq." + jobType;
          const rows = await sbGet(env, ep);
          if (rows.length > 0) {
            return cors({
              images: rows.map(r => ({
                id:       r.id,
                key:      r.r2_key,
                url:      r.public_url || (r2Base && r.r2_key ? r2Base+"/"+r.r2_key : ""),
                topic:    r.topic || r.job_id || "India Tech",
                cluster:  r.cluster || "AI",
                engine:   r.engine  || "FLUX",
                job_type: r.job_type || "shorts",
                scene_idx:r.scene_idx || 0,
                uploaded: r.created_at,
                has_url:  !!(r.public_url || r2Base),
              })),
              total: rows.length,
              source: "image_cache",
            });
          }
        } catch(e) {
          console.error("image_cache query failed:", e.message);
        }

        // Fallback: scan R2 under images/ prefix
        if (env.R2) {
          const prefix = cluster ? `images/${cluster}/` : "images/";
          const listed = await env.R2.list({prefix, limit:500});
          const images = (listed.objects||[])
            .filter(o => o.key.match(/\.(png|jpg|jpeg)$/i))
            .sort((a,b) => new Date(b.uploaded) - new Date(a.uploaded))
            .map(o => {
              const parts = o.key.split("/");
              return {
                key:      o.key,
                url:      r2Base + "/" + o.key,
                cluster:  parts[1] || "AI",
                topic:    parts[2] ? parts[2].split("_")[0].replace(/-/g," ") : "India Tech",
                engine:   "FLUX",
                job_type: "shorts",
                uploaded: o.uploaded,
              };
            });
          return cors({images, total:images.length, source:"r2"});
        }

        return cors({images:[], total:0, source:"empty",
          note:"Run Supabase migration and configure R2 binding to see images"});
      } catch(e) { return cors({error:e.message,images:[]}); }
    }

    if (url.pathname === "/delete-images" && request.method === "POST") {
      try {
        const body = await request.json();
        const keys = body.keys || [];   // R2 keys to delete
        const ids  = body.ids  || [];   // image_cache row ids to delete
        if (!keys.length && !ids.length) return cors({error:"No keys or ids provided"},400);

        let r2Deleted = 0, dbDeleted = 0, errors = [];

        // Delete from R2 if bucket is bound
        if (env.R2 && keys.length) {
          for (const key of keys) {
            try {
              await env.R2.delete(key);
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
      ctx.waitUntil(triggerReplenish(env,body.target||12,body.categories||null));
      return cors({status:"replenish_triggered",categories:body.categories,target:body.target||12});
    }

    if (url.pathname === "/publish-state") {
      if (request.method==="GET") { try { const r=await sbGet(env,"system_state?id=eq.main&select=publish"); return cors({publish:r[0]?.publish===true}); } catch(e){return cors({publish:false});} }
      if (request.method==="POST") { try { const {publish}=await request.json(); await upsertState(env,{publish:!!publish}); return cors({publish:!!publish}); } catch(e){return cors({error:e.message},500);} }
    }

    if (url.pathname === "/voice-mode") {
      if (request.method==="GET") { try { const r=await sbGet(env,"system_state?id=eq.main&select=voice_mode"); return cors({voice_mode:r[0]?.voice_mode||"ai"}); } catch(e){return cors({voice_mode:"ai"});} }
      if (request.method==="POST") { try { const {voice_mode}=await request.json(); const m=["ai","human"].includes(voice_mode)?voice_mode:"ai"; await upsertState(env,{voice_mode:m}); return cors({voice_mode:m}); } catch(e){return cors({error:e.message},500);} }
    }

    if (url.pathname === "/set-schedule" && request.method === "POST") {
      try { const {videos_per_day}=await request.json(); const vpd=Math.min(3,Math.max(1,parseInt(videos_per_day)||1)); await upsertState(env,{videos_per_day:vpd}); return cors({videos_per_day:vpd}); }
      catch(e){return cors({error:e.message},500);}
    }
    if (url.pathname === "/get-schedule") {
      try { const r=await sbGet(env,"system_state?id=eq.main&select=videos_per_day"); return cors({videos_per_day:r[0]?.videos_per_day||1}); }
      catch(e){return cors({videos_per_day:1});}
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

    if (url.pathname === "/sync-analytics" && request.method === "POST") { ctx.waitUntil(syncYouTubeAnalytics(env)); return cors({status:"sync_started"}); }

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

    if (url.pathname === "/retry-upload" && request.method === "POST") {
      try {
        const {job_id}=await request.json();
        if (!job_id) return cors({error:"Missing job_id"},400);
        const triggerUrl=(env.RENDER_PIPELINE_URL||"").trim().replace(/\/$/,"");
        await sbPatch(env,"jobs?id=eq."+job_id,{status:"upload",error:null,updated_at:new Date().toISOString()});
        ctx.waitUntil(fetch(triggerUrl,{method:"POST",headers:{"content-type":"application/json"},
          body:JSON.stringify({action:"retry-upload",job_id})}).catch(e=>console.error("retry:",e.message)));
        return cors({status:"retry_triggered",job_id});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/upload-voice" && request.method === "POST") {
      try {
        const jobId=url.searchParams.get("job_id");
        if (!jobId) return cors({error:"Missing job_id"},400);
        const blob=await request.arrayBuffer();
        const r2Key="voices/"+jobId+"/voice.webm";
        if (env.R2) await env.R2.put(r2Key,blob,{httpMetadata:{contentType:"audio/webm"}});
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

    if (url.pathname === "/mark-cbdp" && request.method === "POST") {
      try {
        const failed=await sbGet(env,"jobs?status=eq.failed&select=id,error,script_package");
        const kw=["400","401","403","youtube","upload","quota","invaliddescription","bad request"];
        let marked=0;
        for (const j of failed) { if (kw.some(k=>(j.error||"").toLowerCase().includes(k))&&j.script_package) { await sbPatch(env,"jobs?id=eq."+j.id,{status:"cbdp",updated_at:new Date().toISOString()}); marked++; } }
        return cors({marked,total_failed:failed.length});
      } catch(e){return cors({error:e.message},500);}
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
        if (!env.R2) return cors({error:"R2 not bound"},500);
        const blob=await request.arrayBuffer();
        if (blob.byteLength<10000) return cors({error:"File too small"},400);
        const r2Key="manual/"+jobId+"/video.mp4";
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        await env.R2.put(r2Key,blob,{httpMetadata:{contentType:"video/mp4"}});
        const publicUrl=r2Base+"/"+r2Key;
        await sbPatch(env,"jobs?id=eq."+jobId,{video_r2_url:publicUrl,status:"staged",updated_at:new Date().toISOString()});
        return cors({status:"uploaded",r2_key:r2Key,url:publicUrl,job_id:jobId,size_kb:Math.round(blob.byteLength/1024)});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/manual-jobs") {
      try {
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const fields="id,topic,cluster,status,script_package,video_r2_url,created_at,updated_at,youtube_id";
        const [p,s,a,c,f]=await Promise.all([
          sbGet(env,"jobs?status=eq.manual_pending&order=created_at.desc&limit=30&select="+fields),
          sbGet(env,"jobs?status=eq.staged&order=created_at.desc&limit=30&select="+fields),
          sbGet(env,"jobs?status=in.(voice,upload,mixing)&order=created_at.desc&limit=20&select="+fields),
          sbGet(env,"jobs?status=eq.complete&order=created_at.desc&limit=20&select="+fields),
          sbGet(env,"jobs?status=eq.failed&order=created_at.desc&limit=10&select="+fields),
        ]);
        const all=[...p,...s,...a,...c,...f].filter(j=>j.script_package&&j.script_package.source==="manual");
        const seen=new Set();
        return cors(all.filter(j=>{if(seen.has(j.id))return false;seen.add(j.id);return true;}).map(j=>{
          const raw=j.video_r2_url||""; const v=raw.startsWith("http")?raw:(raw&&r2Base?r2Base+"/"+raw:null);
          return {...j,video_public_url:v,has_video:!!v};
        }));
      } catch(e){return cors({error:e.message},500);}
    }

    // ══════════════════════════════════════════════════════════
    // LONG-FORM ROUTES
    // ══════════════════════════════════════════════════════════

    if (url.pathname === "/longform/create" && request.method === "POST") {
      try {
        const {topic,cluster,target_duration}=await request.json().catch(()=>({}));
        if (!topic) return cors({error:"Missing topic"},400);
        const safeCluster=(cluster&&ALL_CATS.includes(cluster))?cluster:"Space";
        const durSecs=Math.min(720,Math.max(180,(parseInt(target_duration)||420)));
        const job=await sbInsert(env,"longform_jobs",{
          topic:topic.trim().slice(0,300),cluster:safeCluster,
          status:"scripting",target_duration:durSecs,
          created_at:new Date().toISOString(),updated_at:new Date().toISOString(),
        });
        const lfUrl=env.LONGFORM_PIPELINE_URL||"";
        if (lfUrl) ctx.waitUntil(fetch(lfUrl+"/dispatch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({action:"generate-script",job_id:job.id,topic,cluster:safeCluster,target_duration:durSecs})}).catch(e=>console.error("lf script:",e.message)));
        return cors({status:"created",job_id:job.id,topic,cluster:safeCluster,target_duration:durSecs});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/jobs") {
      try { return cors(await sbGet(env,"longform_jobs?order=created_at.desc&limit=30")); }
      catch(e){return cors({error:e.message},500);}
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

    if (url.pathname === "/longform/segment/script" && request.method === "POST") {
      try {
        const {job_id,segment_idx,script}=await request.json();
        if (!job_id||segment_idx==null||!script) return cors({error:"Missing fields"},400);
        await sbPatch(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx,{script,updated_at:new Date().toISOString()});
        await _updateLongformStatus(env,job_id);
        return cors({status:"updated",job_id,segment_idx});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/segment/upload-media" && request.method === "POST") {
      try {
        if (!env.R2) return cors({error:"R2 not bound"},500);
        const jobId=url.searchParams.get("job_id");
        const segIdx=url.searchParams.get("segment_idx");
        const mediaIdx=url.searchParams.get("media_idx")||"0";
        const mediaType=url.searchParams.get("media_type")||"image";
        if (!jobId||segIdx==null) return cors({error:"Missing job_id or segment_idx"},400);
        const ext=mediaType==="video"?"mp4":"png";
        const r2Key="longform/"+jobId+"/seg"+segIdx+"_media"+mediaIdx+"."+ext;
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const blob=await request.arrayBuffer();
        const ct=request.headers.get("content-type")||(mediaType==="video"?"video/mp4":"image/png");
        await env.R2.put(r2Key,blob,{httpMetadata:{contentType:ct}});
        const publicUrl=r2Base+"/"+r2Key;
        const segs=await sbGet(env,"longform_segments?job_id=eq."+jobId+"&segment_idx=eq."+segIdx+"&select=id,media,script");
        if (!segs.length) return cors({error:"Segment not found"},404);
        const media=segs[0].media||[];
        const idx=parseInt(mediaIdx);
        while (media.length<=idx) media.push(null);
        media[idx]={type:mediaType,r2_url:r2Key,public_url:publicUrl,order:idx};
        const filtered=media.filter(Boolean);
        const newStatus=filtered.length>0?(segs[0].script?"has_media":"has_media_no_script"):segs[0].status;
        await sbPatch(env,"longform_segments?job_id=eq."+jobId+"&segment_idx=eq."+segIdx,{media:filtered,status:newStatus,updated_at:new Date().toISOString()});
        await _updateLongformStatus(env,jobId);
        return cors({status:"uploaded",r2_key:r2Key,public_url:publicUrl,segment_idx:parseInt(segIdx),media_idx:idx,type:mediaType});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/segment/upload-voice" && request.method === "POST") {
      try {
        if (!env.R2) return cors({error:"R2 not bound"},500);
        const jobId=url.searchParams.get("job_id");
        const segIdx=url.searchParams.get("segment_idx");
        if (!jobId||segIdx==null) return cors({error:"Missing job_id or segment_idx"},400);
        const r2Key="longform/"+jobId+"/seg"+segIdx+"_voice.webm";
        const r2Base=(env.R2_BASE_URL||"").replace(/\/$/,"");
        const blob=await request.arrayBuffer();
        await env.R2.put(r2Key,blob,{httpMetadata:{contentType:"audio/webm"}});
        const publicUrl=r2Base+"/"+r2Key;
        await sbPatch(env,"longform_segments?job_id=eq."+jobId+"&segment_idx=eq."+segIdx,{voice_r2_url:r2Key,voice_source:"human",status:"ready",updated_at:new Date().toISOString()});
        await _updateLongformStatus(env,jobId);
        return cors({status:"uploaded",r2_key:r2Key,public_url:publicUrl,segment_idx:parseInt(segIdx)});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/segment/generate-voice" && request.method === "POST") {
      try {
        const {job_id,segment_idx}=await request.json();
        if (!job_id||segment_idx==null) return cors({error:"Missing fields"},400);
        const segs=await sbGet(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx+"&select=id,script");
        if (!segs.length) return cors({error:"Segment not found"},404);
        if (!segs[0].script) return cors({error:"No script for this segment"},400);
        await sbPatch(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx,{status:"generating_voice",updated_at:new Date().toISOString()});
        const lfUrl=env.LONGFORM_PIPELINE_URL||"";
        if (lfUrl) ctx.waitUntil(fetch(lfUrl+"/dispatch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({action:"generate-segment-voice",job_id,segment_idx})}).catch(e=>console.error("seg voice:",e.message)));
        return cors({status:"generating",job_id,segment_idx});
      } catch(e){return cors({error:e.message},500);}
    }

    if (url.pathname === "/longform/segment/generate-images" && request.method === "POST") {
      try {
        const {job_id,segment_idx}=await request.json();
        if (!job_id||segment_idx==null) return cors({error:"Missing fields"},400);
        await sbPatch(env,"longform_segments?job_id=eq."+job_id+"&segment_idx=eq."+segment_idx,{status:"generating_images",updated_at:new Date().toISOString()});
        const lfUrl=env.LONGFORM_PIPELINE_URL||"";
        if (lfUrl) ctx.waitUntil(fetch(lfUrl+"/dispatch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({action:"generate-segment-images",job_id,segment_idx})}).catch(e=>console.error("seg img:",e.message)));
        return cors({status:"generating",job_id,segment_idx});
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
        ctx.waitUntil(fetch(lfUrl+"/dispatch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({action:"render-full",job_id,publish_at:publish_at||null})}).catch(e=>console.error("lf render:",e.message)));
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
    if (cron==="* * * * *") {
      await processQueue(env,ctx);
      if (env.MODAL_HEALTH_URL) fetch(env.MODAL_HEALTH_URL).catch(()=>{});
      if (env.TOPIC_COUNCIL_HEALTH_URL) fetch(env.TOPIC_COUNCIL_HEALTH_URL).catch(()=>{});
    }
    if (cron==="30 0,6,12 * * *") {
      try {
        const rows=await sbGet(env,"system_state?id=eq.main&select=videos_per_day,last_cluster");
        const vpd=rows[0]?.videos_per_day||1; const last=rows[0]?.last_cluster||"";
        const utcH=new Date().getUTCHours();
        const fire=vpd===3||(vpd===2&&(utcH===0||utcH===12))||(vpd===1&&utcH===6);
        if (fire) {
          const t=await pickTopic(env,null,last);
          const j=await createJob(t,env);
          await upsertState(env,{last_cluster:t.category});
          ctx.waitUntil(triggerRender(j,env));
          console.log("Scheduled:",j.id,t.topic,t.category);
        }
      } catch(e){console.error("Scheduled:",e.message);}
    }
    if (cron==="30 20 * * *") {
      ctx.waitUntil(syncYouTubeAnalytics(env));
      try { const av=await sbGet(env,"topics?used=eq.false&council_score=gte.70&select=id"); if(av.length<5)ctx.waitUntil(triggerReplenish(env,12,null)); } catch(e){}
    }
  }
};

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
async function triggerReplenish(env,target,categories){
  const url=env.TOPIC_COUNCIL_URL||"";
  if(!url){console.error("Replenish: TOPIC_COUNCIL_URL not set");return;}
  console.log("Replenish: POST",url,"target="+target);
  try{
    const r=await fetch(url,{method:"POST",headers:{"content-type":"application/json"},
      body:JSON.stringify({action:"replenish",target,categories:categories||ALL_CATS}),
      signal:AbortSignal.timeout(120000)
    });
    const txt=await r.text().catch(()=>"(no body)");
    console.log("Replenish response:",r.status,txt.slice(0,200));
  }catch(e){console.error("Replenish:",e.message);}
}
async function createJob(t,env){return await sbInsert(env,"jobs",{topic:t.topic,cluster:t.category||"AI",status:"pending",script_package:t.script_package||null,council_score:t.council_score||0,retries:0,created_at:new Date().toISOString(),updated_at:new Date().toISOString()});}
async function processQueue(env,ctx){const ago=new Date(Date.now()-15*60000).toISOString();try{for(const j of await sbGet(env,"jobs?status=eq.processing&updated_at=lt."+ago+"&retries=lt.3"))await sbPatch(env,"jobs?id=eq."+j.id,{status:"pending",retries:(j.retries||0)+1,updated_at:new Date().toISOString()});for(const j of await sbGet(env,"jobs?status=eq.processing&updated_at=lt."+ago+"&retries=gte.3"))await sbPatch(env,"jobs?id=eq."+j.id,{status:"failed",error:"max_retries_exceeded",updated_at:new Date().toISOString()});const pending=await sbGet(env,"jobs?status=eq.pending&order=created_at.asc&limit=1");if(!pending.length)return;await sbPatch(env,"jobs?id=eq."+pending[0].id,{status:"processing",started_at:new Date().toISOString(),updated_at:new Date().toISOString()});ctx.waitUntil(triggerRender(pending[0],env));}catch(e){console.error("Queue:",e.message);}}
async function triggerRender(job,env,image_urls){if(!env.RENDER_PIPELINE_URL){await sbPatch(env,"jobs?id=eq."+job.id,{status:"failed",error:"RENDER_PIPELINE_URL not set",updated_at:new Date().toISOString()});return;}const renderUrl=env.RENDER_PIPELINE_URL.trim().replace(/\/$/,"");try{const body={job_id:job.id,topic:job.topic,script_package:job.script_package,webhook_url:(env.WORKER_URL||"").trim().replace(/\/$/,"")+"/webhook"};if(image_urls&&image_urls.length>=3)body.image_urls=image_urls;const r=await fetch(renderUrl,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body),signal:AbortSignal.timeout(60000)});if(!r.ok)throw new Error(r.status+": "+(await r.text()).slice(0,100));}catch(e){console.error("Render trigger:",e.message);await sbPatch(env,"jobs?id=eq."+job.id,{status:"failed",error:e.message,updated_at:new Date().toISOString()});}}
async function createAnalyticsRecord(job_id,youtube_id,env){try{const jobs=await sbGet(env,"jobs?id=eq."+job_id+"&select=topic,cluster,council_score");const j=jobs[0]||{};const ex=await sbGet(env,"analytics?video_id=eq."+job_id);if(!ex.length){await sbInsert(env,"analytics",{video_id:job_id,youtube_id,topic:j.topic||"",cluster:j.cluster||"AI",council_score:j.council_score||0,youtube_views:0,youtube_likes:0,comment_count:0,score:0,created_at:new Date().toISOString()});}else{await sbPatch(env,"analytics?video_id=eq."+job_id,{youtube_id,updated_at:new Date().toISOString()});}}catch(e){console.error("createAnalyticsRecord:",e.message);}}
async function syncYouTubeAnalytics(env){
  if(!env.YOUTUBE_CLIENT_ID)return;
  try{
    // Get analytics rows that have a youtube_id
    const rows=(await sbGet(env,"analytics?youtube_id=not.is.null&order=created_at.desc&limit=50"))
      .filter(r=>r.youtube_id&&r.youtube_id!=="TEST_MODE");
    if(!rows.length){
      // Fallback: sync from jobs table for older records
      const jobs=(await sbGet(env,"jobs?status=eq.complete&youtube_id=not.is.null&order=created_at.desc&limit=50"))
        .filter(j=>j.youtube_id&&j.youtube_id!=="TEST_MODE");
      if(!jobs.length)return;
      const tr=await fetch("https://oauth2.googleapis.com/token",{method:"POST",headers:{"content-type":"application/x-www-form-urlencoded"},body:new URLSearchParams({client_id:env.YOUTUBE_CLIENT_ID,client_secret:env.YOUTUBE_CLIENT_SECRET,refresh_token:env.YOUTUBE_REFRESH_TOKEN,grant_type:"refresh_token"})});
      if(!tr.ok)return;
      const token=(await tr.json()).access_token;
      const ids=jobs.map(j=>j.youtube_id).join(",");
      const res=await fetch("https://www.googleapis.com/youtube/v3/videos?part=statistics&id="+ids+"&access_token="+token);
      if(!res.ok)return;
      for(const item of(await res.json()).items||[]){
        const s=item.statistics||{};
        const views=parseInt(s.viewCount||0),likes=parseInt(s.likeCount||0),coms=parseInt(s.commentCount||0),score=views+likes*50+coms*30;
        const job=jobs.find(j=>j.youtube_id===item.id);
        if(!job)continue;
        const ex=await sbGet(env,"analytics?video_id=eq."+job.id);
        if(ex.length>0)await sbPatch(env,"analytics?video_id=eq."+job.id,{youtube_id:item.id,youtube_views:views,youtube_likes:likes,comment_count:coms,score,updated_at:new Date().toISOString()});
        else await sbInsert(env,"analytics",{video_id:job.id,youtube_id:item.id,topic:job.topic||"",cluster:job.cluster||"AI",youtube_views:views,youtube_likes:likes,comment_count:coms,score,created_at:new Date().toISOString()});
      }
      return;
    }
    const tr=await fetch("https://oauth2.googleapis.com/token",{method:"POST",headers:{"content-type":"application/x-www-form-urlencoded"},body:new URLSearchParams({client_id:env.YOUTUBE_CLIENT_ID,client_secret:env.YOUTUBE_CLIENT_SECRET,refresh_token:env.YOUTUBE_REFRESH_TOKEN,grant_type:"refresh_token"})});
    if(!tr.ok){console.error("Analytics: OAuth failed",await tr.text());return;}
    const token=(await tr.json()).access_token;
    const ids=rows.map(r=>r.youtube_id).join(",");
    const res=await fetch("https://www.googleapis.com/youtube/v3/videos?part=statistics&id="+ids+"&access_token="+token);
    if(!res.ok){console.error("Analytics: YouTube API failed",await res.text());return;}
    for(const item of(await res.json()).items||[]){
      const s=item.statistics||{};
      const views=parseInt(s.viewCount||0),likes=parseInt(s.likeCount||0),coms=parseInt(s.commentCount||0),score=views+likes*50+coms*30;
      const row=rows.find(r=>r.youtube_id===item.id);
      if(!row)continue;
      await sbPatch(env,"analytics?video_id=eq."+row.video_id,{youtube_views:views,youtube_likes:likes,comment_count:coms,score,updated_at:new Date().toISOString()});
    }
    console.log("Analytics synced:",rows.length,"videos");
  }catch(e){console.error("Analytics sync:",e.message);}
}
async function _updateLongformStatus(env,jobId){try{const segs=await sbGet(env,"longform_segments?job_id=eq."+jobId+"&select=status");if(!segs.length)return;const allReady=segs.every(s=>s.status==="ready");const allHaveMedia=segs.every(s=>["has_media","has_media_no_script","ready","generating_voice","generating_images"].includes(s.status));const newStatus=allReady?"ready_to_render":allHaveMedia?"media_collecting":"scripting";await sbPatch(env,"longform_jobs?id=eq."+jobId,{status:newStatus,updated_at:new Date().toISOString()});}catch(e){console.error("_updateLongformStatus:",e.message);}}