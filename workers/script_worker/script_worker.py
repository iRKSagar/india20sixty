export default {

  async fetch(request, env) {

    try {

      const url = new URL(request.url)

      const base = `${env.SUPABASE_URL}/rest/v1`

      const headers = {
        apikey: env.SUPABASE_ANON_KEY,
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json"
      }

      // --------------------------------
      // HEALTH CHECK
      // --------------------------------

      if (url.pathname === "/") {

        return Response.json({
          system: "India20Sixty",
          status: "running"
        })

      }

      // --------------------------------
      // LIST TOPICS
      // --------------------------------

      if (url.pathname === "/topics") {

        const res = await fetch(
          `${base}/topics?select=*&limit=20`,
          { headers }
        )

        const data = await res.json()

        return Response.json(data)

      }

      // --------------------------------
      // GET NEXT TOPIC
      // --------------------------------

      if (url.pathname === "/topic") {

        const res = await fetch(
          `${base}/topics?used=eq.false&limit=1`,
          { headers }
        )

        const rows = await res.json()

        if (!rows.length) {

          return Response.json({
            error: "No topics available"
          })

        }

        return Response.json(rows[0])

      }

      // --------------------------------
      // CREATE JOB
      // --------------------------------

      if (url.pathname === "/create-job") {

        const topicRes = await fetch(
          `${base}/topics?used=eq.false&limit=1`,
          { headers }
        )

        const rows = await topicRes.json()

        if (!rows.length) {

          return Response.json({
            error: "No topics left"
          })

        }

        const topic = rows[0]

        const jobRes = await fetch(
          `${base}/jobs`,
          {
            method: "POST",
            headers: {
              ...headers,
              Prefer: "return=representation"
            },
            body: JSON.stringify({
              topic: topic.topic,
              cluster: topic.cluster,
              status: "job_created"
            })
          }
        )

        const job = await jobRes.json()

        await fetch(
          `${base}/topics?id=eq.${topic.id}`,
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({ used: true })
          }
        )

        return Response.json(job[0])

      }

      // --------------------------------
      // LIST JOBS
      // --------------------------------

      if (url.pathname === "/jobs") {

        const res = await fetch(
          `${base}/jobs?select=*&order=id.desc`,
          { headers }
        )

        const data = await res.json()

        return Response.json(data)

      }

      // --------------------------------
      // SCRIPT GENERATOR
      // --------------------------------

      if (url.pathname === "/script") {

        const topicRes = await fetch(
          `${base}/topics?used=eq.false&limit=1`,
          { headers }
        )

        const rows = await topicRes.json()

        if (!rows.length) {

          return Response.json({
            error: "No topics available"
          })

        }

        const topic = rows[0]

        const hooks = [

          `Socho agar ${topic.topic} reality ban jaye...`,

          `Sach bataun... ${topic.topic} India mein possible hai`,

          `2035 tak ${topic.topic} common ho sakta hai`,

          `Kya India ${topic.topic} ke liye ready hai?`

        ]

        const hook =
          hooks[Math.floor(Math.random() * hooks.length)]

        const script = {

          topic: topic.topic,

          hook,

          trend:
            "India mein technology rapidly evolve ho rahi hai.",

          insight:
            `${topic.topic} jaise innovations already research stage mein hain.`,

          future:
            "2060 tak ye system India ke millions logon ki life change kar sakta hai.",

          question:
            "Aapko kya lagta hai — kya India ready hoga?"

        }

        return Response.json(script)

      }

      // --------------------------------
      // IMAGE PROMPTS
      // --------------------------------

      if (url.pathname === "/prompts") {

        const topicRes = await fetch(
          `${base}/topics?used=eq.false&limit=1`,
          { headers }
        )

        const rows = await topicRes.json()

        if (!rows.length) {

          return Response.json({
            error: "No topics available"
          })

        }

        const topic = rows[0].topic

        const style =
          "futuristic India, advanced technology, cinematic lighting, ultra realistic, blue neon accents"

        const prompts = [

          `futuristic Indian city skyline sunrise, ${style}`,

          `AI hospital system treating patients in India, ${style}`,

          `robotic technology assisting humans India, ${style}`,

          `India 2060 smart megacity infrastructure, ${style}`,

          `wide cinematic futuristic India skyline sunset, ${style}`

        ]

        return Response.json({
          topic,
          prompts
        })

      }

      // --------------------------------
      // FALLBACK
      // --------------------------------

      return new Response("India20Sixty Worker")

    }

    catch (error) {

      return Response.json({
        error: error.message,
        stack: error.stack
      })

    }

  }

}
