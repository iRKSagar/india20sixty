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
      // ROOT
      // --------------------------------

      if (url.pathname === "/") {

        return Response.json({
          system: "India20Sixty",
          status: "Worker running"
        })

      }

      // --------------------------------
      // LIST TOPICS
      // --------------------------------

      if (url.pathname === "/topics") {

        const res = await fetch(
          `${base}/topics?select=*&limit=10`,
          { headers }
        )

        const data = await res.json()

        return Response.json(data)

      }

      // --------------------------------
      // GET NEXT UNUSED TOPIC
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

        const topic = rows[0]

        await fetch(
          `${base}/topics?id=eq.${topic.id}`,
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({
              used: true
            })
          }
        )

        return Response.json(topic)

      }

      // --------------------------------
      // CREATE JOB
      // --------------------------------

      if (url.pathname === "/create-job") {

        const topicRes = await fetch(
          `${base}/topics?used=eq.false&limit=1`,
          { headers }
        )

        const topics = await topicRes.json()

        if (!topics.length) {

          return Response.json({
            error: "No topics left"
          })

        }

        const topic = topics[0]

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
              status: "topic_generated"
            })
          }
        )

        const job = await jobRes.json()

        await fetch(
          `${base}/topics?id=eq.${topic.id}`,
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({
              used: true
            })
          }
        )

        return Response.json(job)

      }

      // --------------------------------
      // LIST JOBS
      // --------------------------------

      if (url.pathname === "/jobs") {

        const res = await fetch(
          `${base}/jobs?select=*`,
          { headers }
        )

        const data = await res.json()

        return Response.json(data)

      }

      // --------------------------------
      // SCRIPT GENERATOR
      // --------------------------------

      if (url.pathname === "/script") {

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

        const topic = rows[0]

        const hooks = [
          `Socho agar ${topic.topic} reality ban jaye…`,
          `Sach bataun… ${topic.topic} India mein possible hai`,
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
      // VISUAL PROMPT GENERATOR
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

        const baseStyle =
          "futuristic India, advanced technology, cinematic lighting, ultra realistic, blue neon accents"

        const prompts = [

          `Indian futuristic city skyline sunrise, ${baseStyle}`,

          `AI medical system operating in Indian hospital, ${baseStyle}`,

          `robotic technology assisting humans in India, ${baseStyle}`,

          `India 2060 futuristic megacity infrastructure, ${baseStyle}`,

          `wide cinematic shot futuristic India skyline sunset, ${baseStyle}`

        ]

        return Response.json({
          topic,
          prompts
        })

      }

      // --------------------------------
      // FALLBACK
      // --------------------------------

      return new Response("India20Sixty API")

    }

    catch (error) {

      return Response.json({
        error: error.message,
        stack: error.stack
      })

    }

  }

}
