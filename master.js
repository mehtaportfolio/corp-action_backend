const express = require("express")
const cors = require("cors")
const nse = require("./nse")
const bse = require("./bse")
const yahoo = require("./yahoo")

const app = express()
const PORT = process.env.PORT || 3000

// Enable CORS for frontend access
app.use(cors({
  origin: process.env.FRONTEND_URL || "*",
  methods: ["GET", "POST"]
}))
app.use(express.json())

async function runScraper() {
  const startTime = Date.now()
  console.log("=== Starting Master Scraper ===")

  try {
    console.log("\n--- Running NSE Scraper ---")
    await nse.run()
  } catch (err) {
    console.error("NSE Scraper failed:", err.message)
  }

  try {
    console.log("\n--- Running BSE Scraper ---")
    await bse.run()
  } catch (err) {
    console.error("BSE Scraper failed:", err.message)
  }

  try {
    console.log("\n--- Running Yahoo Scraper ---")
    await yahoo.run()
  } catch (err) {
    console.error("Yahoo Scraper failed:", err.message)
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2)
  console.log(`\n=== Master Scraper Finished in ${duration}s ===`)
  return duration
}

// Health check endpoint
app.get("/health", (req, res) => {
  res.status(200).json({ status: "ok", message: "Service is healthy" })
})

// Trigger endpoint
app.post("/trigger", async (req, res) => {
  try {
    // Run scraper in background or await if short enough
    // For Render web services, long-running requests might timeout, 
    // but we can return early or await.
    const duration = await runScraper()
    res.status(200).json({ 
      success: true, 
      message: "Scraping completed", 
      duration: `${duration}s` 
    })
  } catch (err) {
    console.error("Manual trigger failed:", err)
    res.status(500).json({ success: false, message: err.message })
  }
})

app.listen(PORT, () => {
  console.log(`Master scraper server listening on port ${PORT}`)
})
