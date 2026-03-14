const nse = require("./nse")
const bse = require("./bse")
const yahoo = require("./yahoo")

async function main() {
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
}

main().catch(err => {
  console.error("Master Scraper encountered a fatal error:", err)
  process.exit(1)
})