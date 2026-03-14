require("dotenv").config()
const fetch = globalThis.fetch
const { createClient } = require("@supabase/supabase-js")

/* ---------------- SUPABASE ---------------- */

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

/* ---------------- BSE API ---------------- */

const API_URL = "https://api.bseindia.com/BseIndiaAPI/api/CorpAction/w"

/* ---------------- HEADERS ---------------- */

const getHeaders = () => ({
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  Accept: "application/json, text/plain, */*",
  Referer: "https://www.bseindia.com/corporates/corporate_act.aspx",
  Origin: "https://www.bseindia.com",
  "Accept-Language": "en-US,en;q=0.9",
  Connection: "keep-alive"
})

/* ---------------- GET BSE COOKIE ---------------- */

async function getBseCookie() {
  const res = await fetch("https://www.bseindia.com/", {
    headers: getHeaders()
  })

  const cookie = res.headers.get("set-cookie")

  if (!cookie) return ""

  return cookie.split(";")[0]
}

/* ---------------- DATE RANGE ---------------- */

function getDateRange() {
  const to = new Date()
  const from = new Date()
  from.setDate(to.getDate() - 30)

  const format = d =>
    `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(
      d.getDate()
    ).padStart(2, "0")}`

  return {
    from: format(from),
    to: format(to)
  }
}

/* ---------------- DATE PARSER ---------------- */

function parseDate(dateStr) {
  if (!dateStr || dateStr === "-") return null

  // DD/MM/YYYY
  if (dateStr.includes("/")) {
    const [day, month, year] = dateStr.split("/")
    return `${year}-${month}-${day}`
  }

  // 09 Mar 2026
  const d = new Date(dateStr)
  if (!isNaN(d)) {
    const y = d.getFullYear()
    const m = String(d.getMonth() + 1).padStart(2, "0")
    const day = String(d.getDate()).padStart(2, "0")
    return `${y}-${m}-${day}`
  }

  return null
}

/* ---------------- ACTION TYPE ---------------- */

function parseAction(purpose) {
  if (!purpose) return "OTHER"

  const p = purpose.toLowerCase()

  if (p.includes("dividend")) return "DIVIDEND"
  if (p.includes("bonus")) return "BONUS"
  if (p.includes("split")) return "SPLIT"
  if (p.includes("rights")) return "RIGHTS"

  return "OTHER"
}

/* ---------------- DIVIDEND AMOUNT ---------------- */

function extractDividendAmount(purpose) {
  if (!purpose) return null;

  const match = purpose.match(/(?:rs|re)[\.\s-]*([0-9]+(?:\.[0-9]+)?)/i);

  return match ? parseFloat(match[1]) : null;
}

/* ---------------- RATIO ---------------- */

function extractRatio(purpose) {
  if (!purpose) return null

  const match = purpose.match(/(\d+:\d+)/)

  return match ? match[1] : null
}

/* ---------------- RECORD DATE ---------------- */

function extractRecordDate(bcPeriod, exDate) {
  if (!bcPeriod || bcPeriod === "-") return exDate

  // Format: "16/03/2026-16/03/2026" or "16/03/2026"
  const parts = bcPeriod.split("-")
  const dateStr = parts[parts.length - 1].trim()

  return parseDate(dateStr) || exDate
}

/* ---------------- FETCH PORTFOLIO SYMBOLS ---------------- */

async function getSymbolsFromTransactions() {
  let allStockNames = []
  let from = 0
  const step = 1000

  while (true) {
    const { data: transData, error: transError } = await supabase
      .from("stock_transactions")
      .select("stock_name")
      .is("sell_date", null)
      .range(from, from + step - 1)

    if (transError) {
      console.log("Error loading symbols:", transError.message)
      return new Set()
    }

    if (!transData.length) break

    allStockNames.push(...transData.map(t => t.stock_name))
    if (transData.length < step) break
    from += step
  }

  return new Set(allStockNames.filter(Boolean).map(n => n.toUpperCase()))
}

/* ---------------- FETCH BSE CORPORATE ACTIONS ---------------- */

async function fetchBSEActions() {
  const cookie = await getBseCookie()

  const range = getDateRange()

  console.log("Fetching BSE actions", range)

  const all = []

  let page = 1
  let fetchMore = true

  while (fetchMore) {
    const url = `${API_URL}?scripcode=&fromDt=${range.from}&toDt=${range.to}&period=Selected&expandable=0&pageno=${page}`

    try {
      const res = await fetch(url, {
        headers: {
          ...getHeaders(),
          Cookie: cookie
        }
      })

      const text = await res.text()

      if (text.includes("Access Denied")) {
        console.log("BSE blocked request")
        break
      }

      const data = JSON.parse(text)

      if (!Array.isArray(data) || data.length === 0) {
        fetchMore = false
        break
      }

      console.log(`Page ${page} records:`, data.length)

      all.push(...data)

      page++

      if (page > 10) fetchMore = false
    } catch (err) {
      console.log("Fetch error:", err.message)
      break
    }
  }

  return all
}

/* ---------------- SAVE TO SUPABASE ---------------- */

async function saveToSupabase(records) {

  const map = new Map()

  records.forEach(item => {

    const actionType = parseAction(item.Purpose)
    if (actionType === "OTHER") return

    const symbol = "BOM:" + String(item.Code)
    const exDate = parseDate(item.ExDate)

    const key = `${symbol}-${actionType}-${exDate}`

    const record = {
      symbol,
      company_name: item.Security,
      action_type: actionType,
      purpose: item.Purpose,
      ex_date: exDate,
      record_date: extractRecordDate(item.BCPeriod, exDate),
      dividend_amount:
        actionType === "DIVIDEND"
          ? extractDividendAmount(item.Purpose)
          : null,
      ratio: extractRatio(item.Purpose),
      source: "BSE"
    }

    map.set(key, record)
  })

  const clean = Array.from(map.values())

  if (clean.length === 0) {
    console.log("No valid records")
    return
  }

  const { error } = await supabase
    .from("corporate_actions")
    .upsert(clean, { 
  onConflict: "symbol,action_type,ex_date,source",
  ignoreDuplicates: false
})

  if (error) console.log("Insert error:", error.message)
  else console.log("Saved records:", clean.length)
}

/* ---------------- MAIN ---------------- */

async function main() {
  console.log("Starting BSE corporate action scraper...")

  const [symbols, actions] = await Promise.all([
    getSymbolsFromTransactions(),
    fetchBSEActions()
  ])

  console.log("Total BSE records:", actions.length)

  const filtered = actions.filter(a =>
    symbols.has((a.Security || "").toUpperCase())
  )

  console.log("Filtered records:", filtered.length)

  if (filtered.length > 0) {
    await saveToSupabase(filtered)
  }

  console.log("BSE scraping finished")
}

module.exports = { run: main }

if (require.main === module) {
  main()
}