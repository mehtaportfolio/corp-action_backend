require("dotenv").config()
const axios = require("axios")
const { createClient } = require("@supabase/supabase-js")

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const BASE_URL = "https://www.nseindia.com"

const headers = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
  Accept: "application/json",
  "Accept-Language": "en-US,en;q=0.9",
  Connection: "keep-alive"
}

function getDateRange() {

  const today = new Date()

  const nextMonth = new Date()
  nextMonth.setDate(today.getDate() + 30)

  const format = (d) => {
    const day = String(d.getDate()).padStart(2, "0")
    const month = String(d.getMonth() + 1).padStart(2, "0")
    const year = d.getFullYear()
    return `${day}-${month}-${year}`
  }

  return {
    from: format(today),
    to: format(nextMonth)
  }
}

async function getCookies() {
  const res = await axios.get(BASE_URL, { headers })

  const cookies = res.headers["set-cookie"]

  if (!cookies) return ""

  return cookies.map(c => c.split(";")[0]).join("; ")
}

function parseAction(purpose) {

  if (!purpose) return "OTHER"

  const p = purpose.toLowerCase()

  if (p.includes("dividend")) return "DIVIDEND"
  if (p.includes("bonus")) return "BONUS"
  if (p.includes("split")) return "SPLIT"
  if (p.includes("rights")) return "RIGHTS"

  return "OTHER"
}

function parseDate(dateStr) {

  if (!dateStr || dateStr === "-") return null

  const months = {
    Jan: "01",
    Feb: "02",
    Mar: "03",
    Apr: "04",
    May: "05",
    Jun: "06",
    Jul: "07",
    Aug: "08",
    Sep: "09",
    Oct: "10",
    Nov: "11",
    Dec: "12"
  }

  const parts = dateStr.split("-")

  return `${parts[2]}-${months[parts[1]]}-${parts[0]}`
}

function extractDividendAmount(purpose) {

  if (!purpose) return null

  const match = purpose.match(/rs\s*([0-9]*\.?[0-9]+)/i)
  if (match) return parseFloat(match[1])

  const matchRe = purpose.match(/re\s*([0-9]*\.?[0-9]+)/i)
  if (matchRe) return parseFloat(matchRe[1])

  return null
}

function extractRatio(purpose) {

  if (!purpose) return null

  const match = purpose.match(/(\d+:\d+)/)
  if (match) return match[1]

  const faceValueMatch = purpose.match(/From\s+R[se]\.?\s*(\d+).*?To\s+R[se]\.?\s*(\d+)/i)
  if (faceValueMatch) {
    const from = parseInt(faceValueMatch[1])
    const to = parseInt(faceValueMatch[2])
    if (to > 0) {
      return `${from / to}:1`
    }
  }

  return null
}

/* ---------------- FETCH SYMBOLS FROM TRANSACTIONS ---------------- */
async function getSymbolsFromTransactions() {
  // 1. Fetch unique stock names for open positions (sell_date is null)
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
      console.error("Error fetching transactions:", transError.message)
      return new Set()
    }

    if (!transData.length) break

    allStockNames.push(...transData.map(t => t.stock_name))
    if (transData.length < step) break
    from += step
  }

  const uniqueStockNames = Array.from(new Set(allStockNames.filter(Boolean)))
  if (uniqueStockNames.length === 0) return new Set()

  // 2. Fetch symbols from master that match these names
  const { data: masterData, error: masterError } = await supabase
    .from("stock_master")
    .select("symbol")
    .in("stock_name", uniqueStockNames)

  if (masterError) {
    console.error("Error fetching master symbols:", masterError.message)
    return new Set()
  }

  // NSE symbols in stock_master are usually "NSE:SYMBOL". NSE API returns "SYMBOL".
  const symbols = new Set()
  masterData.forEach(m => {
    const [prefix, sym] = m.symbol.split(":")
    if (prefix === "NSE") {
      symbols.add(sym || prefix)
    } else {
      symbols.add(m.symbol)
    }
  })

  return symbols
}

async function fetchNSEActions() {

  try {

    console.log("Getting NSE cookies...")

    const cookies = await getCookies()

    const { from, to } = getDateRange()

    console.log("Fetching actions from", from, "to", to)

    const API_URL =
      `https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date=${from}&to_date=${to}`

    const response = await axios.get(API_URL, {
      headers: {
        ...headers,
        Referer: BASE_URL,
        Cookie: cookies
      }
    })

    return response.data

  } catch (err) {

    console.error("NSE fetch error:", err.message)
    return []

  }
}

async function saveToSupabase(records) {

  const cleanRecords = records.map(item => {

    const purpose = item.subject || ""

    const actionType = parseAction(purpose)
    
    if (actionType === "OTHER") return null

    return {
      symbol: "NSE:" + item.symbol,
      stock_name: item.symbol,
      company_name: item.comp,
      action_type: actionType,
      purpose: purpose,
      ex_date: parseDate(item.exDate),
      record_date: parseDate(item.recDate),
      dividend_amount:
        actionType === "DIVIDEND"
          ? extractDividendAmount(purpose)
          : null,
      ratio: extractRatio(purpose),
      source: "NSE"
    }
  }).filter(Boolean)

  const { error } = await supabase
    .from("corporate_actions")
    .upsert(cleanRecords, {
      onConflict: "symbol,action_type,ex_date,source"
    })

  if (error) {
    console.log("Insert error:", error.message)
  } else {
    console.log("Saved records:", cleanRecords.length)
  }
}

async function main() {

  console.log("Starting NSE corporate action scraper...")

  // 1. Fetch symbols from transactions and NSE actions in parallel
  const [targetSymbols, actions] = await Promise.all([
    getSymbolsFromTransactions(),
    fetchNSEActions()
  ])

  if (!actions || actions.length === 0) {
    console.log("No data received from NSE")
    return
  }

  console.log("Records fetched:", actions.length)

  // 2. Filter actions to only include target symbols
  const filteredActions = actions.filter(a => targetSymbols.has(a.symbol))
  console.log("Records after filtering:", filteredActions.length)

  if (filteredActions.length > 0) {
    await saveToSupabase(filteredActions)
  }

  console.log("NSE scraping complete")
}

module.exports = { run: main }

if (require.main === module) {
  main()
}