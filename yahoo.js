require("dotenv").config()
const { createClient } = require("@supabase/supabase-js")
const YahooFinance = require("yahoo-finance2").default
const yahooFinance = new YahooFinance({ validation: { logErrors: false } })

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

/* ---------------- DATE RANGE ---------------- */
function getDateRange() {
  const today = new Date()
  const lastMonth = new Date()
  lastMonth.setDate(today.getDate() - 30)
  const format = d => d.toISOString().split("T")[0] // YYYY-MM-DD
  return { from: format(lastMonth), to: format(today) }
}

/* ---------------- PARSERS ---------------- */
function parseDate(dateStr) {
  if (!dateStr) return null
  return dateStr.split("T")[0] // YYYY-MM-DD
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
      return []
    }

    if (!transData.length) break

    allStockNames.push(...transData.map(t => t.stock_name))
    if (transData.length < step) break
    from += step
  }

  const uniqueStockNames = Array.from(new Set(allStockNames.filter(Boolean)))
  if (uniqueStockNames.length === 0) return []

  // 2. Fetch symbols from master that match these names
  const { data: masterData, error: masterError } = await supabase
    .from("stock_master")
    .select("symbol, stock_name")
    .in("stock_name", uniqueStockNames)

  if (masterError) {
    console.error("Error fetching master symbols:", masterError.message)
    return {}
  }

  const symbolMap = {}
  masterData.forEach(m => {
    symbolMap[m.symbol] = m.stock_name
  })
  return symbolMap
}

/* ---------------- FETCH YAHOO (SINGLE SYMBOL) ---------------- */
async function fetchSingleSymbol(masterSymbol, stockName, from, to) {
  const parts = masterSymbol.split(":")
  const prefix = parts.length > 1 ? parts[0] : null
  const cleanSymbol = parts.length > 1 ? parts[1] : parts[0]
  
  let yahooSymbol = null
  if (prefix === "NSE") {
    yahooSymbol = cleanSymbol + ".NS"
  } else if (prefix === "BOM") {
    yahooSymbol = cleanSymbol + ".BO"
  } else {
    // Guess based on symbol format (Numeric = BOM, Alpha = NSE)
    if (/^\d+$/.test(cleanSymbol)) {
      yahooSymbol = cleanSymbol + ".BO"
    } else {
      yahooSymbol = cleanSymbol + ".NS"
    }
  }

  try {
    const result = await yahooFinance.chart(yahooSymbol, {
      period1: from,
      period2: to,
      events: "div|split"
    })

    if (!result.events) return []

    const records = []
    if (result.events.dividends) {
      result.events.dividends.forEach(item => {
        records.push({
          symbol: masterSymbol,
          company_name: stockName,
          action_type: "DIVIDEND",
          purpose: "Dividend",
          ex_date: parseDate(item.date.toISOString()),
          record_date: null,
          dividend_amount: item.amount,
          ratio: null,
          source: "YAHOO"
        })
      })
    }

    if (result.events.splits) {
      result.events.splits.forEach(item => {
        records.push({
          symbol: masterSymbol,
          company_name: stockName,
          action_type: "SPLIT",
          purpose: "Stock Split",
          ex_date: parseDate(item.date.toISOString()),
          record_date: null,
          dividend_amount: null,
          ratio: item.splitRatio,
          source: "YAHOO"
        })
      })
    }
    return records
  } catch (err) {
    // If rate limited, log it but keep going
    if (err.name === 'HTTPError' && err.response && err.response.status === 429) {
      console.warn(`Yahoo rate limited for ${yahooSymbol}`)
    }
    return []
  }
}

/* ---------------- FETCH YAHOO (PARALLEL WORKER POOL) ---------------- */
async function fetchYahooActions(symbolMap, bonusMap) {
  const masterSymbols = Object.keys(symbolMap)
  const { from, to } = getDateRange()
  const CONCURRENCY = 500 // Ultra super fast
  const BATCH_SAVE_SIZE = 250
  let totalSaved = 0
  let pendingRecords = []
  let currentIndex = 0

  const worker = async () => {
    while (currentIndex < masterSymbols.length) {
      const sym = masterSymbols[currentIndex++]
      if (!sym) continue
      
      const stockName = symbolMap[sym]
      const records = await fetchSingleSymbol(sym, stockName, from, to)
      if (records.length) {
        // Filter SPLIT against BONUS
        const filtered = records.filter(rec => {
          if (rec.action_type === "SPLIT") {
            const key = `${rec.symbol}_${rec.ex_date}`
            return !bonusMap.has(key)
          }
          return true
        })

        if (filtered.length) {
          pendingRecords.push(...filtered)
        }
      }

      // Buffer save
      if (pendingRecords.length >= BATCH_SAVE_SIZE) {
        const toSave = [...pendingRecords]
        pendingRecords = []
        totalSaved += await saveToSupabase(toSave)
      }
    }
  }

  // Launch workers
  const workers = Array.from({ length: Math.min(CONCURRENCY, masterSymbols.length) }, () => worker())

  await Promise.all(workers)

  // Final flush
  if (pendingRecords.length > 0) {
    totalSaved += await saveToSupabase(pendingRecords)
  }

  return totalSaved
}

/* ---------------- SAVE TO SUPABASE ---------------- */
async function saveToSupabase(records) {
  if (!records.length) return 0

  const { error } = await supabase
    .from("corporate_actions")
    .upsert(records, { onConflict: "symbol,action_type,ex_date,source" })

  if (error) {
    console.error("Insert error:", error.message)
    return 0
  }
  return records.length
}

/* ---------------- MAIN ---------------- */
async function main() {
  const startTime = Date.now()
  console.log("Starting Yahoo Finance fallback scraper...")

  const { from, to } = getDateRange()
  
  // 1. Fetch symbols and existing bonus records in parallel
  const [symbolMap, { data: existingBonus }] = await Promise.all([
    getSymbolsFromTransactions(),
    supabase
      .from("corporate_actions")
      .select("symbol, ex_date")
      .eq("action_type", "BONUS")
      .gte("ex_date", from)
  ])

  if (!Object.keys(symbolMap).length) {
    console.log("No symbols to process.")
    return
  }

  const bonusMap = new Set((existingBonus || []).map(b => `${b.symbol}_${b.ex_date}`))
  
  // 2. Process with worker pool and incremental saving
  const savedCount = await fetchYahooActions(symbolMap, bonusMap)

  const duration = ((Date.now() - startTime) / 1000).toFixed(2)
  console.log(`${Object.keys(symbolMap).length} symbols processed and ${savedCount} records updated in ${duration}s`)
  console.log("Yahoo fallback scraping complete")
}

module.exports = { run: main }

if (require.main === module) {
  main()
}