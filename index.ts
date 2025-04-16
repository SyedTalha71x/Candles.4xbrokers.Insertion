import pkg from "pg";
const { Pool } = pkg;
import Bull from "bull";
import { configDotenv } from "dotenv";
import candlesLogger from "./logger";

configDotenv();

const CENTROID_HOST = process.env.CENTROID_PG_HOST;
const CENTROID_PORT = process.env.CENTROID_PG_PORT ? Number(process.env.CENTROID_PG_PORT) : 5432;
const CENTROID_USER = process.env.CENTROID_PG_USER;
const CENTROID_PASSWORD = process.env.CENTROID_PG_PASSWORD;
const CENTROID_DATABASE = process.env.CENTROID_PG_DATABASE;

const LPPIME_HOST = process.env.PG_HOST;
const LPPIME_PORT = process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432;
const LPPIME_USER = process.env.PG_USER;
const LPPIME_PASSWORD = process.env.PG_PASSWORD;
const LPPIME_DATABASE = process.env.PG_DATABASE;

const REDIS_HOST = process.env.REDIS_HOST || "3.82.229.23";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379");
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

const timeFrames: Record<string, number> = {
  M1: 60000,
  H1: 3600000,
  D1: 86400000,
};

// Interfaces
interface TickData {
  symbol: string;
  price: number;
  timestamp: Date;
  lots: number;
  type: "BID" | "ASK";
}

interface CurrencyPairInfo {
  currpair: string;
  contractsize: number | null;
}

const centroidPool = new Pool({
  host: CENTROID_HOST,
  port: CENTROID_PORT,
  user: CENTROID_USER,
  password: CENTROID_PASSWORD,
  database: CENTROID_DATABASE,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const lppimePool = new Pool({
  host: LPPIME_HOST,
  port: LPPIME_PORT,
  user: LPPIME_USER,
  password: LPPIME_PASSWORD,
  database: LPPIME_DATABASE,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Bull Queues
const marketDataQueue = new Bull("marketData", {
  redis: {
    host: REDIS_HOST,
    port: REDIS_PORT,
    password: REDIS_PASSWORD,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: true,
    timeout: 30000,
  },
  limiter: {
    max: 100,
    duration: 1000,
  },
  settings: {
    maxStalledCount: 1,
  },
});

const candleProcessingQueue = new Bull("candleProcessing", {
  redis: {
    host: REDIS_HOST,
    port: REDIS_PORT,
    password: REDIS_PASSWORD,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: true,
    timeout: 30000,
  },
  limiter: {
    max: 50,
    duration: 1000,
  },
});

let availableCurrencyPairs: CurrencyPairInfo[] = [];
const subscribedPairs: Set<string> = new Set();

async function pgListener() {
  const client = await centroidPool.connect();
  // console.log("Connected to Centroid PostgreSQL for notifications");

  client.on("notification", async (msg) => {
    if (msg.channel === 'tick') {
      try {
        candlesLogger.info(`Received notification from Centroid: ${msg.payload}`);       
        const [symbol, lotsStr, bora, priceStr, timestampStr] = (msg.payload ?? "").split(" ");
        const lots = parseInt(lotsStr);
        const price = parseFloat(priceStr);
        const timestamp = new Date(parseInt(timestampStr));

        if (isNaN(timestamp.getTime())) {
          candlesLogger.error(`Invalid timestamp received ----", ${timestampStr}`)
          console.error("Invalid timestamp received ----", timestampStr);
          return;
        }

        if (isNaN(lots) || isNaN(price)) {
          console.error("Invalid data in notification:", { lots, price });
          return;
        }

        const tickData: TickData = {
          symbol,
          price,
          timestamp,
          lots,
          type: bora === "A" ? "ASK" : "BID"
        };

        candlesLogger.info(`Processing tick data from Centroid: ${JSON.stringify(tickData)}`);
        processMarketData(tickData);
      } catch (error: any) {
        console.error("Error parsing notification payload from Centroid:", error);
        return;
      }
    }
  });

  await client.query("LISTEN tick");
  // console.log("Listening for notifications on Centroid channel 'tick'...");
}

function processMarketData(data: TickData) {
  marketDataQueue.add(data)
}

async function initDatabase() {
  try {
    await fetchAllCurrencyPairs();
    return true;
  } catch (error) {
    console.error("Lppime database init failed:", error);
    throw error;
  }
}

async function fetchAllCurrencyPairs() {
  try {
    const result = await lppimePool.query(
      "SELECT currpair, contractsize FROM currpairdetails"
    );
    availableCurrencyPairs = result.rows;
    // console.log(`Found ${availableCurrencyPairs.length} currency pairs in Lppime`);
    candlesLogger.info(`Found ${availableCurrencyPairs.length} currency pairs in Lppime`);

    const validPairs = availableCurrencyPairs.filter(
      (pair) => pair.contractsize !== null
    );
    // console.log(`Found ${validPairs.length} valid pairs with contract size`);
    candlesLogger.info(`Found ${validPairs.length} valid pairs with contract size`);

    validPairs.forEach((pair) => {
      subscribedPairs.add(pair.currpair);
    });

    for (const pair of validPairs) {
      const tableNameBid = `ticks_${pair.currpair.toLowerCase()}_bid`;
      const tableNameAsk = `ticks_${pair.currpair.toLowerCase()}_ask`;
      const candleTableName = `candles_${pair.currpair.toLowerCase()}_bid`;

      await ensureTableExists(tableNameBid);
      await ensureTableExists(tableNameAsk);
      await ensureCandleTableExists(candleTableName);
    }
  } catch (error) {
    console.error("Error fetching currency pairs from Lppime:", error);
    throw error;
  }
}

async function ensureTableExists(tableName: string): Promise<void> {
  try {
    const tableCheck = await lppimePool.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = $1
      )`,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await lppimePool.query(`
        CREATE TABLE ${tableName} (
          ticktime TIMESTAMP WITH TIME ZONE NOT NULL,
          lots INTEGER PRIMARY KEY,
          price NUMERIC NOT NULL
        )
      `);
      candlesLogger.info(`Created table ${tableName} in Lppime`);
      // console.log(`Created table ${tableName} in Lppime`);
    }
  } catch (error) {
    console.error(`Error ensuring table ${tableName} exists in Lppime:`, error);
    throw error;
  }
}

async function ensureCandleTableExists(tableName: string): Promise<void> {
  try {
    const tableCheck = await lppimePool.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = $1
      )`,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await lppimePool.query(`
        CREATE TABLE ${tableName} (
          candlesize TEXT NOT NULL,
          lots SMALLINT NOT NULL,
          candletime TIMESTAMP WITH TIME ZONE NOT NULL,
          open NUMERIC(12,5) NOT NULL,
          high NUMERIC(12,5) NOT NULL,
          low NUMERIC(12,5) NOT NULL,
          close NUMERIC(12,5) NOT NULL,
          PRIMARY KEY (candlesize, lots, candletime)
        )
      `);
      candlesLogger.info(`Created candle table ${tableName} in Lppime`);
      // console.log(`Created candle table ${tableName} in Lppime`);
    }
  } catch (error) {
    console.error(`Error ensuring candle table ${tableName} exists in Lppime:`, error);
    throw error;
  }
}

marketDataQueue.process(5, async (job) => {
  try {
    const data = job.data;

    if (!data.symbol || !data.price || !data.lots) {
      throw new Error(`Invalid market data: ${JSON.stringify(data)}`);
    }

    let lots = parseInt(data.lots.toString());
    let ticktime: Date;

    if (data.timestamp instanceof Date) {
      ticktime = data.timestamp;
    } else if (data.timestamp) {
      ticktime = new Date(data.timestamp);
    } else {
      ticktime = new Date();
    }

    if (isNaN(ticktime.getTime())) {
      throw new Error(`Invalid timestamp received: ${data.timestamp}`);
    }

    let tableName: string;
    const symbolLower = data.symbol.toLowerCase();

    if (data.type === "BID") {
      tableName = `ticks_${symbolLower}_bid`;
    } else {
      tableName = `ticks_${symbolLower}_ask`;
    }


    const insertQuery = await lppimePool.query({
      text: `
        INSERT INTO ${tableName} 
        (ticktime, lots, price)
        VALUES ($1, $2, $3)
        ON CONFLICT (lots) DO NOTHING
        RETURNING *;
      `,
      values: [
        ticktime.toISOString(),
        lots,
        parseFloat(data.price.toString())
      ],
    });

    if (insertQuery.rowCount !== null && insertQuery.rowCount > 0) {
      candlesLogger.info(`Successfully inserted tick for ${data.symbol} into Lppime ${tableName}`);
      // console.log(`Successfully inserted tick for ${data.symbol} into Lppime ${tableName}`);
    } else {
      candlesLogger.info(`Tick for ${data.symbol} with lots ${lots} already exists in Lppime ${tableName}`);
      // console.log(`Tick for ${data.symbol} with lots ${lots} already exists in Lppime ${tableName}`);
    }

    if (data.type === "BID") {
      await processTickForCandles({
        symbol: data.symbol,
        price: data.price,
        timestamp: ticktime,
        lots: lots,
        type: data.type
      });
    }

    return { success: true, symbol: data.symbol, type: data.type };
  } catch (error) {
    console.error(`Error processing market data job ${job.id} for ${job.data?.symbol}:`, error);
    throw error;
  }
});

async function processTickForCandles(tickData: TickData) {
  try {
    const jobId = `candle_${tickData.symbol}_${Date.now()}`;

    await candleProcessingQueue.add(
      {
        tickData,
        timeFrames: Object.keys(timeFrames),
      },
      {
        jobId,
      }
    );
  } catch (error) {
    console.error("Error adding tick to candle processing queue:", error);
    throw error;
  }
}

candleProcessingQueue.process(async (job) => {
  const { tickData, timeFrames } = job.data;
  const { symbol, price, timestamp } = tickData;

  if (!tickData || !timeFrames || !symbol || !price || timestamp == null) {
    candlesLogger.error(`Incomplete job data: ${JSON.stringify(job.data)}`);
    throw new Error("Invalid job data");
  }


  const lots = 1;
  const tableName = `candles_${symbol.toLowerCase()}_bid`;

  await ensureCandleTableExists(tableName);

  let processedTimestamp: Date;

  if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
    processedTimestamp = timestamp;
  } else if (typeof timestamp === 'string' || typeof timestamp === 'number') {
    // Try parsing as milliseconds first
    processedTimestamp = new Date(timestamp);

    // If invalid, try parsing as seconds (common Unix timestamp format)
    if (isNaN(processedTimestamp.getTime())) {
      const numTimestamp = Number(timestamp);
      if (!isNaN(numTimestamp)) {
        processedTimestamp = new Date(
          numTimestamp > 1000000000000 ? numTimestamp : numTimestamp * 1000
        );
      }
    }

    if (isNaN(processedTimestamp.getTime())) {
      candlesLogger.warn(`Invalid timestamp ${timestamp} for ${symbol}, using current time`);
      processedTimestamp = new Date();
    }
  } else {
    candlesLogger.warn(`Unexpected timestamp type for ${symbol}, using current time`);
    processedTimestamp = new Date();
  }

  if (isNaN(processedTimestamp.getTime())) {
    processedTimestamp = new Date();
  }



  for (const [timeframe, duration] of Object.entries(timeFrames) as [string, number][]) {
    try {

      if (isNaN(processedTimestamp.getTime())) {
        throw new Error(`Invalid processed timestamp for ${symbol}: ${processedTimestamp}`);
      }

      if (!duration || isNaN(duration) || duration <= 0) {
        candlesLogger.error(`Invalid duration ${duration} for timeframe ${timeframe}`);
        continue;
      }

      const candleTime = new Date(
        Math.floor(processedTimestamp.getTime() / duration) * duration
      );

      if (isNaN(candleTime.getTime())) {
        throw new Error(`Invalid candle time calculation for ${symbol} ${timeframe}`);
      }


      const existingCandle = await lppimePool.query({
        text: `
          SELECT * FROM ${tableName}
          WHERE candlesize = $1 AND lots = $2 AND candletime = $3
        `,
        values: [timeframe, lots, candleTime.toISOString()],
      });

      if (existingCandle.rows.length > 0) {

        const updateResult = await lppimePool.query({
          text: `
            UPDATE ${tableName}
            SET high = GREATEST(high, $1),
                low = LEAST(low, $2),
                close = $3
            WHERE candlesize = $4
            AND lots = $5
            AND candletime = $6
            RETURNING *
          `,
          values: [
            price,
            price,
            price,
            timeframe,
            lots,
            candleTime.toISOString(),
          ],
        });
        candlesLogger.info(`Updated candle for ${symbol} in ${timeframe}`, updateResult.rows[0]);
      } else {
        const insertResult = await lppimePool.query({
          text: `
            INSERT INTO ${tableName}
            (candlesize, lots, candletime, open, high, low, close)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
          `,
          values: [
            timeframe,
            lots,
            candleTime.toISOString(),
            price, // open
            price, // high
            price, // low
            price, // close
          ],
        });

        candlesLogger.info(`Created new candle for ${symbol} in ${timeframe}`, insertResult.rows[0]);
      }
    } catch (error) {
      console.error(`Error processing ${timeframe} candle for ${symbol}:`, error);
      throw error;
    }
  }

  return { success: true, symbol };
});

async function initializeApplication() {
  try {
    candlesLogger.info('Starting Candles Server with dual database setup...');
    console.log('Starting Candles Server with dual database setup...');

    await marketDataQueue.empty();
    await candleProcessingQueue.empty();

    await initDatabase();

    await pgListener();

  } catch (error) {
    console.error("Application initialization failed:", error);
    process.exit(1);
  }
}

marketDataQueue.on("completed", (job, result) => {
  // console.log(`MarketData Job ${job.id} completed: ${result.symbol} ${result.type}`);
});

marketDataQueue.on("failed", (job, error) => {
  console.error(`MarketData Job ${job.id} failed:`, error);
});

candleProcessingQueue.on("completed", (job, result) => {
  // console.log(`CandleProcessing Job ${job.id} completed for ${result.symbol}`);
});

candleProcessingQueue.on("failed", (job, error) => {
  console.error(`CandleProcessing Job ${job.id} failed:`, error);
});

process.on("SIGINT", async () => {
  console.log("Shutting down gracefully...");

  try {
    await marketDataQueue.close();
    await candleProcessingQueue.close();

    await centroidPool.end();
    await lppimePool.end();
    process.exit(0);
  } catch (error) {
    console.error("Error during shutdown:", error);
    process.exit(1);
  }
});

initializeApplication();