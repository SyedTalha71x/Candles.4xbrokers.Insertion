import pkg, { Pool as PoolType } from "pg";
const { Pool } = pkg;
import { configDotenv } from "dotenv";
import candlesLogger from "./logger";
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import moment from "moment";

configDotenv();

const redisConnection = new IORedis({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379,
  maxRetriesPerRequest: null
});

async function cleanQueues() {
  try {
    const tickQueue = new Queue('tickQueue', { connection: redisConnection });
    const candleQueue = new Queue('candleQueue', { connection: redisConnection });

    await tickQueue.obliterate({ force: true });
    await candleQueue.obliterate({ force: true });

    candlesLogger.info("Cleaned existing queues");
  } catch (error) {
    candlesLogger.error("Error cleaning queues:", error);
  }
}

export const tickQueue = new Queue('tickQueue', {
  connection: redisConnection,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: 1000,
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000
    }
  }
});

export const candleQueue = new Queue('candleQueue', {
  connection: redisConnection,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: 1000,
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000
    }
  }
});

// Database configuration
const CENTROID_HOST = process.env.CENTROID_PG_HOST;
const CENTROID_PORT = process.env.CENTROID_PG_PORT ? Number(process.env.CENTROID_PG_PORT) : 5432;
const CENTROID_USER = process.env.CENTROID_PG_USER;
const CENTROID_PASSWORD = process.env.CENTROID_PG_PASSWORD;
const CENTROID_DATABASE = process.env.CENTROID_PG_DATABASE;

const LPPIME_HOST = process.env.LPPRIME_PG_HOST;
const LPPIME_PORT = process.env.LPPRIME_PG_PORT ? Number(process.env.LPPRIME_PG_PORT) : 5432;
const LPPIME_USER = process.env.LPPRIME_PG_USER;
const LPPIME_PASSWORD = process.env.LPPRIME_PG_PASSWORD;
const LPPIME_DATABASE = process.env.LPPRIME_PG_DATABASE;

let isShuttingDown = false;

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

interface TickBatch {
  data: TickData[];
  lastUpdated: moment.Moment;
}

interface OrganizedBatches {
  [key: string]: {
    BID: TickBatch;
    ASK: TickBatch;
  };
}

const centroidPool = new Pool({
  host: CENTROID_HOST,
  port: CENTROID_PORT,
  user: CENTROID_USER,
  password: CENTROID_PASSWORD,
  database: CENTROID_DATABASE,
  max: 30,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 60000,
  allowExitOnIdle: true,
  maxUses: 1000,
});

const lppimePool = new Pool({
  host: LPPIME_HOST,
  port: LPPIME_PORT,
  user: LPPIME_USER,
  password: LPPIME_PASSWORD,
  database: LPPIME_DATABASE,
  max: 30,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 60000,
  allowExitOnIdle: true,
  maxUses: 1000,
});

let availableCurrencyPairs: CurrencyPairInfo[] = [];
const subscribedPairs: Set<string> = new Set();

const BATCH_SIZE = process.env.BATCH_SIZE ? Number(process.env.BATCH_SIZE) : 1000;
const BATCH_TIMEOUT_MIN = process.env.BATCH_TIMEOUT
  ? Number(process.env.BATCH_TIMEOUT) * 60 * 1000
  : 5 * 60 * 1000;

let organizedBatches: OrganizedBatches = {};
let cleanupInterval: NodeJS.Timeout | null = null;

async function cleanFailedJobs() {
  try {
    const failedTickJobs = await tickQueue.getFailed();
    const failedCandleJobs = await candleQueue.getFailed();

    if (failedTickJobs.length > 0 || failedCandleJobs.length > 0) {
      candlesLogger.info(`Cleaning ${failedTickJobs.length} failed tick jobs and ${failedCandleJobs.length} failed candle jobs`);
      await Promise.all([
        ...failedTickJobs.map(job => job.remove()),
        ...failedCandleJobs.map(job => job.remove())
      ]);
    }
  } catch (error) {
    candlesLogger.error('Error during queue cleanup:', error);
  }
}

async function processTickBatch(symbol: string, bidTicks: TickData[], askTicks: TickData[]) {
  if (bidTicks.length === 0 && askTicks.length === 0) return;

  candlesLogger.debug(`Processing tick batch for ${symbol}`, {
    bidCount: bidTicks.length,
    askCount: askTicks.length,
    firstBidTimestamp: bidTicks[0]?.timestamp.toISOString(),
    lastBidTimestamp: bidTicks[bidTicks.length - 1]?.timestamp.toISOString(),
    firstAskTimestamp: askTicks[0]?.timestamp.toISOString(),
    lastAskTimestamp: askTicks[askTicks.length - 1]?.timestamp.toISOString()
  });

  try {
    await tickQueue.add('processTickBatch', { symbol, bidTicks, askTicks });
    candlesLogger.debug(`Enqueued tick batch for ${symbol}`, {
      bidCount: bidTicks.length,
      askCount: askTicks.length
    });
  } catch (error) {
    candlesLogger.error(`Failed to enqueue tick batch for ${symbol}:`, error);
    throw error;
  }
}

async function processCandleBatch(symbol: string, bidTicks: TickData[]) {
  if (bidTicks.length === 0) return;

  candlesLogger.debug(`Processing candle batch for ${symbol}`, {
    tickCount: bidTicks.length,
    firstTimestamp: bidTicks[0]?.timestamp.toISOString(),
    lastTimestamp: bidTicks[bidTicks.length - 1]?.timestamp.toISOString()
  });

  try {
    await candleQueue.add('processCandleBatch', { symbol, bidTicks });
    candlesLogger.debug(`Enqueued candle batch for ${symbol}`, {
      tickCount: bidTicks.length
    });
  } catch (error) {
    candlesLogger.error(`Failed to enqueue candle batch for ${symbol}:`, error);
    throw error;
  }
}



async function pgListener() {
  let client;
  try {
    client = await centroidPool.connect();
    candlesLogger.info("Connected to Centroid PostgreSQL for tick notifications");

    client.on("notification", async (msg) => {
      if (msg.channel === 'tick') {
        try {
          const [symbol, lotsStr, bora, priceStr, timestampStr] = (msg.payload ?? "").split(" ");
          const lots = parseInt(lotsStr);
          const price = parseFloat(priceStr);
          const timestamp = new Date(parseInt(timestampStr));

          if (!timestamp.getTime() || isNaN(lots) || isNaN(price)) {
            candlesLogger.error("Invalid tick data received", {
              symbol,
              lots,
              price,
              timestamp: timestamp.getTime()
            });
            return;
          }

          const tickData: TickData = {
            symbol,
            price,
            timestamp,
            lots,
            type: bora === "A" ? "ASK" : "BID"
          };

          // Initialize batch if not exists
          if (!organizedBatches[symbol]) {
            organizedBatches[symbol] = {
              BID: { data: [], lastUpdated: moment() },
              ASK: { data: [], lastUpdated: moment() }
            };
          }

          // Add to appropriate batch
          const batchType = tickData.type;
          const batch = organizedBatches[symbol][batchType];
          batch.data.push(tickData);
          // console.log(batch.data.length)
          // candlesLogger.info(` batch length ---- ${batch.data.length} ${symbol} ${batchType}`)

          // Check if batch size is reached (priority condition)
          if (batch.data.length === BATCH_SIZE) {
            console.log(batch.data.length === BATCH_SIZE);
            // candlesLogger.info(`batch size lenght condition is true ${batch.data.length === BATCH_SIZE}`)
            
            const ticksToProcess = batch.data.splice(0, BATCH_SIZE);
            batch.lastUpdated = moment();

            if (batchType === "BID") {
              // Process tick and candle batches in parallel
              await Promise.all([
                processTickBatch(symbol, ticksToProcess, []),
                processCandleBatch(symbol, ticksToProcess)
              ]);
            } else {
              await processTickBatch(symbol, [], ticksToProcess);
            }
          }
          // Only check timeout if batch not processed by size
          else {
            const now = moment();
            const timeSinceLastProcess = now.diff(batch.lastUpdated, 'milliseconds');
            if (timeSinceLastProcess >= BATCH_TIMEOUT_MIN && batch.data.length > 1) {
              // console.log(timeSinceLastProcess >= BATCH_TIMEOUT_MIN && batch.data.length > 1);
              candlesLogger.info(`Condition inside timeout --- ${timeSinceLastProcess >= BATCH_TIMEOUT_MIN && batch.data.length > 1}`)
              

              const ticksToProcess = batch.data.splice(0, batch.data.length - 1);
              
              // Don't update lastUpdated until after actual processing
              if (ticksToProcess.length > 0) {
                batch.lastUpdated = moment();
          
                if (batchType === "BID") {
                  await Promise.all([
                    processTickBatch(symbol, ticksToProcess, []),
                    processCandleBatch(symbol, ticksToProcess)
                  ]);
                } else {
                  await processTickBatch(symbol, [], ticksToProcess);
                }
              }
            }
          }

        } catch (error: any) {
          candlesLogger.error("Error processing tick notification:", error);
        }
      }
    });

    client.on('error', (err) => {
      candlesLogger.error("Centroid client error:", err);
    });

    await client.query("LISTEN tick");
    candlesLogger.info("Now listening for tick notifications");

  } catch (error: any) {
    candlesLogger.error(`Centroid connection failed: ${error.message}`);
    throw error;
  }
}

async function initDatabase() {
  try {
    await fetchAllCurrencyPairs();
    return true;
  } catch (error) {
    candlesLogger.error("LPPime database init failed:", error);
    throw error;
  }
}

async function fetchAllCurrencyPairs() {
  let client;
  try {
    client = await centroidPool.connect();
    const result = await client.query(
      "SELECT currpair, contractsize FROM currpairdetails"
    );
    availableCurrencyPairs = result.rows;
    candlesLogger.info(`Found ${availableCurrencyPairs.length} currency pairs in Centroid`);

    const validPairs = availableCurrencyPairs.filter(
      (pair) => pair.contractsize !== null
    );
    candlesLogger.info(`Found ${validPairs.length} valid pairs with contract size`);

    for (const pair of validPairs) {
      subscribedPairs.add(pair.currpair);
      const tableNameBid = `ticks_${pair.currpair.toLowerCase()}_bid`;
      const tableNameAsk = `ticks_${pair.currpair.toLowerCase()}_ask`;
      const candleTableName = `candles_${pair.currpair.toLowerCase()}_bid`;

      // Create tables in both databases
      await Promise.all([
        ensureTableExists(centroidPool, tableNameBid),
        ensureTableExists(centroidPool, tableNameAsk),
        ensureCandleTableExists(centroidPool, candleTableName),
        ensureTableExists(lppimePool, tableNameBid),
        ensureTableExists(lppimePool, tableNameAsk),
        ensureCandleTableExists(lppimePool, candleTableName)
      ]);
    }
  } catch (error) {
    candlesLogger.error("Error initializing database tables:", error);
    throw error;
  } finally {
    if (client) await client.release();
  }
}

async function ensureTableExists(pool: PoolType, tableName: string): Promise<void> {
  let client;
  try {
    client = await pool.connect();
    const exists = await client.query(
      `SELECT EXISTS (SELECT FROM information_schema.tables 
       WHERE table_schema = 'public' AND table_name = $1)`,
      [tableName]
    );

    if (!exists.rows[0].exists) {
      await client.query(`
        CREATE TABLE ${tableName} (
          ticktime TIMESTAMP WITH TIME ZONE NOT NULL,
          lots INTEGER NOT NULL,
          price NUMERIC NOT NULL,
          PRIMARY KEY (lots, ticktime)
        )`);
      candlesLogger.info(`Created table ${tableName} in ${pool === centroidPool ? 'Centroid' : 'LPPime'}`);
    }
  } catch (error) {
    candlesLogger.error(`Error ensuring table ${tableName} exists in ${pool === centroidPool ? 'Centroid' : 'LPPime'}:`, error);
    throw error;
  } finally {
    if (client) await client.release();
  }
}


async function ensureCandleTableExists(pool: PoolType, tableName: string): Promise<void> {
  let client;
  try {
    client = await pool.connect();
    const exists = await client.query(
      `SELECT EXISTS (SELECT FROM information_schema.tables 
       WHERE table_schema = 'public' AND table_name = $1)`,
      [tableName]
    );

    if (!exists.rows[0].exists) {
      await client.query(`
        CREATE TABLE ${tableName} (
          candlesize TEXT NOT NULL,
          lots SMALLINT,
          candletime TIMESTAMP WITH TIME ZONE NOT NULL,
          open NUMERIC(12,5) NOT NULL,
          high NUMERIC(12,5) NOT NULL,
          low NUMERIC(12,5) NOT NULL,
          close NUMERIC(12,5) NOT NULL,
          PRIMARY KEY (candlesize, lots, candletime)
        )`);
      candlesLogger.info(`Created candle table ${tableName} in ${pool === centroidPool ? 'Centroid' : 'LPPime'}`);
    }
  } catch (error) {
    candlesLogger.error(`Error ensuring candle table ${tableName} exists in ${pool === centroidPool ? 'Centroid' : 'LPPime'}:`, error);
    throw error;
  } finally {
    if (client) await client.release();
  }
}

async function initializeApplication() {
  try {
    candlesLogger.info('Starting Candles Server...');
    await cleanQueues();

    organizedBatches = {};

    // clean every failed jobs after every 10 minutes
    cleanupInterval = setInterval(cleanFailedJobs, 10 * 60 * 1000);

    await cleanFailedJobs();

    await initDatabase();
    await pgListener();
    candlesLogger.info('Application initialized. Waiting for ticks...');
  } catch (error) {
    candlesLogger.error("Application initialization failed:", error);
    process.exit(1);
  }
}

process.on("SIGINT", async () => {
  if (isShuttingDown) return;
  isShuttingDown = true;
  candlesLogger.info("Shutting down gracefully...");

  try {

    await tickQueue.close();
    await candleQueue.close();
    await redisConnection.quit();
    await centroidPool.end();
    candlesLogger.info("Resources closed successfully");
    process.exit(0);
  } catch (error) {
    candlesLogger.error("Error during shutdown:", error);
    process.exit(1);
  }
});

initializeApplication();