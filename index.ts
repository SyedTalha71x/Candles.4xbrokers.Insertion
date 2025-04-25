import pkg from "pg";
const { Pool } = pkg;
import { configDotenv } from "dotenv";
import candlesLogger from "./logger";
import { Queue } from 'bullmq';
import IORedis from 'ioredis';

configDotenv();

const redisConnection = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: null
});

export const tickQueue = new Queue('tickQueue', { connection: redisConnection });
export const candleQueue = new Queue('candleQueue', { connection: redisConnection });

const CENTROID_HOST = process.env.PG_HOST;
const CENTROID_PORT = process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432;
const CENTROID_USER = process.env.PG_USER;
const CENTROID_PASSWORD = process.env.PG_PASSWORD;
const CENTROID_DATABASE = process.env.PG_DATABASE;


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



centroidPool.on('error', (err) => {
  candlesLogger.error(`Unexpected error on idle client: ${err.message}`, err.stack);
});

let availableCurrencyPairs: CurrencyPairInfo[] = [];
const subscribedPairs: Set<string> = new Set();

async function pgListener() {
  let client;
  try {
    client = await centroidPool.connect();
    candlesLogger.info("Connected to Centroid PostgreSQL for notifications");

    client.on("notification", async (msg) => {
      if (msg.channel === 'tick') {
        try {
          const [symbol, lotsStr, bora, priceStr, timestampStr] = (msg.payload ?? "").split(" ");
          const lots = parseInt(lotsStr);
          const price = parseFloat(priceStr);
          const timestamp = new Date(parseInt(timestampStr));

          if (isNaN(timestamp.getTime())) {
            candlesLogger.error(`Invalid timestamp received: ${timestampStr}`);
            return;
          }

          if (isNaN(lots) || isNaN(price)) {
            candlesLogger.error("Invalid data in notification:", { lots, price });
            return;
          }

          const tickData: TickData = {
            symbol,
            price,
            timestamp,
            lots,
            type: bora === "A" ? "ASK" : "BID"
          };

          await tickQueue.add('processTick', tickData, {
            attempts: 3,
            backoff: {
              type: 'exponential',
              delay: 1000,
            },
          });

        } catch (error: any) {
          candlesLogger.error("Error parsing notification payload from Centroid:", error);
        }
      }
    });

    client.on('error', (err) => {
      candlesLogger.error("PostgreSQL client error:", err);
    });

    await client.query("LISTEN tick");

  } catch (error: any) {
    candlesLogger.error(`Connection failed: ${error.message}`);
    throw error;
  }
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
  let client;
  try {
    client = await centroidPool.connect();
    const result = await client.query(
      "SELECT currpair, contractsize FROM currpairdetails"
    );
    availableCurrencyPairs = result.rows;
    candlesLogger.info(`Found ${availableCurrencyPairs.length} currency pairs in Lppime`);

    const validPairs = availableCurrencyPairs.filter(
      (pair) => pair.contractsize !== null
    );
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
  } finally {
    if (client) {
      await client.release();
    }
  }
}

async function ensureTableExists(tableName: string): Promise<void> {
  let client;
  try {
    client = await centroidPool.connect();
    const tableCheck = await client.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = $1
      )`,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await client.query(`
        CREATE TABLE ${tableName} (
          ticktime TIMESTAMP WITH TIME ZONE NOT NULL,
          lots INTEGER NOT NULL,
          price NUMERIC NOT NULL,
          PRIMARY KEY (lots, ticktime)
        )
      `);
      candlesLogger.info(`Created table ${tableName} in Lppime`);
    }
  } catch (error) {
    console.error(`Error ensuring table ${tableName} exists in Lppime:`, error);
    candlesLogger.error(`Error ensuring table ${tableName} exists in Lppime:`, error);
    throw error;
  } finally {
    if (client) {
      await client.release();
    }
  }
}

async function ensureCandleTableExists(tableName: string): Promise<void> {
  let client;
  try {
    client = await centroidPool.connect();
    const tableCheck = await client.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = $1
      )`,
      [tableName]
    );

    if (!tableCheck.rows[0].exists) {
      await client.query(`
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
    }
  } catch (error) {
    console.error(`Error ensuring candle table ${tableName} exists in Lppime:`, error);
    candlesLogger.error(`Error ensuring candle table ${tableName} exists in Lppime:`, error);
    throw error;
  } finally {
    if (client) {
      await client.release();
    }
  }
}

async function initializeApplication() {
  try {
    candlesLogger.info('Starting Candles Server...');
    console.log('Starting Candles Server...');

    await initDatabase();
    await pgListener();

    console.log('Application initialized. Waiting for ticks...');

  } catch (error) {
    console.error("Application initialization failed:", error);
    process.exit(1);
  }
}

process.on("SIGINT", async () => {
  console.log("Shutting down gracefully...");
  isShuttingDown = true;

  try {
    await tickQueue.close();
    await candleQueue.close();
    await redisConnection.quit();
    await centroidPool.end();
    console.log("Resources closed successfully");
    process.exit(0);
  } catch (error) {
    console.error("Error during shutdown:", error);
    process.exit(1);
  }
});

initializeApplication();