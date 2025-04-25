import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import pkg from "pg";
const { Pool } = pkg;
import { configDotenv } from "dotenv";
import candlesLogger from "./logger";
import { candleQueue } from '.';

configDotenv();

const redisConnection = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null
  });

const centroidPool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
  max: 30,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 60000,
  allowExitOnIdle: true,
  maxUses: 1000,
});



interface TickData {
  symbol: string;
  price: number;
  timestamp: Date;
  lots: number;
  type: "BID" | "ASK";
}

const timeFrames: Record<string, number> = {
  M1: 60000,
  H1: 3600000,
  D1: 86400000,
};

const tickWorker = new Worker('tickQueue', async (job) => {
  const tickData: TickData = job.data;
  await storeTickData(tickData);
  
  if (tickData.type === "BID") {
    await candleQueue.add('processCandle', tickData, {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 1000,
      },
    });
  }
}, { connection: redisConnection, concurrency: 10 });

const candleWorker = new Worker('candleQueue', async (job) => {
  const tickData: TickData = job.data;
  await processTickForCandles(tickData);
}, { connection: redisConnection, concurrency: 5 });

async function storeTickData(data: TickData) {
  if (!data.symbol || !data.price || !data.lots) {
    throw new Error(`Invalid market data: ${JSON.stringify(data)}`);
  }

  const lots = parseInt(data.lots.toString());
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

  const tableName = `ticks_${data.symbol.toLowerCase()}_${data.type.toLowerCase()}`;

  let client;
  try {
    client = await centroidPool.connect();
    
    const insertQuery = await client.query({
      text: `
        INSERT INTO ${tableName} 
        (ticktime, lots, price)
        VALUES ($1, $2, $3)
      `,
      values: [
        ticktime.toISOString(),
        lots,
        parseFloat(data.price.toString())
      ],
    });

    if (insertQuery.rowCount !== null && insertQuery.rowCount > 0) {
      candlesLogger.info(`Successfully inserted tick for ${data.symbol} into ${tableName}`);
    }
  } catch (error) {
    candlesLogger.error(`Error storing tick data for ${data.symbol}:`, error);
    throw error;
  } finally {
    if (client) {
      await client.release();
    }
  }
}

async function processTickForCandles(tickData: TickData) {
  const { symbol, price, timestamp } = tickData;
  const lots = 1;
  const tableName = `candles_${symbol.toLowerCase()}_bid`;

  let processedTimestamp: Date;

  if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
    processedTimestamp = timestamp;
  } else if (typeof timestamp === 'string' || typeof timestamp === 'number') {
    processedTimestamp = new Date(timestamp);
    if (isNaN(processedTimestamp.getTime())) {
      const numTimestamp = Number(timestamp);
      processedTimestamp = new Date(
        numTimestamp > 1000000000000 ? numTimestamp : numTimestamp * 1000
      );
    }
  } else {
    processedTimestamp = new Date();
  }

  if (isNaN(processedTimestamp.getTime())) {
    processedTimestamp = new Date();
  }

  for (const timeframe of Object.keys(timeFrames)) {
    let client;
    try {
      client = await centroidPool.connect();
      
      const duration = timeFrames[timeframe];

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

      const existingCandle = await client.query({
        text: `
          SELECT * FROM ${tableName}
          WHERE candlesize = $1 AND lots = $2 AND candletime = $3
        `,
        values: [timeframe, lots, candleTime.toISOString()],
      });

      if (existingCandle.rows.length > 0) {
        await client.query({
          text: `
            UPDATE ${tableName}
            SET high = GREATEST(high, $1),
                low = LEAST(low, $2),
                close = $3
            WHERE candlesize = $4
            AND lots = $5
            AND candletime = $6
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
        candlesLogger.info(`Updated candle for ${symbol} in ${timeframe}`);
      } else {
         await client.query({
          text: `
            INSERT INTO ${tableName}
            (candlesize, lots, candletime, open, high, low, close)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          values: [
            timeframe,
            lots,
            candleTime.toISOString(),
            price, 
            price, 
            price,
            price, 
          ],
        });
        candlesLogger.info(`Created new candle for ${symbol} in ${timeframe}`);
      }
    } catch (error) {
      candlesLogger.error(`Error processing ${timeframe} candle for ${symbol}:`, error);
      throw error;
    } finally {
      if (client) {
        await client.release();
      }
    }
  }
}

console.log('Worker started');

process.on("SIGINT", async () => {
  console.log("Shutting down worker gracefully...");
  
  try {
    await tickWorker.close();
    await candleWorker.close();
    await redisConnection.quit();
    await centroidPool.end();
    console.log("Worker shut down successfully");
    process.exit(0);
  } catch (error) {
    console.error("Error during worker shutdown:", error);
    process.exit(1);
  }
});