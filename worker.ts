import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import pkg, { PoolClient } from "pg";
const { Pool } = pkg;
import { configDotenv } from "dotenv";
import candlesLogger from "./logger";

configDotenv();

const redisConnection = new IORedis({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 6379,
  maxRetriesPerRequest: null
});

const centroidPool = new Pool({
  host: process.env.CENTROID_PG_HOST,
  port: process.env.CENTROID_PG_PORT ? Number(process.env.CENTROID_PG_PORT) : 5432,
  user: process.env.CENTROID_PG_USER,
  password: process.env.CENTROID_PG_PASSWORD,
  database: process.env.CENTROID_PG_DATABASE,
  max: 30,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 60000,
  allowExitOnIdle: true,
  maxUses: 1000,
});

const lppimePool = new Pool({
  host: process.env.LPPIME_HOST,
  port: process.env.LPPIME_PORT ? Number(process.env.LPPIME_PORT) : 5432,
  user: process.env.LPPIME_USER,
  password: process.env.LPPIME_PASSWORD,
  database: process.env.LPPIME_DATABASE,
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
  if (job.name === 'processTickBatch') {
    const { symbol, bidTicks, askTicks } = job.data;
    
    // Process centroid database first
    try {
      const centroidClient = await centroidPool.connect();
      try {
        await centroidClient.query('BEGIN');
        
        if (bidTicks.length > 0) {
          await storeTickDataBatch(symbol, "bid", bidTicks, centroidClient, 'centroid');
        }
        
        if (askTicks.length > 0) {
          await storeTickDataBatch(symbol, "ask", askTicks, centroidClient, 'centroid');
        }
        
        await centroidClient.query('COMMIT');
        candlesLogger.info(`Successfully processed tick batch for ${symbol} in centroid (BID: ${bidTicks.length}, ASK: ${askTicks.length})`);
      } catch (error) {
        await centroidClient.query('ROLLBACK').catch(e => candlesLogger.error('Centroid rollback error:', e));
        throw error;
      } finally {
        centroidClient.release();
      }

      // Only proceed to lppime if centroid was successful
      const lppimeClient = await lppimePool.connect();
      try {
        await lppimeClient.query('BEGIN');
        
        if (bidTicks.length > 0) {
          await storeTickDataBatch(symbol, "bid", bidTicks, lppimeClient, 'lppime');
        }
        
        if (askTicks.length > 0) {
          await storeTickDataBatch(symbol, "ask", askTicks, lppimeClient, 'lppime');
        }
        
        await lppimeClient.query('COMMIT');
        candlesLogger.info(`Successfully processed tick batch for ${symbol} in lppime (BID: ${bidTicks.length}, ASK: ${askTicks.length})`);
      } catch (error) {
        await lppimeClient.query('ROLLBACK').catch(e => candlesLogger.error('Lppime rollback error:', e));
        throw error;
      } finally {
        lppimeClient.release();
      }

    } catch (error) {
      candlesLogger.error(`Error processing tick batch for ${symbol}:`, error);
      throw error;
    }
  }
}, {
  connection: redisConnection,
  concurrency: 3,
  limiter: { max: 10, duration: 1000 }
});

async function storeTickDataBatch(symbol: string, type: "bid" | "ask", ticks: TickData[], client: PoolClient, dbName: string) {
  try {
    const tableName = `ticks_${symbol.toLowerCase()}_${type}`;
    
    candlesLogger.debug(`Processing ${ticks.length} ticks for ${symbol} ${type} in ${dbName}`);
    
    // Ensure all timestamps are Date objects
    const processedTicks = ticks.map(tick => {
      if (!(tick.timestamp instanceof Date)) {
        return {
          ...tick,
          timestamp: new Date(tick.timestamp)
        };
      }
      return tick;
    });

    // Check existing ticks with all three values (timestamp, lots, AND price)
    const existingTicksQuery = await client.query({
      text: `
        SELECT ticktime, lots, price FROM ${tableName}
        WHERE (ticktime, lots, price) IN (
          ${processedTicks.map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`).join(', ')}
        )
      `,
      values: processedTicks.flatMap(tick => [
        tick.timestamp.toISOString(),
        tick.lots,
        tick.price
      ])
    });

    const existingTicksMap = new Map<string, boolean>();
    for (const row of existingTicksQuery.rows) {
      const key = `${row.ticktime.toISOString()}_${row.lots}_${row.price}`;
      existingTicksMap.set(key, true);
    }

    const ticksToProcess = processedTicks.filter(tick => {
      const key = `${tick.timestamp.toISOString()}_${tick.lots}_${tick.price}`;
      return !existingTicksMap.has(key);
    });

    if (ticksToProcess.length === 0) {
      candlesLogger.debug(`All ${ticks.length} ticks already exist with identical values in ${dbName}`);
      return;
    }

    // Process in smaller batches to avoid parameter limits
    const batchSize = 500;
    for (let i = 0; i < ticksToProcess.length; i += batchSize) {
      const batch = ticksToProcess.slice(i, i + batchSize);
      
      const values = [];
      const params = [];
      let paramIndex = 1;
      
      for (const tick of batch) {
        values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
        params.push(
          tick.timestamp.toISOString(),
          tick.lots,
          tick.price
        );
      }
      
      const queryText = `
        INSERT INTO ${tableName} (ticktime, lots, price)
        VALUES ${values.join(', ')}
      `;
      
      const result = await client.query(queryText, params);
      candlesLogger.debug(`Inserted ${result.rowCount} ticks into ${tableName} in ${dbName} (batch ${i/batchSize + 1})`);
    }
    
  } catch (error) {
    candlesLogger.error(`Error in ${dbName} batch processing for ${symbol} ${type}:`, error);
    throw error;
  }
}

const candleWorker = new Worker('candleQueue', async (job) => {
  if (job.name === 'processCandleBatch') {
    const { symbol, bidTicks } = job.data;
    
    // Process centroid database first
    try {
      const centroidClient = await centroidPool.connect();
      try {
        await centroidClient.query('BEGIN');
        
        if (bidTicks.length > 0) {
          await processCandleBatch(symbol, bidTicks, centroidClient, 'centroid');
        }
        
        await centroidClient.query('COMMIT');
        candlesLogger.info(`Successfully processed candle batch for ${symbol} in centroid with ${bidTicks.length} ticks`);
      } catch (error) {
        await centroidClient.query('ROLLBACK').catch(e => candlesLogger.error('Centroid rollback error:', e));
        throw error;
      } finally {
        centroidClient.release();
      }

      // Only proceed to lppime if centroid was successful
      const lppimeClient = await lppimePool.connect();
      try {
        await lppimeClient.query('BEGIN');
        
        if (bidTicks.length > 0) {
          await processCandleBatch(symbol, bidTicks, lppimeClient, 'lppime');
        }
        
        await lppimeClient.query('COMMIT');
        candlesLogger.info(`Successfully processed candle batch for ${symbol} in lppime with ${bidTicks.length} ticks`);
      } catch (error) {
        await lppimeClient.query('ROLLBACK').catch(e => candlesLogger.error('Lppime rollback error:', e));
        throw error;
      } finally {
        lppimeClient.release();
      }

    } catch (error) {
      candlesLogger.error(`Error processing candle batch for ${symbol}:`, error);
      throw error;
    }
  }
}, { connection: redisConnection, concurrency: 3 });

async function processCandleBatch(symbol: string, ticks: TickData[], client: PoolClient, dbName: string) {
  try {
    const tableName = `candles_${symbol.toLowerCase()}_bid`;
    const lots = 1;
    
    candlesLogger.debug(`Processing ${ticks.length} candle ticks for ${symbol} in ${dbName}`);
    
    // Ensure all timestamps are Date objects
    const processedTicks = ticks.map(tick => {
      if (!(tick.timestamp instanceof Date)) {
        return {
          ...tick,
          timestamp: new Date(tick.timestamp)
        };
      }
      return tick;
    });

    for (const timeframe of Object.keys(timeFrames)) {
      const duration = timeFrames[timeframe];
      const candleMap = new Map<string, {
        time: Date,
        minPrice: number,
        maxPrice: number,
        firstPrice: number,
        lastPrice: number
      }>();

      // Group ticks into candles
      for (const tick of processedTicks) {
        const candleTime = new Date(Math.floor(tick.timestamp.getTime() / duration) * duration);
        const candleTimeStr = candleTime.toISOString();
        
        if (!candleMap.has(candleTimeStr)) {
          candleMap.set(candleTimeStr, {
            time: candleTime,
            minPrice: tick.price,
            maxPrice: tick.price,
            firstPrice: tick.price,
            lastPrice: tick.price
          });
        } else {
          const candle = candleMap.get(candleTimeStr)!;
          candle.minPrice = Math.min(candle.minPrice, tick.price);
          candle.maxPrice = Math.max(candle.maxPrice, tick.price);
          candle.lastPrice = tick.price;
        }
      }

      const candleTimes = Array.from(candleMap.keys());

      if (candleTimes.length === 0) {
        candlesLogger.debug(`No valid candles to process for ${symbol} ${timeframe} in ${dbName}`);
        continue;
      }
      
      // Process in smaller batches to avoid parameter limits
      const timeBatchSize = 100;
      for (let i = 0; i < candleTimes.length; i += timeBatchSize) {
        const timeBatch = candleTimes.slice(i, i + timeBatchSize);
        
        // Check existing candles
        const existingQuery = await client.query({
          text: `
            SELECT candletime, open, high, low, close 
            FROM ${tableName} 
            WHERE candlesize = $1 AND lots = $2 AND candletime = ANY($3::timestamp[])
          `,
          values: [timeframe, lots, timeBatch]
        });
        
        const existingCandles = new Map<string, {
          open: number,
          high: number,
          low: number,
          close: number
        }>();
        
        for (const row of existingQuery.rows) {
          existingCandles.set(row.candletime.toISOString(), {
            open: parseFloat(row.open),
            high: parseFloat(row.high),
            low: parseFloat(row.low),
            close: parseFloat(row.close)
          });
        }
        
        // Prepare insert and update operations
        const insertCandles: Array<{
          time: Date,
          firstPrice: number,
          minPrice: number,
          maxPrice: number,
          lastPrice: number
        }> = [];
        
        const updateCandles: Array<{
          time: Date,
          minPrice: number,
          maxPrice: number,
          lastPrice: number
        }> = [];
        
        for (const candleTimeStr of timeBatch) {
          const candle = candleMap.get(candleTimeStr);
          if (!candle) continue;
          
          const existingCandle = existingCandles.get(candleTimeStr);
          
          if (!existingCandle) {
            insertCandles.push({
              time: candle.time,
              firstPrice: candle.firstPrice,
              minPrice: candle.minPrice,
              maxPrice: candle.maxPrice,
              lastPrice: candle.lastPrice
            });
          } else {
            const newMin = Math.min(existingCandle.low, candle.minPrice);
            const newMax = Math.max(existingCandle.high, candle.maxPrice);
            
            if (newMin !== existingCandle.low || newMax !== existingCandle.high || candle.lastPrice !== existingCandle.close) {
              updateCandles.push({
                time: candle.time,
                minPrice: newMin,
                maxPrice: newMax,
                lastPrice: candle.lastPrice
              });
            }
          }
        }
        
        // Process inserts
        if (insertCandles.length > 0) {
          const insertBatchSize = 100;
          for (let j = 0; j < insertCandles.length; j += insertBatchSize) {
            const batch = insertCandles.slice(j, j + insertBatchSize);
            const values = batch.map((_, idx) => 
              `($${idx*7+1}, $${idx*7+2}, $${idx*7+3}, $${idx*7+4}, $${idx*7+5}, $${idx*7+6}, $${idx*7+7})`
            ).join(', ');
            
            const params = batch.flatMap(c => [
              timeframe,
              lots,
              c.time.toISOString(),
              c.firstPrice,
              c.maxPrice,
              c.minPrice,
              c.lastPrice
            ]);
            
            await client.query({
              text: `
                INSERT INTO ${tableName} 
                (candlesize, lots, candletime, open, high, low, close)
                VALUES ${values}
              `,
              values: params
            });
          }
          candlesLogger.debug(`Inserted ${insertCandles.length} new candles for ${symbol} ${timeframe} in ${dbName}`);
        }
        
        // Process updates
        if (updateCandles.length > 0) {
          const updateBatchSize = 50;
          for (let j = 0; j < updateCandles.length; j += updateBatchSize) {
            const batch = updateCandles.slice(j, j + updateBatchSize);
            
            let paramIndex = 1;
            const timeValues = [];
            const caseStatements = {
              low: [] as string[],
              high: [] as string[],
              close: [] as string[]
            };
            const queryParams = [];
            
            for (const candle of batch) {
              timeValues.push(`$${paramIndex}`);
              queryParams.push(candle.time.toISOString());
              
              caseStatements.low.push(`WHEN candletime = $${paramIndex} THEN $${paramIndex+1}`);
              caseStatements.high.push(`WHEN candletime = $${paramIndex} THEN $${paramIndex+2}`);
              caseStatements.close.push(`WHEN candletime = $${paramIndex} THEN $${paramIndex+3}`);
              
              queryParams.push(candle.minPrice, candle.maxPrice, candle.lastPrice);
              paramIndex += 4;
            }
            
            queryParams.push(timeframe, lots);
            
            const updateQuery = `
              UPDATE ${tableName}
              SET
                low = CASE ${caseStatements.low.join(' ')} ELSE low END,
                high = CASE ${caseStatements.high.join(' ')} ELSE high END,
                close = CASE ${caseStatements.close.join(' ')} ELSE close END
              WHERE candlesize = $${paramIndex++}
                AND lots = $${paramIndex++}
                AND candletime IN (${timeValues.join(', ')})
            `;
            
            await client.query({
              text: updateQuery,
              values: queryParams
            });
          }
          candlesLogger.debug(`Updated ${updateCandles.length} existing candles for ${symbol} ${timeframe} in ${dbName}`);
        }
      }
    }
  } catch (error) {
    candlesLogger.error(`Error in ${dbName} processing candle batch for ${symbol}:`, {
      error: error instanceof Error ? error.message : String(error),
      symbol,
      tickCount: ticks.length
    });
    throw error;
  }
}

process.on("SIGINT", async () => {
  candlesLogger.info("Shutting down worker gracefully...");
  try {
    await Promise.all([
      tickWorker.close(),
      candleWorker.close(),
      redisConnection.quit(),
      centroidPool.end(),
      lppimePool.end()
    ]);
    process.exit(0);
  } catch (error) {
    candlesLogger.error("Error during worker shutdown:", error);
    process.exit(1);
  }
});