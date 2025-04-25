// import pkg from "pg";
// const { Pool } = pkg;
// import Bull from "bull";
// import { configDotenv } from "dotenv";
// import { createClient } from "redis";
// import type { RedisClientType } from "redis";

// configDotenv();

// const PG_HOST = process.env.PG_HOST;
// const PG_PORT = process.env.PG_PORT ? Number(process.env.PG_PORT) : 5432;
// const PG_USER = process.env.PG_USER;
// const PG_PASSWORD = process.env.PG_PASSWORD;
// const PG_DATABASE = process.env.PG_DATABASE;
// const REDIS_HOST = process.env.REDIS_HOST || "3.82.229.23";
// const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379");
// const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

// const timeFrames = {
//   M1: 60000,
//   H1: 3600000,
//   D1: 86400000,
// };

// // Interfaces
// interface TickData {
//   symbol: string;
//   price: number;
//   timestamp: Date;
//   lots: number;
//   type: "BID" | "ASK";
// }

// interface CurrencyPairInfo {
//   currpair: string;
//   contractsize: number | null;
// }

// // Initialize clients and queues
// const redisClient: RedisClientType = createClient({
//   url: `redis://${REDIS_HOST}:${REDIS_PORT}`,
//   password: REDIS_PASSWORD,
//   socket: {
//     reconnectStrategy: (retries) => {
//       const delay = Math.min(retries * 50, 2000);
//       console.log(`Redis reconnecting, attempt ${retries}, next try in ${delay}ms`);
//       return delay;
//     }
//   }
// });

// const pgPool = new Pool({
//   host: PG_HOST,
//   port: PG_PORT,
//   user: PG_USER,
//   password: PG_PASSWORD,
//   database: PG_DATABASE,
//   max: 20,
//   idleTimeoutMillis: 30000,
//   connectionTimeoutMillis: 2000,
// });

// const marketDataQueue = new Bull("marketData", {
//   redis: {
//     host: REDIS_HOST,
//     port: REDIS_PORT,
//     password: REDIS_PASSWORD,
//   },
//   defaultJobOptions: {
//     attempts: 3,
//     backoff: {
//       type: "exponential",
//       delay: 1000,
//     },
//     removeOnComplete: true,
//     timeout: 30000,
//   },
//   limiter: {
//     max: 100,
//     duration: 1000,
//   },
//   settings: {
//     maxStalledCount: 1,
//   },
// });

// const candleProcessingQueue = new Bull("candleProcessing", {
//   redis: {
//     host: REDIS_HOST,
//     port: REDIS_PORT,
//     password: REDIS_PASSWORD,
//   },
//   defaultJobOptions: {
//     attempts: 3,
//     backoff: {
//       type: "exponential",
//       delay: 1000,
//     },
//     removeOnComplete: true,
//     timeout: 30000,
//   },
//   limiter: {
//     max: 50,
//     duration: 1000,
//   },
// });

// let availableCurrencyPairs: CurrencyPairInfo[] = [];
// const subscribedPairs: Set<string> = new Set();


// async function pgListenner(){
//   const client = await pgPool.connect();

//   client.on("notification", async (msg)=>{
//     if(msg.channel === 'tick'){
//       try{
//       const [symbol, lotsStr, bora, priceStr, timestampStr] = (msg.payload ?? "").split(" ");
//       const lots = parseInt(lotsStr);
//       const price = parseFloat(priceStr);
//       const timestamp = new Date(parseInt(timestampStr));

//       const tickData: TickData = {
//         symbol,
//         price,
//         timestamp,
//         lots,
//         type: bora === "A" ? "ASK" : "BID"
//       };
//       processMarketData(tickData);
//       console.log("Received notification:", tickData);
//     }
//       catch(error: any){
//         console.error("Error parsing notification payload:", error);
//         return;
//       }
//     }
//   })

//   await client.query("LISTEN tick");
//   console.log("Listening for notifications on channel 'tick'...");
// }


// function processMarketData(data: TickData) {
//   marketDataQueue.add(data);
//   // console.log(`Processed tick for ${data.symbol}: ${data.price} with lots ${data.lots}`);
// }

// async function initDatabase() {
//   try {
//     await fetchAllCurrencyPairs();
//     await populateRedisWithMarkuplots();
//     return true;
//   } catch (error) {
//     console.error("Database init failed:", error);
//     throw error;
//   }
// }

// async function fetchAllCurrencyPairs() {
//   try {
//     const result = await pgPool.query(
//       "SELECT currpair, contractsize FROM currpairdetails"
//     );
//     availableCurrencyPairs = result.rows;

//     const validPairs = availableCurrencyPairs.filter(
//       (pair) => pair.contractsize !== null
//     );

//     validPairs.forEach((pair) => {
//       subscribedPairs.add(pair.currpair);
//     });

//     // Create necessary tables
//     for (const pair of validPairs) {
//       await ensureTableExists(`ticks_${pair.currpair.toLowerCase()}_bid`);
//       await ensureTableExists(`ticks_${pair.currpair.toLowerCase()}_ask`);
//       await ensureCandleTableExists(`candles_${pair.currpair.toLowerCase()}_bid`);
//     }
//   } catch (error) {
//     console.error("Error fetching currency pairs:", error);
//   }
// }

// async function ensureTableExists(tableName: string): Promise<void> {
//   try {
//     const tableCheck = await pgPool.query(
//       `SELECT EXISTS (
//         SELECT FROM information_schema.tables
//         WHERE table_schema = 'public'
//         AND table_name = $1
//       )`,
//       [tableName]
//     );

//     if (!tableCheck.rows[0].exists) {
//       await pgPool.query(`
//         CREATE TABLE ${tableName} (
//           ticktime TIMESTAMP WITH TIME ZONE NOT NULL,
//           lots INTEGER PRIMARY KEY,
//           price NUMERIC NOT NULL
//         )
//       `);
//     }
//   } catch (error) {
//     console.error(`Error ensuring table ${tableName} exists:`, error);
//     throw error;
//   }
// }

// async function ensureCandleTableExists(tableName: string): Promise<void> {
//   try {
//     const tableCheck = await pgPool.query(
//       `SELECT EXISTS (
//         SELECT FROM information_schema.tables
//         WHERE table_schema = 'public'
//         AND table_name = $1
//       )`,
//       [tableName]
//     );

//     if (!tableCheck.rows[0].exists) {
//       await pgPool.query(`
//         CREATE TABLE ${tableName} (
//           candlesize TEXT NOT NULL,
//           lots SMALLINT NOT NULL,
//           candletime TIMESTAMP WITH TIME ZONE NOT NULL,
//           open NUMERIC(12,5) NOT NULL,
//           high NUMERIC(12,5) NOT NULL,
//           low NUMERIC(12,5) NOT NULL,
//           close NUMERIC(12,5) NOT NULL,
//           PRIMARY KEY (candlesize, lots, candletime)
//         )
//       `);
//       console.log(`Created candle table ${tableName}`);
//     }
//   } catch (error) {
//     console.error(`Error ensuring candle table ${tableName} exists:`, error);
//     throw error;
//   }
// }

// async function populateRedisWithMarkuplots(): Promise<void> {
//   try {
//     const markuplotsData = await fetchMarkuplotsFromDatabase();

//     for (const row of markuplotsData) {
//       const { currpair, tradertype, decimals, mu_b, mu_a } = row;
//       const markuppipsBid = typeof mu_b === 'number' ? parseFloat(mu_b.toFixed(decimals)) : 0;
//       const markuppipsAsk = typeof mu_a === 'number' ? parseFloat(mu_a.toFixed(decimals)) : 0;

//       const redisKeyBid = `markup_${currpair}_${tradertype}_B`;
//       const redisKeyAsk = `markup_${currpair}_${tradertype}_A`;

//       await redisClient.set(redisKeyBid, markuppipsBid);
//       await redisClient.set(redisKeyAsk, markuppipsAsk);

//       console.log(`Stored markuplots for ${currpair} (${tradertype}): Bid=${markuppipsBid}, Ask=${markuppipsAsk}`);
//     }

//     console.log("Successfully populated Redis with markuplots data");
//   } catch (error) {
//     console.error("Error populating Redis with markuplots data:", error);
//   }
// }

// async function fetchMarkuplotsFromDatabase(): Promise<any[]> {
//   const query = `
//     WITH cpd AS (
//       SELECT currpair, pointsperunit, decimals, 
//              ROW_NUMBER() OVER (PARTITION BY currpair ORDER BY effdate DESC) AS rn 
//       FROM currpairdetails 
//       WHERE effdate <= NOW()
//     ),
//     mul AS (
//       SELECT currpair, tradertype, markuppips_bid, markuppips_ask, 
//              ROW_NUMBER() OVER (PARTITION BY currpair, tradertype ORDER BY effdate DESC) AS rn 
//       FROM markuplots 
//       WHERE effdate <= NOW()
//     )
//     SELECT mul.currpair, mul.tradertype, decimals, 
//            10 * markuppips_bid / pointsperunit AS mu_b, 
//            10 * markuppips_ask / pointsperunit AS mu_a
//     FROM cpd, mul
//     WHERE cpd.currpair = mul.currpair
//       AND cpd.rn = 1
//       AND mul.rn = 1
//     ORDER BY mul.currpair, mul.tradertype;
//   `;

//   try {
//     const result = await pgPool.query(query);
//     return result.rows;
//   } catch (error) {
//     console.error("Error fetching markuplots data from database:", error);
//     throw error;
//   }
// }

// marketDataQueue.process(5, async (job) => {
//   try {
//     const data = job.data;
//     // console.log(`Processing market data job for ${data.symbol} (${data.type})`);

//     if (!data.symbol || !data.price || !data.lots) {
//       throw new Error(`Invalid market data: ${JSON.stringify(data)}`);
//     }

//     let lots = parseInt(data.lots.toString());
//     // console.log("lots ----", lots);
    
//     let ticktime: Date;

//     if (data.timestamp instanceof Date) {
//       ticktime = data.timestamp;
//     } else if (data.timestamp) {
//       ticktime = new Date(data.timestamp);
//     } else {
//       ticktime = new Date();
//     }

//     if (isNaN(ticktime.getTime())) {
//       throw new Error(`Invalid timestamp received: ${data.timestamp}`);
//     }

//     let tableName: string;
//     const symbolLower = data.symbol.toLowerCase();

//     if (data.type === "BID") {
//       tableName = `ticks_${symbolLower}_bid`;
//     } else {
//       tableName = `ticks_${symbolLower}_ask`;
//     }

//     await ensureTableExists(tableName);

//     // console.log(`Processing tick - Symbol: ${data.symbol}, Type: ${data.type}, Price: ${data.price}, Lots: ${lots}, Time: ${ticktime}`);

//   //       await pgPool.query({
//   //     text: `
//   //       INSERT INTO ${tableName} 
//   //       (ticktime, lots, price)
//   //       VALUES ($1, $2, $3)
//   //       ON CONFLICT (lots) DO NOTHING;
//   //     `,
//   //     values: [
//   //       ticktime.toISOString(), 
//   //       lots, 
//   //       parseFloat(data.price.toString()) 
//   //     ],
//   // });

//     // const result = await pgPool.query(insertQuery);

//     // if (result.rowCount && result.rowCount > 0) {
//     //   console.log(`✅ Successfully inserted into ${tableName}:`, {
//     //     symbol: data.symbol,
//     //     type: data.type,
//     //     price: data.price,
//     //     lots: lots,
//     //     timestamp: ticktime.toISOString(),
//     //     rowsAffected: result.rowCount
//     //   });
//     // } else {
//     //   console.log(`ℹ️ No rows inserted (likely duplicate) into ${tableName}:`, {
//     //     symbol: data.symbol,
//     //     lots: lots
//     //   });
//     // }
    
//     await processTick({
//       currpair: data.symbol,
//       lots: lots,
//       price: data.price,
//       tickepoch: Math.floor(ticktime.getTime() / 1000),
//     });

//     if (data.type === "BID") {
//       await processTickForCandles({
//         symbol: data.symbol,
//         price: data.price,
//         timestamp: ticktime,
//         lots: lots,
//         type: data.type
//       });
//     }

//     // console.log(`Successfully processed tick for ${data.symbol}`);
//     return { success: true, symbol: data.symbol, type: data.type };
//   } catch (error) {
//     console.error(`Error processing market data job for ${job.data?.symbol}:`, error);
//     throw error;
//   }
// });

// async function processTickForCandles(tickData: TickData) {
//   try {
//     await candleProcessingQueue.add(
//       {
//         tickData,
//         timeFrames: Object.keys(timeFrames),
//       },
//       {
//         jobId: `candle_${tickData.symbol}_${Date.now()}`,
//       }
//     );
//   } catch (error) {
//     console.error("Error adding tick to candle processing queue:", error);
//   }
// }

// async function processTick(tickData: {
//   currpair: string;
//   lots: number;
//   price: number;
//   tickepoch: number;
// }) {
//   const { currpair, lots, price, tickepoch } = tickData;
//   const resolutions = ["M1", "H1", "D1"];

//   for (const resolution of resolutions) {
//     await processTickResolution(currpair, lots, price, tickepoch, resolution);
//   }
// }

// async function processTickResolution(
//   currpair: string,
//   lots: number,
//   price: number,
//   tickepoch: number,
//   resolution: string
// ) {
//   const redisKey = `${currpair}_${resolution}`;

//   let floor;
//   switch (resolution) {
//     case "M1":
//       floor = Math.floor(tickepoch / 60) * 60;
//       break;
//     case "H1":
//       floor = Math.floor(tickepoch / 3600) * 3600;
//       break;
//     case "D1":
//       const date = new Date(tickepoch * 1000);
//       floor =
//         new Date(
//           date.getUTCFullYear(),
//           date.getUTCMonth(),
//           date.getUTCDate()
//         ).getTime() / 1000;
//       break;
//     default:
//       return;
//   }

//   const existingCandle = await redisClient.zRangeByScore(
//     redisKey,
//     floor,
//     floor
//   );

//   if (existingCandle.length > 0) {
//     const candle = JSON.parse(existingCandle[0]);
//     candle.close = price;
//     candle.high = Math.max(candle.high, price);
//     candle.low = Math.min(candle.low, price);

//     await addRedisRecord(redisKey, { candleepoch: floor, ...candle }, true);
//   } else {
//     const newCandle = {
//       candleepoch: floor,
//       open: price,
//       high: price,
//       low: price,
//       close: price,
//     };

//     await addRedisRecord(redisKey, newCandle);
//   }
// }

// async function addRedisRecord(
//   redisKey: string,
//   candleData: any,
//   deleteExisting = false
// ) {
//   try {
//     if (
//       candleData.candleepoch === undefined ||
//       isNaN(Number(candleData.candleepoch))
//     ) {
//       throw new Error(`Invalid score: ${candleData.candleepoch}`);
//     }

//     if (deleteExisting) {
//       const score = Number(candleData.candleepoch);
//       await redisClient.zRemRangeByScore(redisKey, score, score);
//     }

//     const score = Number(candleData.candleepoch);
//     const record = JSON.stringify({
//       time: candleData.candleepoch,
//       open: candleData.open,
//       high: candleData.high,
//       low: candleData.low,
//       close: candleData.close,
//     });

//     await redisClient.zAdd(redisKey, [
//       {
//         score: score,
//         value: record,
//       },
//     ]);
//   } catch (error) {
//     console.error(`Error adding/updating Redis record for ${redisKey}:`, error);
//   }
// }

// candleProcessingQueue.process(async (job) => {
//   const { tickData, timeFrames } = job.data;
//   const { symbol, price, timestamp } = tickData;

//   const lots = 1;
//   const tableName = `candles_${symbol.toLowerCase()}_bid`;

//   await ensureCandleTableExists(tableName);

//   let processedTimestamp: Date;
//   if (timestamp instanceof Date && !isNaN(timestamp.getTime())) {
//     processedTimestamp = timestamp;
//   } else if (timestamp) {
//     try {
//       processedTimestamp = new Date(timestamp);
//       if (isNaN(processedTimestamp.getTime())) {
//         const numTimestamp = Number(timestamp);
//         processedTimestamp = new Date(
//           numTimestamp > 1000000000000 ? numTimestamp : numTimestamp * 1000
//         );
//       }
//     } catch (error) {
//       console.error(`Failed to parse timestamp for ${symbol}:`, timestamp);
//       throw new Error(`Invalid timestamp for ${symbol}: ${timestamp}`);
//     }
//   } else {
//     processedTimestamp = new Date();
//   }

//   if (isNaN(processedTimestamp.getTime())) {
//     throw new Error(`Invalid timestamp for ${symbol}: ${timestamp}`);
//   }

//   const resolvedTimeFrames = {
//     M1: 60000,
//     H1: 3600000,
//     D1: 86400000,
//   };

//   for (const [timeframe, duration] of Object.entries(resolvedTimeFrames)) {
//     try {
//       const candleTime = new Date(
//         Math.floor(processedTimestamp.getTime() / duration) * duration
//       );

//       const existingCandle = await pgPool.query({
//         text: `
//           SELECT * FROM ${tableName}
//           WHERE candlesize = $1 AND lots = $2 AND candletime = $3
//         `,
//         values: [timeframe, lots, candleTime.toISOString()],
//       });

//       if (existingCandle.rows.length > 0) {
//         //    await pgPool.query({
//         //   text: `
//         //     UPDATE ${tableName}
//         //     SET high = GREATEST(high, $1),
//         //         low = LEAST(low, $2),
//         //         close = $3
//         //     WHERE candlesize = $4
//         //     AND lots = $5
//         //     AND candletime = $6
//         //     RETURNING *
//         //   `,
//         //   values: [
//         //     price,
//         //     price,
//         //     price,
//         //     timeframe,
//         //     lots,
//         //     candleTime.toISOString(),
//         //   ],
//         // });
//         // console.log(`🔄 Updated ${timeframe} candle for ${symbol}:`, updateResult.rows[0]);
//       } else {
//         // await pgPool.query({
//         //      text: `
//         //        INSERT INTO ${tableName}
//         //        (candlesize, lots, candletime, open, high, low, close)
//         //      VALUES ($1, $2, $3, $4, $5, $6, $7)
//         //        RETURNING *
//         //      `,
//         //      values: [
//         //        timeframe,
//         //        lots,
//         //        candleTime.toISOString(),
//         //        price, // open
//         //        price, // high
//         //        price, // low
//         //        price, // close
//         //      ],
//         //    });
//           //  console.log(`✅ Created new ${timeframe} candle for ${symbol}:`, insertResult.rows[0]);
//       }

//       await processTick({
//         currpair: symbol,
//         lots: 1,
//         price: price,
//         tickepoch: Math.floor(processedTimestamp.getTime() / 1000),
//       });

//     } catch (error) {
//       console.error(`Error processing ${timeframe} candle for ${symbol}:`, error);
//       throw error;
//     }
//   }

//   return { success: true, symbol };
// });

// function setupRedisHealthCheck() {
//   const HEALTH_CHECK_INTERVAL = 30000;

//   setInterval(async () => {
//     try {
//       if (!redisClient.isOpen) {
//         console.log("Redis connection is down, attempting to reconnect...");
//         await redisClient.connect();
//         console.log("Redis connection restored");
//       } else {
//         await redisClient.ping();
//       }
//     } catch (error) {
//       console.error("Redis health check failed:", error);
//       try {
//         if (redisClient.isOpen) {
//           await redisClient.disconnect();
//         }
//         await redisClient.connect();
//         console.log("Redis connection restored after forced reconnection");
//       } catch (reconnectError) {
//         console.error("Failed to restore Redis connection:", reconnectError);
//       }
//     }
//   }, HEALTH_CHECK_INTERVAL);
// }

// async function initializeApplication() {
//   try {
//     console.log('Candles Server is runnning perfect!!!!');
    
//     await redisClient.connect();
//     console.log("Connected to Redis");
//     setupRedisHealthCheck();


//     await marketDataQueue.empty();
//     await candleProcessingQueue.empty();

//     await initDatabase();

//     await pgListenner();

//     console.log("Application initialized successfully");
//   } catch (error) {
//     console.error("Application initialization failed:", error);
//     process.exit(1);
//   }
// }

// marketDataQueue.on("completed", (job, result) => {
//   console.log(`Job ${job.id} completed: ${result.symbol} ${result.type}`);
// });

// marketDataQueue.on("failed", (job, error) => {
//   console.error(`Job ${job.id} failed:`, error);
// });

// candleProcessingQueue.on("completed", (job, result) => {
//   // console.log(`Job ${job.id} completed: ${result.symbol}`);
// });

// candleProcessingQueue.on("failed", (job, error) => {
//   console.error(`Job ${job.id} failed:`, error);
// });

// process.on("SIGINT", async () => {
//   console.log("Shutting down...");

//   // console.log("Closing Bull queues...");
//   await marketDataQueue.close();
//   await candleProcessingQueue.close();

//   // console.log("Closing Redis connection...");
//   await redisClient.quit();

//   await pgPool.end().then(() => {
//     // console.log("Database connection closed");
//     process.exit(0);
//   });
// });

// initializeApplication();