import winston from 'winston';
import { format } from 'winston';
import fs from 'fs';
import path from 'path';

const logDir = 'logs';
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

const createServiceLogger = (serviceName: string) => {
  const logFormat = format.printf(({ level, message }) => {
    return `[${level}] ${serviceName}: ${message}`;
  });

  const transports = [
    // File transport (plain text)
    new winston.transports.File({
      filename: path.join(logDir, `${serviceName}.log`),
      format: logFormat
    })
  ];

  return winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    transports,
    exitOnError: false
  });
};

export const candlesLogger = createServiceLogger('candles-logger');

export default candlesLogger;