import express from 'express';
import dotenv from 'dotenv';
import pkg from 'pg';
import Redis from 'ioredis';

dotenv.config();
const app = express();
app.use(express.json());
import client from 'prom-client';
const gRegistry = new client.Registry();
client.collectDefaultMetrics({ register: gRegistry });
const httpRequests = new client.Counter({ name: 'gateway_http_requests_total', help: 'HTTP requests', registers: [gRegistry] });

app.get('/metrics', async (req, res) => {
  res.setHeader('Content-Type', gRegistry.contentType);
  res.end(await gRegistry.metrics());
});

app.use((req, res, next) => { httpRequests.inc(); next(); });


const { Pool } = pkg;
const pool = new Pool({
  host: 'postgres',
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
  port: 5432
});

const redis = new Redis(process.env.REDIS_URL);

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    await redis.ping();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/assignments', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT ride_id, driver_id, eta_min, assigned_at FROM assignments ORDER BY assigned_at DESC LIMIT 50');
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(process.env.PORT || 8080, () => {
  console.log('gateway listening on', process.env.PORT || 8080);
});
