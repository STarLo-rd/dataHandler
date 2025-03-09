const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { InfluxDB } = require('@influxdata/influxdb-client');
const path = require('path');
require('dotenv').config(); // Load .env file for configuration

// InfluxDB configuration (from .env or defaults)
const token = process.env.INFLUXDB_TOKEN || 'OS0t8w6jBnwwL-HIWgU1lWniUARhRc85gLtFqTbhZiEqVNPvludyzs1vswBDAsegbfWk1pJGpk3dY1LKK_2zDQ==';
const org = process.env.INFLUXDB_ORG || '6914ea40681fbe1d';
const bucket = process.env.INFLUXDB_BUCKET || 'brandpulse';
const url = process.env.INFLUXDB_URL || 'http://localhost:8086';
const client = new InfluxDB({ url, token });
const queryApi = client.getQueryApi(org);

// Express and Socket.io setup
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Serve static files (index.html, Chart.js, sounds)
app.use(express.static(path.join(__dirname, 'public')));

// Serve the dashboard
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Flux queries
const sentimentQuery = `
  from(bucket: "${bucket}")
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "tweets")
    |> filter(fn: (r) => r._field == "count")
    |> group(columns: ["sentiment"])
    |> sum()
`;

const historicalQuery = `
  from(bucket: "${bucket}")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "tweets")
    |> filter(fn: (r) => r._field == "count")
    |> aggregateWindow(every: 1m, fn: sum)
    |> group(columns: ["sentiment"])
    |> pivot(rowKey: ["_time"], columnKey: ["sentiment"], valueColumn: "_value")
`;

// Real-time and historical data update
const updateDashboard = async () => {
  try {
    // Real-time sentiment data
    const sentimentRows = await queryApi.collectRows(sentimentQuery);
    const sentimentCounts = { positive: 0, negative: 0, neutral: 0 };
    
    sentimentRows.forEach(row => {
      if (row.sentiment === 'positive') sentimentCounts.positive = row._value || 0;
      if (row.sentiment === 'negative') sentimentCounts.negative = row._value || 0;
      if (row.sentiment === 'neutral') sentimentCounts.neutral = row._value || 0;
    });

    const total = sentimentCounts.positive + sentimentCounts.negative + sentimentCounts.neutral;
    const sentimentPercentages = {
      positive: total ? (sentimentCounts.positive / total * 100).toFixed(1) : 0,
      negative: total ? (sentimentCounts.negative / total * 100).toFixed(1) : 0,
      neutral: total ? (sentimentCounts.neutral / total * 100).toFixed(1) : 0,
    };

    // Historical sentiment data
    const historicalRows = await queryApi.collectRows(historicalQuery);
    const historicalData = {
      positive: [],
      negative: [],
      neutral: [],
      labels: []
    };

    historicalRows.forEach(row => {
      const time = new Date(row._time).toISOString();
      historicalData.labels.push(time);
      historicalData.positive.push(row.positive ? (row.positive / (row.positive + row.negative + row.neutral) * 100).toFixed(1) : 0);
      historicalData.negative.push(row.negative ? (row.negative / (row.positive + row.negative + row.neutral) * 100).toFixed(1) : 0);
      historicalData.neutral.push(row.neutral ? (row.neutral / (row.positive + row.negative + row.neutral) * 100).toFixed(1) : 0);
    });

    // Alerts
    const alertMessages = [];
    if (sentimentPercentages.negative > 50) {
      alertMessages.push('Crisis Alert: Negative sentiment spike!');
    }
    if (sentimentPercentages.positive > 70) {
      alertMessages.push('Opportunity Alert: Positive sentiment surge!');
    }
    const alert = alertMessages.join('\n');

    // Emit to all connected clients
    io.emit('sentimentUpdate', {
      sentiment: sentimentPercentages,
      totalTweets: total, // Use actual total from InfluxDB
      historical: historicalData,
      alert,
    });
  } catch (err) {
    console.error('InfluxDB query error:', err.message);
  }
};

// Update every second
setInterval(updateDashboard, 1000);

// Socket.io connection
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.on('disconnect', () => console.log('Client disconnected:', socket.id));
});

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Dashboard running on http://localhost:${PORT}`);
});