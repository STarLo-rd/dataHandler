const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { InfluxDB } = require('@influxdata/influxdb-client');
const path = require('path');

// InfluxDB configuration
const token = 'OS0t8w6jBnwwL-HIWgU1lWniUARhRc85gLtFqTbhZiEqVNPvludyzs1vswBDAsegbfWk1pJGpk3dY1LKK_2zDQ==';
const org = '6914ea40681fbe1d';
const bucket = 'brandpulse';
const url = 'http://localhost:8086';
const client = new InfluxDB({ url, token });
const queryApi = client.getQueryApi(org);

// Express and Socket.io setup
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Serve static files (index.html, Chart.js)
app.use(express.static(path.join(__dirname, 'public')));

// Serve the dashboard
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Corrected Flux query
const sentimentQuery = `
  from(bucket: "${bucket}")
    |> range(start: -10m)
    |> filter(fn: (r) => r._measurement == "tweets")
    |> filter(fn: (r) => r._field == "count")
    |> group(columns: ["sentiment"])
    |> sum()
`;

// Real-time data update
const updateDashboard = async () => {
  try {
    const rows = await queryApi.collectRows(sentimentQuery);
    console.log('Query result:', JSON.stringify(rows, null, 2)); // Detailed logging

    const sentimentCounts = { positive: 0, negative: 0, neutral: 0 };
    
    rows.forEach(row => {
      if (row.sentiment === 'positive') sentimentCounts.positive = row._value;
      if (row.sentiment === 'negative') sentimentCounts.negative = row._value;
      if (row.sentiment === 'neutral') sentimentCounts.neutral = row._value;
    });

    const total = sentimentCounts.positive + sentimentCounts.negative + sentimentCounts.neutral;
    const sentimentPercentages = {
      positive: total ? (sentimentCounts.positive / total * 100).toFixed(1) : 0,
      negative: total ? (sentimentCounts.negative / total * 100).toFixed(1) : 0,
      neutral: total ? (sentimentCounts.neutral / total * 100).toFixed(1) : 0,
    };

    // Check for crisis alert (negatives > 50%)
    const alert = sentimentPercentages.negative > 50 ? 'Crisis Alert: Negative sentiment spike!' : null;

    // Emit to all connected clients
    io.emit('sentimentUpdate', {
      sentiment: sentimentPercentages,
      totalTweets: 500000, // Fixed at 500K/sec for demo
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
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`Dashboard running on http://localhost:${PORT}`);
});