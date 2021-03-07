// Setup basic express server
const express = require('express');
const cassandra = require('cassandra-driver');
const app = express();
const path = require('path');
const server = require('http').createServer(app);
const io = require('socket.io')(server);
const port = process.env.PORT || 3000;

// Database Connect and Fetch Data
const client = new cassandra.Client({
  contactPoints: ['localhost'],
  localDataCenter: 'datacenter1',
  keyspace: 'chibboleth',
  protocolOptions: {
    port: 9042
  },
  credentials: {username: "cassandra", password: "cassandra"}
});

const query = 'SELECT * FROM sloth';
// Only query when you send a new message (Don't worry about deadlock here hehe)
// const query = 'SELECT MAX(id) FROM sloth';

async function fetchData(query, socket) {
  let previousMessages = await client.execute(query, []).then(result => result.rows);
  socket.emit('login', {
    previousMessages,
    numUsers: numUsers
  });
  // echo globally (all clients) that a person has connected
  socket.broadcast.emit('user joined', {
    username: socket.username,
    numUsers: numUsers
  });
}

server.listen(port, () => {
  console.log('Server listening at port %d', port);
});

// Routing
app.use(express.static(path.join(__dirname, 'public')));

// Chatroom
let numUsers = 0;

io.on('connection', (socket) => {
  let addedUser = false;

  // when the client emits 'new message', this listens and executes
  socket.on('new message', (data) => {
    // we tell the client to execute 'new message'
    console.log(data)
    pushData(socket, data);
  });
  async function pushData(socket, data){
    const queryID = 'SELECT MAX(id) FROM sloth';
    let latestID = await client.execute(queryID, []).then(result => result.rows);
    latestID = latestID[0]['system.max(id)'] + 1;
    const queryInsert = `INSERT INTO sloth (id, username, message) VALUES (${latestID}, '${socket.username}', '${data}')`;
    await client.execute(queryInsert, []).catch(result => console.log(result));
    socket.broadcast.emit('new message', {
      username: socket.username,
      message: data
    });
  }
  // when the client emits 'add user', this listens and executes
  socket.on('add user', (username) => {
    if (addedUser) return;

    // we store the username in the socket session for this client
    socket.username = username;
    ++numUsers;
    addedUser = true;
    // const previousMessages = fetchData(query);
    fetchData(query, socket);
  });

  // when the user disconnects.. perform this
  socket.on('disconnect', () => {
    if (addedUser) {
      --numUsers;

      // echo globally that this client has left
      socket.broadcast.emit('user left', {
        username: socket.username,
        numUsers: numUsers
      });
    }
  });
});
