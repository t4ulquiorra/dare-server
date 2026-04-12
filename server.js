/**
 * Dare(誰だ) Listen Together Server
 * WebSocket server using protobuf binary protocol
 */

const WebSocket = require('ws');
const protobuf = require('protobufjs');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const PORT = process.env.PORT || 8080;

// ─── Protobuf setup ───────────────────────────────────────────────────────────

const proto = protobuf.loadSync(path.join(__dirname, 'listentogether.proto'));

const Envelope               = proto.lookupType('listentogether.Envelope');
const RoomCreatedPayload     = proto.lookupType('listentogether.RoomCreatedPayload');
const JoinRequestPayload     = proto.lookupType('listentogether.JoinRequestPayload');
const JoinApprovedPayload    = proto.lookupType('listentogether.JoinApprovedPayload');
const JoinRejectedPayload    = proto.lookupType('listentogether.JoinRejectedPayload');
const UserJoinedPayload      = proto.lookupType('listentogether.UserJoinedPayload');
const UserLeftPayload        = proto.lookupType('listentogether.UserLeftPayload');
const UserDisconnectedPayload= proto.lookupType('listentogether.UserDisconnectedPayload');
const UserReconnectedPayload = proto.lookupType('listentogether.UserReconnectedPayload');
const PlaybackActionPayload  = proto.lookupType('listentogether.PlaybackActionPayload');
const BufferWaitPayload      = proto.lookupType('listentogether.BufferWaitPayload');
const BufferCompletePayload  = proto.lookupType('listentogether.BufferCompletePayload');
const SyncStatePayload       = proto.lookupType('listentogether.SyncStatePayload');
const ReconnectedPayload     = proto.lookupType('listentogether.ReconnectedPayload');
const HostChangedPayload     = proto.lookupType('listentogether.HostChangedPayload');
const KickedPayload          = proto.lookupType('listentogether.KickedPayload');
const ErrorPayload           = proto.lookupType('listentogether.ErrorPayload');
const SuggestionReceivedPayload  = proto.lookupType('listentogether.SuggestionReceivedPayload');
const SuggestionApprovedPayload  = proto.lookupType('listentogether.SuggestionApprovedPayload');
const SuggestionRejectedPayload  = proto.lookupType('listentogether.SuggestionRejectedPayload');
const RoomState              = proto.lookupType('listentogether.RoomState');

// ─── Encode / decode helpers ──────────────────────────────────────────────────

function encode(type, PayloadType, fields) {
  const payloadBytes = fields
    ? PayloadType.encode(PayloadType.create(fields)).finish()
    : new Uint8Array(0);

  return Buffer.from(
    Envelope.encode(Envelope.create({
      type,
      payload: payloadBytes,
      compressed: false,
    })).finish()
  );
}

function encodeNoPayload(type) {
  return Buffer.from(
    Envelope.encode(Envelope.create({
      type,
      payload: new Uint8Array(0),
      compressed: false,
    })).finish()
  );
}

function decode(data) {
  const envelope = Envelope.decode(data);
  return { type: envelope.type, payload: Buffer.from(envelope.payload) };
}

function decodePayload(PayloadType, bytes) {
  if (!bytes || bytes.length === 0) return {};
  return PayloadType.decode(bytes);
}

// ─── Room state helpers ───────────────────────────────────────────────────────

function buildRoomState(room) {
  return {
    roomCode: room.code,
    hostId: room.hostId,
    users: Object.values(room.users).map(u => ({
      userId: u.id,
      username: u.username,
      isHost: u.id === room.hostId,
      isConnected: u.ws !== null,
    })),
    currentTrack: room.currentTrack || null,
    isPlaying: room.isPlaying,
    position: room.position,
    lastUpdate: room.lastUpdate,
    volume: room.volume,
    queue: room.queue || [],
  };
}

// ─── State ────────────────────────────────────────────────────────────────────

// rooms[roomCode] = {
//   code, hostId, users: { userId: { id, username, ws, sessionToken } },
//   pendingJoins: { userId: { id, username, ws } },
//   currentTrack, isPlaying, position, lastUpdate, volume, queue,
//   bufferWaiting: { trackId, waitingFor: Set<userId> }
// }
const rooms = {};

// sessionTokens[token] = { roomCode, userId }
const sessionTokens = {};

// wsToUser[ws] = { roomCode, userId }  (set after join/create)
const wsToUser = new WeakMap();

// ─── Room code generator ──────────────────────────────────────────────────────

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms[code]);
  return code;
}

// ─── Send helpers ─────────────────────────────────────────────────────────────

function send(ws, type, PayloadType, fields) {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(encode(type, PayloadType, fields));
}

function sendNoPayload(ws, type) {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(encodeNoPayload(type));
}

function broadcast(room, type, PayloadType, fields, excludeUserId = null) {
  const buf = encode(type, PayloadType, fields);
  for (const user of Object.values(room.users)) {
    if (user.id === excludeUserId) continue;
    if (user.ws && user.ws.readyState === WebSocket.OPEN) {
      user.ws.send(buf);
    }
  }
}

// ─── Message handlers ─────────────────────────────────────────────────────────

function broadcastAll(room, type, PayloadType, fields) {
  const buf = encode(type, PayloadType, fields);
  for (const user of Object.values(room.users)) {
    if (user.ws && user.ws.readyState === WebSocket.OPEN) {
      user.ws.send(buf);
    }
  }
}

function handleCreateRoom(ws, payloadBytes) {
  const p = decodePayload(proto.lookupType('listentogether.CreateRoomPayload'), payloadBytes);
  const username = p.username || 'Host';
  const userId = uuidv4();
  const sessionToken = uuidv4();
  const roomCode = generateRoomCode();

  rooms[roomCode] = {
    code: roomCode,
    hostId: userId,
    users: {
      [userId]: { id: userId, username, ws, sessionToken }
    },
    pendingJoins: {},
    currentTrack: null,
    isPlaying: false,
    position: 0,
    lastUpdate: Date.now(),
    volume: 1.0,
    queue: [],
    bufferWaiting: null,
    suggestions: {},
  };

  sessionTokens[sessionToken] = { roomCode, userId };
  wsToUser.set(ws, { roomCode, userId });

  send(ws, 'room_created', RoomCreatedPayload, { roomCode, userId, sessionToken });
  console.log(`Room created: ${roomCode} by ${username} (${userId})`);
}

function handleJoinRoom(ws, payloadBytes) {
  const p = decodePayload(proto.lookupType('listentogether.JoinRoomPayload'), payloadBytes);
  const { roomCode, username } = p;

  const room = rooms[roomCode];
  if (!room) {
    send(ws, 'join_rejected', JoinRejectedPayload, { reason: 'Room not found' });
    return;
  }

  const userId = uuidv4();
  room.pendingJoins[userId] = { id: userId, username, ws };

  // Notify host of join request
  const hostUser = room.users[room.hostId];
  if (hostUser && hostUser.ws) {
    send(hostUser.ws, 'join_request', JoinRequestPayload, { userId, username });
  }

  console.log(`Join request: ${username} (${userId}) -> room ${roomCode}`);
}

function handleApproveJoin(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.ApproveJoinPayload'), payloadBytes);
  const { userId } = p;

  const pending = room.pendingJoins[userId];
  if (!pending) return;

  delete room.pendingJoins[userId];

  const sessionToken = uuidv4();
  room.users[userId] = { id: userId, username: pending.username, ws: pending.ws, sessionToken };
  sessionTokens[sessionToken] = { roomCode: room.code, userId };
  wsToUser.set(pending.ws, { roomCode: room.code, userId });

  // Send join_approved to new user with full room state
  send(pending.ws, 'join_approved', JoinApprovedPayload, {
    roomCode: room.code,
    userId,
    sessionToken,
    state: buildRoomState(room),
  });

  // If there is a current track, immediately send buffer_complete + seek + play
  if (room.currentTrack && pending.ws.readyState === 1) {
    send(pending.ws, 'buffer_complete', BufferCompletePayload, { trackId: room.currentTrack.id });
    send(pending.ws, 'sync_playback', PlaybackActionPayload, { action: 'seek', position: room.position || 0, serverTime: Date.now() });
    if (room.isPlaying) {
      send(pending.ws, 'sync_playback', PlaybackActionPayload, { action: 'play', position: room.position || 0, serverTime: Date.now() });
    }
  }

  // Notify everyone else
  broadcast(room, 'user_joined', UserJoinedPayload, {
    userId,
    username: pending.username,
  }, userId);

  console.log(`Join approved: ${pending.username} (${userId}) in room ${room.code}`);

  // Ask host to send a full sync so the new guest gets current track
  const hostUser = room.users[room.hostId];
  if (hostUser && hostUser.ws) {
    sendNoPayload(hostUser.ws, 'request_sync');
  }
}

function handleRejectJoin(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.RejectJoinPayload'), payloadBytes);
  const { userId, reason } = p;

  const pending = room.pendingJoins[userId];
  if (!pending) return;

  delete room.pendingJoins[userId];
  send(pending.ws, 'join_rejected', JoinRejectedPayload, { reason: reason || 'Rejected by host' });
}

function handleLeaveRoom(ws, room, userId) {
  if (!room || !userId) return;

  const user = room.users[userId];
  if (!user) return;

  const username = user.username;
  const wasHost = userId === room.hostId;

  delete room.users[userId];
  if (user.sessionToken) delete sessionTokens[user.sessionToken];

  const remainingUsers = Object.values(room.users);

  if (remainingUsers.length === 0) {
    delete rooms[room.code];
    console.log(`Room ${room.code} deleted (empty)`);
    return;
  }

  broadcast(room, 'user_left', UserLeftPayload, { userId, username });

  // Transfer host if needed
  if (wasHost) {
    const newHost = remainingUsers.find(u => u.ws && u.ws.readyState === WebSocket.OPEN);
    if (newHost) {
      room.hostId = newHost.id;
      broadcast(room, 'host_changed', HostChangedPayload, {
        newHostId: newHost.id,
        newHostName: newHost.username,
      });
      console.log(`Host transferred to ${newHost.username} in room ${room.code}`);
    }
  }
}

function handlePlaybackAction(ws, payloadBytes, room, userId) {
  if (userId !== room.hostId) return;

  const p = decodePayload(PlaybackActionPayload, payloadBytes);

  room.lastUpdate = Date.now();
  if (p.position !== undefined && p.position > 0) room.position = p.position;
  if (p.volume !== undefined && p.volume > 0) room.volume = p.volume;
  if (p.trackInfo) room.currentTrack = p.trackInfo;
  if (p.queue && p.queue.length > 0) room.queue = p.queue;
  p.serverTime = Date.now();

  // change_track: special handling — broadcast to all, then pause + buffer_complete + seek
  if (p.action === 'change_track' && p.trackInfo) {
    const trackId = p.trackInfo.id;
    room.currentTrack = p.trackInfo;
    room.position = 0;
    room.isPlaying = false;
    room.bufferWaiting = null;

    const userCount = Object.keys(room.users).length;
    console.log(`[change_track] trackId=${trackId}, users=${userCount}`);
    broadcastAll(room, 'sync_playback', PlaybackActionPayload, p);
    console.log('[change_track] sent change_track to all');
    broadcastAll(room, 'sync_playback', PlaybackActionPayload, { action: 'pause', position: 0, serverTime: Date.now() });
    console.log('[change_track] sent pause to all');
    broadcastAll(room, 'buffer_complete', BufferCompletePayload, { trackId });
    console.log('[change_track] sent buffer_complete to all');
    broadcastAll(room, 'sync_playback', PlaybackActionPayload, { action: 'seek', position: 0, serverTime: Date.now() });
    console.log('[change_track] sent seek to all');
    return;
  }

  // All other actions: update state and broadcast to all
  if (p.action === 'play') { room.isPlaying = true; room.position = p.position || 0; p.serverTime = Date.now(); }
  if (p.action === 'pause') { room.isPlaying = false; room.position = p.position || 0; }
  if (p.action === 'seek') { room.position = p.position || 0; }
  if (p.volume !== undefined && p.volume > 0) room.volume = p.volume;
  if (p.queue && p.queue.length > 0) room.queue = p.queue;

  broadcastAll(room, 'sync_playback', PlaybackActionPayload, p);
}

function handleBufferReady(ws, payloadBytes, room, userId) {
  const p = decodePayload(proto.lookupType('listentogether.BufferReadyPayload'), payloadBytes);
  const { trackId } = p;
  const position = room.position || 0;

  // Per-client ACK: send buffer_complete + seek + play back to just this client
  const user = room.users[userId];
  if (user && user.ws && user.ws.readyState === 1) {
    send(user.ws, 'buffer_complete', BufferCompletePayload, { trackId });
    send(user.ws, 'sync_playback', PlaybackActionPayload, { action: 'seek', position, serverTime: Date.now() });
    if (room.isPlaying) {
      send(user.ws, 'sync_playback', PlaybackActionPayload, { action: 'play', position, serverTime: Date.now() });
    }
  }
}

function handleKickUser(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.KickUserPayload'), payloadBytes);
  const { userId, reason } = p;

  const target = room.users[userId];
  if (!target) return;

  send(target.ws, 'kicked', KickedPayload, { reason: reason || 'Kicked by host' });
  handleLeaveRoom(target.ws, room, userId);
}

function handleTransferHost(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.TransferHostPayload'), payloadBytes);
  const { newHostId } = p;

  const newHost = room.users[newHostId];
  if (!newHost) return;

  room.hostId = newHostId;
  broadcast(room, 'host_changed', HostChangedPayload, {
    newHostId,
    newHostName: newHost.username,
  });
}

function handleRequestSync(ws, room) {
  send(ws, 'sync_state', SyncStatePayload, {
    currentTrack: room.currentTrack || null,
    isPlaying: room.isPlaying,
    position: room.position,
    lastUpdate: room.lastUpdate,
    queue: room.queue || [],
    volume: room.volume,
  });
}

function handleReconnect(ws, payloadBytes) {
  const p = decodePayload(proto.lookupType('listentogether.ReconnectPayload'), payloadBytes);
  const { sessionToken } = p;

  const session = sessionTokens[sessionToken];
  if (!session) {
    send(ws, 'error', ErrorPayload, { code: 'INVALID_TOKEN', message: 'Session not found' });
    return;
  }

  const room = rooms[session.roomCode];
  if (!room) {
    send(ws, 'error', ErrorPayload, { code: 'ROOM_NOT_FOUND', message: 'Room no longer exists' });
    delete sessionTokens[sessionToken];
    return;
  }

  const user = room.users[session.userId];
  if (!user) {
    send(ws, 'error', ErrorPayload, { code: 'USER_NOT_FOUND', message: 'User not in room' });
    return;
  }

  // Update ws reference
  user.ws = ws;
  wsToUser.set(ws, { roomCode: room.code, userId: user.id });

  const isHost = user.id === room.hostId;

  send(ws, 'reconnected', ReconnectedPayload, {
    roomCode: room.code,
    userId: user.id,
    state: buildRoomState(room),
    isHost,
  });

  broadcast(room, 'user_reconnected', UserReconnectedPayload, {
    userId: user.id,
    username: user.username,
  }, user.id);

  console.log(`User reconnected: ${user.username} in room ${room.code}`);
}

function handleSuggestTrack(ws, payloadBytes, room, userId) {
  const p = decodePayload(proto.lookupType('listentogether.SuggestTrackPayload'), payloadBytes);
  const suggestionId = uuidv4();

  room.suggestions = room.suggestions || {};
  room.suggestions[suggestionId] = { fromUserId: userId, trackInfo: p.trackInfo };

  const user = room.users[userId];
  const host = room.users[room.hostId];
  if (host && host.ws) {
    send(host.ws, 'suggestion_received', SuggestionReceivedPayload, {
      suggestionId,
      fromUserId: userId,
      fromUsername: user ? user.username : 'Unknown',
      trackInfo: p.trackInfo,
    });
  }
}

function handleApproveSuggestion(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.ApproveSuggestionPayload'), payloadBytes);
  const suggestion = room.suggestions && room.suggestions[p.suggestionId];
  if (!suggestion) return;

  broadcast(room, 'suggestion_approved', SuggestionApprovedPayload, {
    suggestionId: p.suggestionId,
    trackInfo: suggestion.trackInfo,
  });

  delete room.suggestions[p.suggestionId];
}

function handleRejectSuggestion(ws, payloadBytes, room, requesterId) {
  if (requesterId !== room.hostId) return;

  const p = decodePayload(proto.lookupType('listentogether.RejectSuggestionPayload'), payloadBytes);
  const suggestion = room.suggestions && room.suggestions[p.suggestionId];
  if (!suggestion) return;

  const suggester = room.users[suggestion.fromUserId];
  if (suggester && suggester.ws) {
    send(suggester.ws, 'suggestion_rejected', SuggestionRejectedPayload, {
      suggestionId: p.suggestionId,
      reason: p.reason || 'Rejected by host',
    });
  }

  delete room.suggestions[p.suggestionId];
}

// ─── WebSocket server ─────────────────────────────────────────────────────────

const http = require('http');
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200);
    res.end('OK');
  } else {
    res.writeHead(426, { 'Upgrade': 'websocket' });
    res.end('Upgrade Required');
  }
});
server.listen(PORT);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('New connection');

  ws.on('message', (data) => {
    try {
      const { type, payload } = decode(data);
      console.log('Received message type:', type);

      // Get room/user context if available
      const ctx = wsToUser.get(ws);
      const room = ctx ? rooms[ctx.roomCode] : null;
      const userId = ctx ? ctx.userId : null;

      switch (type) {
        case 'create_room':
          handleCreateRoom(ws, payload);
          break;
        case 'join_room':
          handleJoinRoom(ws, payload);
          break;
        case 'approve_join':
          if (room) handleApproveJoin(ws, payload, room, userId);
          break;
        case 'reject_join':
          if (room) handleRejectJoin(ws, payload, room, userId);
          break;
        case 'leave_room':
          if (room) handleLeaveRoom(ws, room, userId);
          break;
        case 'playback_action':
          if (room) handlePlaybackAction(ws, payload, room, userId);
          break;
        case 'buffer_ready':
          if (room) handleBufferReady(ws, payload, room, userId);
          break;
        case 'kick_user':
          if (room) handleKickUser(ws, payload, room, userId);
          break;
        case 'transfer_host':
          if (room) handleTransferHost(ws, payload, room, userId);
          break;
        case 'request_sync':
          if (room) handleRequestSync(ws, room);
          break;
        case 'reconnect':
          handleReconnect(ws, payload);
          break;
        case 'suggest_track':
          if (room) handleSuggestTrack(ws, payload, room, userId);
          break;
        case 'approve_suggestion':
          if (room) handleApproveSuggestion(ws, payload, room, userId);
          break;
        case 'reject_suggestion':
          if (room) handleRejectSuggestion(ws, payload, room, userId);
          break;
        case 'client_capabilities':
          // Respond with server capabilities
          send(ws, 'server_capabilities', proto.lookupType('listentogether.ServerCapabilities'), {
            supportsProtobuf: true,
            supportsCompression: false,
            serverVersion: '1.0.0',
          });
          break;
        case 'ping':
          sendNoPayload(ws, 'pong');
          break;
        default:
          console.log(`Unknown message type: ${type}`);
      }
    } catch (err) {
      console.error('Error handling message:', err.message, err.stack);
    }
  });

  ws.on('close', () => {
    const ctx = wsToUser.get(ws);
    if (!ctx) return;

    const room = rooms[ctx.roomCode];
    if (!room) return;

    const user = room.users[ctx.userId];
    if (!user) return;

    // Mark as disconnected but keep in room for reconnection
    user.ws = null;

    broadcast(room, 'user_disconnected', UserDisconnectedPayload, {
      userId: ctx.userId,
      username: user.username,
    });

    console.log(`User disconnected: ${user.username} from room ${ctx.roomCode}`);

    // Clean up room after 5 minutes if host disconnects with no reconnection
    if (ctx.userId === room.hostId) {
      setTimeout(() => {
        const r = rooms[ctx.roomCode];
        if (!r) return;
        const u = r.users[ctx.userId];
        if (u && u.ws === null) {
          // Host still disconnected, transfer or close
          const newHost = Object.values(r.users).find(x => x.id !== ctx.userId && x.ws !== null);
          if (newHost) {
            r.hostId = newHost.id;
            broadcast(r, 'host_changed', HostChangedPayload, {
              newHostId: newHost.id,
              newHostName: newHost.username,
            });
          } else {
            delete rooms[ctx.roomCode];
            console.log(`Room ${ctx.roomCode} deleted (host timeout)`);
          }
        }
      }, 5 * 60 * 1000);
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
  });
});

console.log(`Dare Listen Together server running on port ${PORT}`);
