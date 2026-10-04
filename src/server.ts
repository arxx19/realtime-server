console.log("File loaded, starting imports...");

import { WebSocketServer, type WebSocket } from "ws";
import * as Y from "yjs";
import { encoding, decoding } from "lib0";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";

const PORT = 1234;
const docs = new Map<
  string,
  { doc: Y.Doc; awareness: awarenessProtocol.Awareness; conns: Set<WebSocket> }
>();

function getRoom(name: string) {
  let room = docs.get(name);
  if (!room) {
    const doc = new Y.Doc();
    const awareness = new awarenessProtocol.Awareness(doc);
    room = { doc, awareness, conns: new Set() };
    docs.set(name, room);
  }
  return room;
}

const wss = new WebSocketServer({ port: PORT });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url ?? "", "http://localhost");
  const roomName = url.pathname.slice(1) || "default";
  const room = getRoom(roomName);
  room.conns.add(ws);

  function send(buf: Uint8Array) {
    if (ws.readyState === ws.OPEN) ws.send(buf);
  }

  // Tell the new client what we have so far
  const syncEncoder = encoding.createEncoder();
  encoding.writeVarUint(syncEncoder, 0); // messageSync
  syncProtocol.writeSyncStep1(syncEncoder, room.doc);
  send(encoding.toUint8Array(syncEncoder));

  const awarenessStates = room.awareness.getStates();
  if (awarenessStates.size > 0) {
    const awEncoder = encoding.createEncoder();
    encoding.writeVarUint(awEncoder, 1); // messageAwareness
    encoding.writeVarUint8Array(
      awEncoder,
      awarenessProtocol.encodeAwarenessUpdate(
        room.awareness,
        Array.from(awarenessStates.keys()),
      ),
    );
    send(encoding.toUint8Array(awEncoder));
  }

  ws.on("message", (data: Buffer) => {
    const decoder = decoding.createDecoder(new Uint8Array(data));
    const messageType = decoding.readVarUint(decoder);

    if (messageType === 0) {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, 0);
      syncProtocol.readSyncMessage(decoder, encoder, room.doc, ws);
      if (encoding.length(encoder) > 1) send(encoding.toUint8Array(encoder));
    } else if (messageType === 1) {
      awarenessProtocol.applyAwarenessUpdate(
        room.awareness,
        decoding.readVarUint8Array(decoder),
        ws,
      );
    }
  });

  // Broadcast every doc change to everyone else in the room
  const updateHandler = (update: Uint8Array, origin: unknown) => {
    if (origin === ws) return;
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);
    syncProtocol.writeUpdate(encoder, update);
    const message = encoding.toUint8Array(encoder);
    room.conns.forEach((conn) => {
      if (conn !== ws && conn.readyState === conn.OPEN) conn.send(message);
    });
  };
  room.doc.on("update", updateHandler);

  const awarenessHandler = (
    {
      added,
      updated,
      removed,
    }: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ) => {
    const changed = added.concat(updated, removed);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 1);
    encoding.writeVarUint8Array(
      encoder,
      awarenessProtocol.encodeAwarenessUpdate(room.awareness, changed),
    );
    const message = encoding.toUint8Array(encoder);
    room.conns.forEach((conn) => {
      if (conn !== origin && conn.readyState === conn.OPEN) conn.send(message);
    });
  };
  room.awareness.on("update", awarenessHandler);

  ws.on("close", () => {
    room.conns.delete(ws);
    room.doc.off("update", updateHandler);
    room.awareness.off("update", awarenessHandler);
    awarenessProtocol.removeAwarenessStates(
      room.awareness,
      [room.doc.clientID],
      null,
    );
    if (room.conns.size === 0) docs.delete(roomName);
  });
});

console.log(`Yjs WebSocket server running on ws://localhost:${PORT}`);
