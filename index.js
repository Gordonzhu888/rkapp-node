import http from "node:http";
import net from "node:net";
import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8080;
const UUID = (process.env.VLESS_UUID || "558b527d-99ac-4649-befa-7edc5db1a2f9").toLowerCase();
const WS_PATH = process.env.WS_PATH || "/762a48b9";
const SUB_PATH = process.env.SUB_PATH || "/dca64addf9c3";
const REMARK = process.env.NODE_REMARK || "北美-FadeHost";
const HOSTNAME = process.env.NODE_HOST || "rkapp.fadehost.app";

function uuidBytes(uuid) {
  return Buffer.from(uuid.replace(/-/g, ""), "hex");
}
const UUID_BYTES = uuidBytes(UUID);

function nodeLink() {
  const params = new URLSearchParams({
    encryption: "none",
    security: "tls",
    type: "ws",
    host: HOSTNAME,
    path: WS_PATH,
  });
  return `vless://${UUID}@${HOSTNAME}:443?${params.toString()}#${encodeURIComponent(REMARK)}`;
}

const server = http.createServer((req, res) => {
  if (req.url === SUB_PATH) {
    const body = Buffer.from(nodeLink(), "utf8").toString("base64");
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    res.end(body);
    return;
  }
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("ok\n");
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname !== WS_PATH) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
});

wss.on("connection", (ws) => {
  let remote = null;
  let headerParsed = false;
  let pending = [];

  ws.on("message", (raw) => {
    const data = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
    if (headerParsed) {
      if (remote && remote.writable) remote.write(data);
      else pending.push(data);
      return;
    }
    headerParsed = true;
    try {
      // VLESS header: version(1) + uuid(16) + addonLen(1) + cmd(1) + port(2) + addrType(1) + addr
      if (data.length < 24) throw new Error("short header");
      if (!data.subarray(1, 17).equals(UUID_BYTES)) throw new Error("bad uuid");
      let offset = 17;
      const addonLen = data[offset];
      offset += 1 + addonLen;
      const cmd = data[offset];
      offset += 1;
      if (cmd !== 1) throw new Error("only tcp supported");
      const port = data.readUInt16BE(offset);
      offset += 2;
      const addrType = data[offset];
      offset += 1;
      let host;
      if (addrType === 1) {
        host = [...data.subarray(offset, offset + 4)].join(".");
        offset += 4;
      } else if (addrType === 2) {
        const len = data[offset];
        offset += 1;
        host = data.subarray(offset, offset + len).toString("utf8");
        offset += len;
      } else if (addrType === 3) {
        const parts = [];
        for (let i = 0; i < 8; i++) {
          parts.push(data.readUInt16BE(offset + i * 2).toString(16));
        }
        host = parts.join(":");
        offset += 16;
      } else {
        throw new Error("bad addr type");
      }
      const payload = data.subarray(offset);
      remote = net.connect(port, host, () => {
        ws.send(Buffer.concat([Buffer.from([data[0], 0])]));
        if (payload.length) remote.write(payload);
        for (const chunk of pending) remote.write(chunk);
        pending = [];
      });
      remote.on("data", (chunk) => {
        if (ws.readyState === ws.OPEN) ws.send(chunk);
      });
      remote.on("error", (e) => {
        try { ws.close(); } catch {}
      });
      remote.on("close", () => {
        try { ws.close(); } catch {}
      });
    } catch (e) {
      try { ws.close(); } catch {}
    }
  });

  ws.on("close", () => {
    if (remote) remote.destroy();
  });
  ws.on("error", () => {
    if (remote) remote.destroy();
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`listening on ${PORT}`);
});
