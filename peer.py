#!/usr/bin/env python3

import argparse
import json
import socket
import threading
import time
import traceback
from typing import Dict, Tuple, List

LOCK = threading.Lock()

def now_ts():
    return int(time.time())

class PeerNode:
    def __init__(self, host: str, port: int, bootstrap: List[Tuple[str,int]]):
        self.host = host
        self.port = port
        self.addr = (host, port)

        self.known_peers = bootstrap[:] if bootstrap else []
        if self.addr in self.known_peers:
            self.known_peers.remove(self.addr)

        self.user_db: Dict[str, Dict] = {}
        self.online_map: Dict[str, Tuple[str,int]] = {}
        self.offline_store: Dict[str, List[Dict]] = {}
        self.session_user = None

        threading.Thread(target=self._server_loop, daemon=True).start()
        threading.Thread(target=self._maintain_loop, daemon=True).start()

        # Announce presence
        for p in self.known_peers:
            try:
                self._send_message(p, {"type": "peer_hello", "from": self._self_info()})
            except:
                pass

    def _self_info(self):
        return {"host": self.host, "port": self.port}

    def _server_loop(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.addr)
        s.listen(10)
        print(f"[{self.port}] Listening on {self.host}:{self.port}")

        while True:
            conn, addr = s.accept()
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn):
        with conn:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk: break
                data += chunk
                while b"\n" in data:
                    line, data = data.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode())
                        self._handle_message(msg)
                    except Exception:
                        traceback.print_exc()

    def _send_message(self, peer, msg):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(peer)
            s.sendall((json.dumps(msg) + "\n").encode())
        except:
            pass
        finally:
            s.close()

    def _gossip(self, msg):
        for peer in self.known_peers:
            if peer != self.addr:
                self._send_message(peer, msg)

    def _handle_message(self, msg):
        msg_type = msg.get("type")

        if msg_type == "peer_hello":
            peer = (msg["from"]["host"], msg["from"]["port"])
            if peer not in self.known_peers and peer != self.addr:
                self.known_peers.append(peer)
            self._send_message(peer, {"type": "peer_state", "peers": self.known_peers})

        elif msg_type == "peer_state":
            for p in msg.get("peers", []):
                if tuple(p) not in self.known_peers and tuple(p) != self.addr:
                    self.known_peers.append(tuple(p))

        elif msg_type == "register_user":
            self.user_db[msg["username"]] = msg["data"]
            self._gossip(msg)

        elif msg_type == "login_announce":
            self.online_map[msg["username"]] = (msg["host"], msg["port"])
            self._deliver_offline_messages(msg["username"])

        elif msg_type == "deliver_message":
            self._display_or_store(msg)

        elif msg_type == "direct_message":
            self._display_or_store(msg)

        else:
            print(f"[DEBUG] Unknown message received: {msg}")

    def _display_or_store(self, msg):
        to_user = msg.get("to_user")
        if not to_user:
            print("[WARNING] Message missing 'to_user':", msg)
            return

        if self.session_user == to_user:
            print(f"\n[{msg['from_user']}] -> you: {msg['text']}\n> ", end="")
        else:
            self.offline_store.setdefault(to_user, []).append(msg)

    def register(self, username, password, fullname):
        if username in self.user_db:
            print("Username exists.")
            return
        data = {"password": password, "fullname": fullname}
        self.user_db[username] = data
        self._gossip({"type": "register_user", "username": username, "data": data})
        print("Registered.")

    def login(self, username, password):
        if self.user_db.get(username, {}).get("password") != password:
            print("Invalid login.")
            return

        self.session_user = username
        self.online_map[username] = self.addr
        self._gossip({"type": "login_announce", "username": username, "host": self.host, "port": self.port})
        self._deliver_offline_messages(username)
        print("Logged in.")

    def _deliver_offline_messages(self, username):
        if username in self.offline_store:
            for msg in self.offline_store[username]:
                self._send_message(self.online_map[username], {"type": "deliver_message", **msg})
            self.offline_store[username] = []

    def send_message(self, to_user, text):
        if not self.session_user:
            print("Login first.")
            return

        msg = {
            "type": "deliver_message",
            "from_user": self.session_user,
            "to_user": to_user,
            "text": text
        }

        target = self.online_map.get(to_user)
        if target:
            self._send_message(target, msg)
            print("Delivered.")
        else:
            self._gossip(msg)
            self.offline_store.setdefault(to_user, []).append(msg)
            print("Recipient offline → message stored!")

    def _maintain_loop(self):
        while True:
            time.sleep(10)
            self._gossip({"type": "peer_state", "peers": self.known_peers})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--bootstrap", default="")
    args = ap.parse_args()

    bootstrap = []
    if args.bootstrap:
        host, port = args.bootstrap.split(":")
        bootstrap = [(host, int(port))]

    peer = PeerNode(args.host, args.port, bootstrap)

    while True:
        try:
            cmd = input("> ").strip().split()
        except EOFError:
            break

        if not cmd:
            continue

        if cmd[0] == "register" and len(cmd) >= 4:
            peer.register(cmd[1], cmd[2], " ".join(cmd[3:]))

        elif cmd[0] == "login" and len(cmd) == 3:
            peer.login(cmd[1], cmd[2])

        elif cmd[0] == "send" and len(cmd) >= 3:
            peer.send_message(cmd[1], " ".join(cmd[2:]))

        elif cmd[0] == "exit":
            print("Bye.")
            break

        else:
            print("Commands: register, login, send, exit")


if __name__ == "__main__":
    main()
