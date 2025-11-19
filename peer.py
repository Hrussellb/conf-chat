import argparse
import json
import socket
import threading
import time
import traceback
from typing import Dict, Tuple, List
import os

LOCK = threading.Lock()

def now_ts():
    return int(time.time())

USERS_FILE = "users.json"

class PeerNode:
    def __init__(self, host: str, port: int, bootstrap: List[Tuple[str,int]]):
        self.host = host
        self.port = port
        self.addr = (host, port)

        self.known_peers = bootstrap[:] if bootstrap else []
        if self.addr in self.known_peers:
            self.known_peers.remove(self.addr)

        # persistent user DB: username -> {password, fullname, created_ts, is_admin}
        self.user_db: Dict[str, Dict] = {}
        self.online_map: Dict[str, Tuple[str,int]] = {}
        self.offline_store: Dict[str, List[Dict]] = {}
        self.session_user = None

        self._load_users()

        threading.Thread(target=self._server_loop, daemon=True).start()
        threading.Thread(target=self._maintain_loop, daemon=True).start()

        # Announce presence
        for p in self.known_peers:
            try:
                self._send_message(p, {"type": "peer_hello", "from": self._self_info()})
            except:
                pass

    # ---------- persistence ----------
    def _load_users(self):
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r", encoding="utf-8") as f:
                    self.user_db = json.load(f)
        except Exception:
            traceback.print_exc()

    def _save_users(self):
        try:
            with LOCK:
                with open(USERS_FILE, "w", encoding="utf-8") as f:
                    json.dump(self.user_db, f)
        except Exception:
            traceback.print_exc()

    # ---------- networking ----------
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
                try:
                    chunk = conn.recv(4096)
                except Exception:
                    break
                if not chunk:
                    break
                data += chunk
                while b"\n" in data:
                    line, data = data.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode())
                        self._handle_message(msg)
                    except Exception:
                        traceback.print_exc()

    def _send_message(self, peer, msg):
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect(peer)
            s.sendall((json.dumps(msg) + "\n").encode())
        except Exception:
            # best-effort; ignore failures
            pass
        finally:
            if s:
                try:
                    s.close()
                except:
                    pass

    def _gossip(self, msg):
        for peer in self.known_peers:
            if peer != self.addr:
                self._send_message(peer, msg)

    # ---------- message handlers ----------
    def _handle_message(self, msg):
        msg_type = msg.get("type")

        if msg_type == "peer_hello":
            peer = (msg["from"]["host"], msg["from"]["port"])
            if peer not in self.known_peers and peer != self.addr:
                self.known_peers.append(peer)
            self._send_message(peer, {"type": "peer_state", "peers": self.known_peers})

        elif msg_type == "peer_state":
            for p in msg.get("peers", []):
                tpl = tuple(p)
                if tpl not in self.known_peers and tpl != self.addr:
                    self.known_peers.append(tpl)

        elif msg_type == "register_user":
            self.user_db[msg["username"]] = msg["data"]
            self._save_users()

        elif msg_type == "delete_user":
            username = msg.get("username")
            if username in self.user_db:
                del self.user_db[username]
                self._save_users()
            # remove any queued offline messages and online presence
            if username in self.offline_store:
                del self.offline_store[username]
            if username in self.online_map:
                del self.online_map[username]

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

        # if user deleted meanwhile, drop
        if to_user not in self.user_db:
            return

        if self.session_user == to_user:
            print(f"\n[{msg.get('from_user')}] -> you: {msg.get('text')}\n> ", end="")
        else:
            self.offline_store.setdefault(to_user, []).append(msg)

    # ---------- user operations ----------
    def register(self, username, password, fullname, is_admin=False):
        if username in self.user_db:
            print("Username exists.")
            return
        data = {
            "password": password,
            "fullname": fullname,
            "created_ts": now_ts(),
            "is_admin": bool(is_admin)
        }
        self.user_db[username] = data
        self._save_users()
        # Gossip registration so other peers add the user record (no password propagation secrecy here; adjust in real deployment)
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

    def delete_user(self, username, by_admin=False):
        # user can delete self; admin (by_admin=True) can delete any user
        if username not in self.user_db:
            print("No such user.")
            return
        if not by_admin:
            if self.session_user != username:
                print("You can only delete your own account unless you're an admin.")
                return
        # delete locally
        del self.user_db[username]
        self._save_users()
        if username in self.offline_store:
            del self.offline_store[username]
        if username in self.online_map:
            del self.online_map[username]
        # gossip deletion
        self._gossip({"type": "delete_user", "username": username})
        # if deleting own session, log out
        if self.session_user == username:
            self.session_user = None
        print(f"User '{username}' deleted.")

    def _deliver_offline_messages(self, username):
        if username in self.offline_store and username in self.online_map:
            for msg in list(self.offline_store[username]):
                # send message to user's current online addr
                target = self.online_map.get(username)
                if target:
                    self._send_message(target, {"type": "deliver_message", **msg})
            self.offline_store[username] = []

    # ---------- messaging ----------
    def send_message(self, to_user, text):
        if not self.session_user:
            print("Login first.")
            return
        if to_user not in self.user_db:
            print("No such recipient.")
            return

        msg = {
            "type": "deliver_message",
            "from_user": self.session_user,
            "to_user": to_user,
            "text": text,
            "ts": now_ts()
        }

        target = self.online_map.get(to_user)
        if target:
            self._send_message(target, msg)
            print("Delivered.")
        else:
            # gossip to let other peers store delivery if they know recipient offline
            self._gossip(msg)
            self.offline_store.setdefault(to_user, []).append(msg)
            print("Recipient offline → message stored!")

    def send_multi(self, recipients: List[str], text: str):
        if not self.session_user:
            print("Login first.")
            return
        # sanitize recipients
        recipients = [r.strip() for r in recipients if r.strip()]
        if not recipients:
            print("No recipients given.")
            return

        for to_user in recipients:
            if to_user not in self.user_db:
                print(f"No such recipient: {to_user} (skipping)")
                continue
            msg = {
                "type": "deliver_message",
                "from_user": self.session_user,
                "to_user": to_user,
                "text": text,
                "ts": now_ts()
            }
            target = self.online_map.get(to_user)
            if target:
                self._send_message(target, msg)
                print(f"Delivered to {to_user}.")
            else:
                self._gossip(msg)
                self.offline_store.setdefault(to_user, []).append(msg)
                print(f"{to_user} offline → message stored!")

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

    print("Commands: register, login, send, sendmulti, delete, promote, exit")
    while True:
        try:
            raw = input("> ")
        except EOFError:
            break

        if raw is None:
            continue
        cmd = raw.strip().split()
        if not cmd:
            continue

        if cmd[0] == "register" and len(cmd) >= 4:
            # optional last argument 'admin' makes the account admin (only locally effective unless gossiping trusted)
            is_admin = False
            if cmd[-1].lower() == "admin":
                is_admin = True
                fullname = " ".join(cmd[3:-1])
            else:
                fullname = " ".join(cmd[3:])
            peer.register(cmd[1], cmd[2], fullname, is_admin=is_admin)

        elif cmd[0] == "login" and len(cmd) == 3:
            peer.login(cmd[1], cmd[2])

        elif cmd[0] == "send" and len(cmd) >= 3:
            peer.send_message(cmd[1], " ".join(cmd[2:]))

        elif cmd[0] == "sendmulti" and len(cmd) >= 3:
            # sendmulti user1,user2,user3 message...
            recipients_raw = cmd[1]
            recipients = recipients_raw.split(",")
            peer.send_multi(recipients, " ".join(cmd[2:]))

        elif cmd[0] == "delete" and len(cmd) >= 2:
            # delete <username> ; regular users can delete themselves only. Admins can delete any user.
            target = cmd[1]
            by_admin = False
            if peer.session_user:
                if peer.user_db.get(peer.session_user, {}).get("is_admin"):
                    by_admin = True
            peer.delete_user(target, by_admin=by_admin)

        elif cmd[0] == "promote" and len(cmd) == 2:
            # promote <username> ; only local admin can promote (simple demo)
            target = cmd[1]
            if not peer.session_user:
                print("Login first.")
                continue
            if not peer.user_db.get(peer.session_user, {}).get("is_admin"):
                print("Only admin accounts can promote.")
                continue
            if target not in peer.user_db:
                print("No such user.")
                continue
            peer.user_db[target]["is_admin"] = True
            peer._save_users()
            # gossip promotion by re-gossiping register_user for updated data
            peer._gossip({"type": "register_user", "username": target, "data": peer.user_db[target]})
            print(f"{target} promoted to admin.")

        elif cmd[0] == "exit":
            print("Bye.")
            break

        else:
            print("Commands: register, login, send, sendmulti, delete, promote, exit")


if __name__ == "__main__":
    main()

