"""
peer.py -- Simple P2P Conf-Chat starter implementation (Python)
Usage:
    python3 peer.py --port 8001
    python3 peer.py --port 8002 --bootstrap 127.0.0.1:8001
Open multiple terminals with different ports to simulate multiple peers.
Each peer provides an interactive CLI.
"""

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

        # Known peers
        self.known_peers: List[Tuple[str,int]] = []
        for b in bootstrap:
            if b != self.addr:
                self.known_peers.append(b)

        self.user_db: Dict[str, Dict] = {}      # username -> {password, fullname}
        self.online_map: Dict[str, Tuple[str,int]] = {}  # username -> addr
        self.friend_map: Dict[str, set] = {}    # username -> set of friends
        self.offline_store: Dict[str, List[Dict]] = {}
        self.chats: Dict[str, Dict] = {}
        self.session_user: str = None

        # Start background listener
        threading.Thread(target=self._server_loop, daemon=True).start()
        for peer in list(self.known_peers):
            try:
                self._send_message(peer, {"type":"peer_hello", "from":{"host":self.host,"port":self.port}})
            except Exception:
                pass

        threading.Thread(target=self._maintain_loop, daemon=True).start()

    def _server_loop(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(10)
        print(f"[{self.port}] Listening on {self.host}:{self.port}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self._handle_conn, args=(conn,addr), daemon=True).start()

    def _handle_conn(self, conn, addr):
        with conn:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                while b"\n" in data:
                    line, data = data.split(b"\n",1)
                    try:
                        msg = json.loads(line.decode())
                        self._handle_message(msg, addr)
                    except Exception:
                        traceback.print_exc()

    def _send_message(self, peer, msg):
        host, port = peer
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        try:
            s.connect((host, port))
            s.sendall((json.dumps(msg)+"\n").encode())
        finally:
            s.close()

    # === Message handlers ===
    def _handle_message(self, msg, addr):
        t = msg.get("type")
        if t == "peer_hello":
            p = msg["from"]
            peer = (p["host"], p["port"])
            if peer not in self.known_peers and peer != self.addr:
                self.known_peers.append(peer)
            self._send_message(peer, {"type":"peer_state","known_peers":self.known_peers})
        elif t == "peer_state":
            for p in msg.get("known_peers", []):
                peer = tuple(p) if isinstance(p, list) else (p["host"],p["port"])
                if peer not in self.known_peers and peer != self.addr:
                    self.known_peers.append(peer)
        elif t == "register_user":
            u = msg["username"]; d = msg["data"]
            if u not in self.user_db: self.user_db[u] = d
            self._gossip(msg, exclude=[addr])
        elif t == "login_announce":
            u = msg["username"]; h = msg["host"]; p = msg["port"]
            self.online_map[u] = (h,p)
            if u in self.offline_store:
                for pm in self.offline_store[u]:
                    self._send_message((h,p), {"type":"deliver_message",**pm,"to_user":u})
                self.offline_store[u] = []
        elif t == "deliver_message":
            if self.session_user == msg["to_user"]:
                print(f"\n[{msg['from_user']}] -> you: {msg['text']}\n> ", end="")
            else:
                self.offline_store.setdefault(msg["to_user"],[]).append(msg)
        elif t == "direct_message":
            if self.session_user == msg["to_user"]:
                print(f"\n[{msg['from_user']}] -> you: {msg['text']}\n> ", end="")
            else:
                self.offline_store.setdefault(msg["to_user"],[]).append(msg)
        elif t == "chat_message":
            cid = msg["chat_id"]; from_user = msg["from_user"]
            text = msg["text"]
            if cid in self.chats:
                ch = self.chats[cid]
                ch["messages"].append({"from":from_user,"text":text})
                if self.session_user in ch["participants"]:
                    print(f"\n[{ch['name']}] {from_user}: {text}\n> ", end="")
            else:
                self.offline_store.setdefault("chats",[]).append(msg)
        elif t == "create_chat":
            self.chats[msg["chat_id"]] = msg["info"]
        else:
            pass

    def _gossip(self, msg, exclude=[]):
        for p in self.known_peers:
            if p not in exclude and p != self.addr:
                try: self._send_message(p,msg)
                except: pass

    # === CLI functions ===
    def register(self,u,pw,name):
        if u in self.user_db:
            print("Username exists."); return
        self.user_db[u]={"password":pw,"fullname":name}
        self._gossip({"type":"register_user","username":u,"data":self.user_db[u]})
        print("Registered.")

    def login(self,u,pw):
        if u not in self.user_db or self.user_db[u]["password"]!=pw:
            print("Invalid."); return
        self.session_user=u
        self.online_map[u]=self.addr
        self._gossip({"type":"login_announce","username":u,"host":self.host,"port":self.port})
        print("Logged in.")

    def send_message(self,to,text):
        if not self.session_user: print("Login first"); return
        if to in self.online_map:
            self._send_message(self.online_map[to],{"type":"deliver_message","from_user":self.session_user,"text":text})
            print("Delivered.")
        else:
            self.offline_store.setdefault(to,[]).append({"from_user":self.session_user,"text":text})
            self._gossip({"type":"direct_message","from_user":self.session_user,"to_user":to,"text":text})
            print("Stored offline and broadcast.")

    def create_chat(self,cid,name,participants):
        if not self.session_user: print("Login first"); return
        info={"name":name,"participants":participants+[self.session_user],"messages":[]}
        self.chats[cid]=info
        self._gossip({"type":"create_chat","chat_id":cid,"info":info})
        print("Chat created.")
    def send_chat_message(self,cid,text):
        if not self.session_user: print("Login first"); return
        if cid not in self.chats: print("Unknown chat"); return
        self._gossip({"type":"chat_message","chat_id":cid,"from_user":self.session_user,"text":text})
        print("Broadcast to chat.")
    def _maintain_loop(self):
        while True:
            time.sleep(10)
            try: self._gossip({"type":"peer_state","known_peers":self.known_peers})
            except: pass
def repl(node):
    helptext="""
Commands:
 register <username> <password> <fullname>
 login <username> <password>
 send <username> <message>
 create_chat <id> <name> <user1,user2,...>
 chat_send <id> <message>
 help
 exit
"""
    print(helptext)
    while True:
        cmd=input("> ").strip().split()
        if not cmd: continue
        if cmd[0]=="register" and len(cmd)>=4:
            node.register(cmd[1],cmd[2]," ".join(cmd[3:]))
        elif cmd[0]=="login" and len(cmd)==3:
            node.login(cmd[1],cmd[2])
        elif cmd[0]=="send" and len(cmd)>=3:
            node.send_message(cmd[1]," ".join(cmd[2:]))
        elif cmd[0]=="create_chat" and len(cmd)>=4:
            node.create_chat(cmd[1],cmd[2],cmd[3].split(","))
        elif cmd[0]=="chat_send" and len(cmd)>=3:
            node.send_chat_message(cmd[1]," ".join(cmd[2:]))
        elif cmd[0]=="help": print(helptext)
        elif cmd[0]=="exit": break
if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--host",default="127.0.0.1")
    ap.add_argument("--port",type=int,required=True)
    ap.add_argument("--bootstrap",default="")
    a=ap.parse_args()
    boots=[tuple(x.split(":")) for x in a.bootstrap.split(",") if x]
    boots=[(h,int(p)) for h,p in boots]
    n=PeerNode(a.host,a.port,boots)
    repl(n)
