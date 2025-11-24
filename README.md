# Conf-Chat (Python P2P version)

## Overview
Conf-Chat is a simple peer-to-peer (P2P) chat and message-sharing system.
Each peer is a standalone Python program that can:
- Register/login users (stored locally, no central server)
- Add and accept friends
- Send and receive direct messages (online/offline)
- Create and participate in conference chats
- Discover peers dynamically
- Stores message if user is not online.
- sadly does not allow multiple messaging of more than one users at a time.
- does not remember users 

## Installation
Requirements: Python 3.8+

## How to use 
1. open the powershell or terminal
2.  input this code (using this on your computer might be different than mine) "cd "C:\Users\hunte\OneDrive\Desktop\New folder (2)"
python peer.py --port 8001"
3.  for  additional people use a seprate terminal window and use this (again code might be different based on the location of the peer.py file location on the computer "cd "C:\Users\hunte\OneDrive\Desktop\New folder (2)"
python peer.py --port 8002 --bootstrap 127.0.0.1:8001" for additional people use 8003 and so on at the port section.
4. list of commands
| Command       | Usage                                       | Example                             |
| ------------- | ------------------------------------------- | ----------------------------------- |
| Register user | `register <username> <password> <fullname>` | `register alice p1 "Alice Example"` | registers a person as a user 
| Login user    | `login <username> <password>`               | `login alice p1`                    | login as the user with appropriate user information, note you do not automatically log in when you register 
| Send message  | `send <username> <message>`                 | `send bob Hello!`                   |sends messages to people, if user is offline, then message will be saved and then sent when they log in
| Exit peer     | `exit`                                      | Stops only the current peer         |exits the session. 

##Known issues 
| Issue                                        | Description                                     | Why it happens                                                   | Workaround                                                         |
| -------------------------------------------- | ----------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------ |
| Duplicate offline messages                   | Some messages may appear more than once         | Gossip broadcasts allow multiple peers to store pending messages | No full fix yet — duplicates safe to ignore                        |
| Peers not discovering each other instantly   | Gossip takes a few seconds to spread peer lists | Asynchronous background updates                                  | Wait 5–10 seconds or run send commands again                       |
| Unknown peer errors when many peers leave    | Some peers may try sending to an offline peer   | No robust “dead peer” detection yet                              | Restart peers to refresh online list                               |
| No encryption/security                       | All messages are plaintext                      | Simplified academic implementation                               | Could add TLS later                                                |



