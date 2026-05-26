import os
import time
import requests
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NODE_ID = int(os.getenv("NODE_ID", "0"))
PEERS_ENV = os.getenv("PEERS", "")
PEERS = PEERS_ENV.split(",") if PEERS_ENV else []

def get_peer_id(peer_url):
    try:
        host = peer_url.split("://")[1].split(":")[0]
        return int(host.split("-")[1])
    except:
        return 0

peer_info = {get_peer_id(url): url for url in PEERS if url}

current_leader = None
election_in_progress = False

def wait_for_victory():
    time.sleep(5)
    if election_in_progress:
        logger.info(f"Node {NODE_ID} timed out waiting for victory, restarting election")
        start_election()

def start_election():
    global election_in_progress, current_leader
    election_in_progress = True
    current_leader = None
    
    higher_peers = {pid: url for pid, url in peer_info.items() if pid > NODE_ID}
    logger.info(f"Node {NODE_ID} starting election. Higher peers: {list(higher_peers.keys())}")
    
    if not higher_peers:
        declare_victory()
        return

    responses = 0
    for pid, url in higher_peers.items():
        try:
            res = requests.post(f"{url}/api/election", json={"sender_id": NODE_ID}, timeout=2)
            if res.status_code == 200:
                responses += 1
        except requests.RequestException:
            pass
            
    if responses == 0:
        declare_victory()
    else:
        threading.Thread(target=wait_for_victory, daemon=True).start()

def handle_election_message(sender_id):
    global election_in_progress
    logger.info(f"Node {NODE_ID} received election from {sender_id}")
    if sender_id < NODE_ID:
        if not election_in_progress:
            threading.Thread(target=start_election, daemon=True).start()
        return True
    return False

def declare_victory():
    global current_leader, election_in_progress
    logger.info(f"Node {NODE_ID} declaring victory!")
    current_leader = NODE_ID
    election_in_progress = False
    
    for pid, url in peer_info.items():
        try:
            requests.post(f"{url}/api/victory", json={"leader_id": NODE_ID}, timeout=2)
        except requests.RequestException:
            pass

def heartbeat_check():
    global current_leader, election_in_progress
    while True:
        time.sleep(5)
        if current_leader and current_leader != NODE_ID and not election_in_progress:
            leader_url = peer_info.get(current_leader)
            if leader_url:
                try:
                    res = requests.get(f"{leader_url}/health", timeout=2)
                    if res.status_code != 200:
                        raise Exception("Bad status")
                except Exception:
                    logger.warning(f"Node {NODE_ID} detects leader {current_leader} is down")
                    threading.Thread(target=start_election, daemon=True).start()
        elif not current_leader and not election_in_progress:
            threading.Thread(target=start_election, daemon=True).start()
