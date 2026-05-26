from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate
from pydantic import BaseModel
import threading
import time
import os
from src import election

class ElectionMessage(BaseModel):
    sender_id: int

class VictoryMessage(BaseModel):
    leader_id: int

# Stagger startup to avoid DB table creation race conditions
time.sleep(int(os.getenv("NODE_ID", "1")))
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    print(f"DB creation error (likely race condition, safe to ignore): {e}")

app = FastAPI()


@app.on_event("startup")
def startup_event():
    threading.Thread(target=election.heartbeat_check, daemon=True).start()

@app.post("/api/election")
def receive_election(msg: ElectionMessage):
    if election.handle_election_message(msg.sender_id):
        return {"status": "ok"}
    else:
        return {"status": "ignored"}

@app.post("/api/victory")
def receive_victory(msg: VictoryMessage):
    election.current_leader = msg.leader_id
    election.election_in_progress = False
    election.logger.info(f"Node {election.NODE_ID} acknowledges node {msg.leader_id} as leader")
    return {"status": "ok"}

@app.get("/leader")
def get_leader():
    return {"leader": election.current_leader, "leader_id": election.current_leader}

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}

@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node

@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()

@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node

@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
