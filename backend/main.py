from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Optional
import asyncio, base64, json, logging, os, random, secrets

from azure.core.exceptions import AzureError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Boolean, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import redis
from redis.exceptions import RedisError

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./mindclash.db')
engine_options = {'connect_args': {'check_same_thread': False}} if DATABASE_URL.startswith('sqlite') else {
    'connect_args': {'connect_timeout': 8},
    'pool_pre_ping': True,
    'pool_size': 3,
    'max_overflow': 0,
    'pool_recycle': 1800,
}
engine = create_engine(DATABASE_URL, **engine_options)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()
SECRET = os.getenv('JWT_SECRET', '').strip()
if len(SECRET.encode()) < 32 or SECRET in {'change-this-secret-before-deployment', 'replace-with-a-long-random-secret'}:
    raise RuntimeError('Set JWT_SECRET to a randomly generated key of at least 32 bytes')
ALGORITHM = 'HS256'
pwd = CryptContext(schemes=['bcrypt'], deprecated='auto')
oauth = OAuth2PasswordBearer(tokenUrl='/auth/login')
REDIS_URL = os.environ['REDIS_URL']
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)

class User(Base):
    __tablename__='users'
    id=Column(Integer, primary_key=True)
    username=Column(String(32), unique=True, nullable=False)
    email=Column(String(255), unique=True, nullable=False)
    password_hash=Column(String(255), nullable=False)
    rating=Column(Integer, default=1000)
    wins=Column(Integer, default=0)
    losses=Column(Integer, default=0)
    created_at=Column(DateTime, default=datetime.utcnow)

class QueueEntry(Base):
    __tablename__='queue'
    id=Column(Integer, primary_key=True)
    user_id=Column(Integer, ForeignKey('users.id'), unique=True)
    rating=Column(Integer)
    arena=Column(String(32))
    created_at=Column(DateTime, default=datetime.utcnow)

class Match(Base):
    __tablename__='matches'
    id=Column(String(40), primary_key=True)
    player1=Column(Integer, ForeignKey('users.id'))
    player2=Column(Integer, ForeignKey('users.id'))
    arena=Column(String(32))
    status=Column(String(20), default='active')
    score1=Column(Integer, default=0)
    score2=Column(Integer, default=0)
    current_question=Column(Integer, default=0)
    winner_id=Column(Integer, nullable=True)
    created_at=Column(DateTime, default=datetime.utcnow)
    finished_at=Column(DateTime, nullable=True)

class Submission(Base):
    __tablename__='submissions'
    id=Column(Integer, primary_key=True)
    match_id=Column(String(40), ForeignKey('matches.id'))
    user_id=Column(Integer, ForeignKey('users.id'))
    question_index=Column(Integer)
    answer=Column(Text)
    correct=Column(Boolean, default=False)
    created_at=Column(DateTime, default=datetime.utcnow)

app=FastAPI(title='MindClash API', version='0.1.0')
app.add_middleware(CORSMiddleware, allow_origins=os.getenv('CORS_ORIGINS','http://localhost:5173').split(','), allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

_schema_initialized = False
_schema_lock = Lock()

def initialize_database():
    global _schema_initialized
    if _schema_initialized:
        return
    with _schema_lock:
        if not _schema_initialized:
            Base.metadata.create_all(bind=engine)
            _schema_initialized = True

if os.getenv('MINDCLASH_USE_QUEUE_WORKER') != 'true':
    initialize_database()

class MatchSocketManager:
    def __init__(self):
        self.connections = {}

    async def connect(self, match_id, user_id, websocket):
        await websocket.accept()
        self.connections.setdefault(match_id, []).append((user_id, websocket))

    def disconnect(self, match_id, websocket):
        peers = self.connections.get(match_id, [])
        self.connections[match_id] = [(uid, ws) for uid, ws in peers if ws is not websocket]
        if not self.connections[match_id]:
            self.connections.pop(match_id, None)

    async def publish(self, match, session):
        peers = list(self.connections.get(match.id, []))
        for uid, ws in peers:
            try:
                player = session.get(User, uid)
                await ws.send_json({'type': 'match_update', 'match': get_match_payload(match, player, session)})
            except Exception:
                self.disconnect(match.id, ws)

manager = MatchSocketManager()

# A single Lua script pairs the closest eligible player and removes both queue entries
# atomically. Redis is the source of truth for the matchmaking queue.
PAIR_SCRIPT = """
local queue = KEYS[1]
local times = KEYS[2]
local self_id = ARGV[1]
local rating = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local base_window = tonumber(ARGV[4])
local stale_before = now - 600
local all_times = redis.call('HGETALL', times)
for i=1,#all_times,2 do
  if tonumber(all_times[i+1]) < stale_before then
    redis.call('ZREM', queue, all_times[i])
    redis.call('HDEL', times, all_times[i])
  end
end
if not redis.call('ZSCORE', queue, self_id) then
  redis.call('ZADD', queue, rating, self_id)
  redis.call('HSET', times, self_id, now)
end
local joined = tonumber(redis.call('HGET', times, self_id) or now)
local waited = math.max(0, now - joined)
local window = math.max(base_window, math.floor(waited / 20) * 50)
local candidates = redis.call('ZRANGEBYSCORE', queue, rating-window, rating+window, 'WITHSCORES')
local best_id = nil
local best_diff = nil
for i=1,#candidates,2 do
  local candidate = candidates[i]
  if candidate ~= self_id then
    local diff = math.abs(tonumber(candidates[i+1]) - rating)
    if (not best_diff) or diff < best_diff then
      best_id = candidate
      best_diff = diff
    end
  end
end
if best_id then
  redis.call('ZREM', queue, self_id, best_id)
  redis.call('HDEL', times, self_id, best_id)
  redis.call('SET', 'mindclash:paired:'..self_id, best_id, 'EX', 30)
  redis.call('SET', 'mindclash:paired:'..best_id, self_id, 'EX', 30)
  return {best_id, tostring(window), tostring(waited)}
end
redis.call('EXPIRE', queue, 900)
redis.call('EXPIRE', times, 900)
return {'', tostring(window), tostring(waited)}
"""

def db():
    initialize_database()
    s=SessionLocal()
    try: yield s
    finally: s.close()

def enqueue_bot_match(match_id, bot_id, player_rating):
    queue_name = 'mindclash-bot-matches'
    connection_string = os.getenv('AzureWebJobsStorage')
    if connection_string:
        queue = QueueClient.from_connection_string(connection_string, queue_name)
        credential = None
    else:
        account_name = os.environ['AzureWebJobsStorage__accountName']
        credential = DefaultAzureCredential()
        queue = QueueClient(
            account_url=f'https://{account_name}.queue.core.windows.net',
            queue_name=queue_name,
            credential=credential,
        )
    try:
        try:
            queue.create_queue()
        except ResourceExistsError:
            pass
        message = json.dumps({
            'match_id': match_id,
            'bot_id': bot_id,
            'player_rating': player_rating,
        }).encode('utf-8')
        queue.send_message(base64.b64encode(message).decode('ascii'))
    finally:
        queue.close()
        if credential:
            credential.close()

def token_for(user):
    return jwt.encode({'sub':str(user.id),'exp':datetime.now(timezone.utc)+timedelta(days=7)}, SECRET, algorithm=ALGORITHM)

def current_user(token:str=Depends(oauth), s:Session=Depends(db)):
    try: uid=int(jwt.decode(token, SECRET, algorithms=[ALGORITHM])['sub'])
    except (JWTError, ValueError, KeyError): raise HTTPException(401,'Invalid or expired token')
    user=s.get(User,uid)
    if not user: raise HTTPException(401,'User not found')
    return user

class RegisterIn(BaseModel):
    username:str=Field(min_length=3,max_length=32, pattern=r'^[a-zA-Z0-9_]+$')
    email:str
    password:str=Field(min_length=8,max_length=72)
class LoginIn(BaseModel): email:str; password:str
class QueueIn(BaseModel): arena:str
class AnswerIn(BaseModel): question_index:int; answer:str

# Answers are deliberately stored server-side. Replace/expand this bank before public launch.
QUESTIONS={
'dsa':[
 {'prompt':'What is the time complexity of binary search on a sorted array?','options':['O(1)','O(log n)','O(n)','O(n log n)'],'answer':'O(log n)','explanation':'Each comparison halves the remaining search interval.'},
 {'prompt':'Which data structure is typically used for BFS?','options':['Stack','Queue','Heap','Hash set'],'answer':'Queue','explanation':'BFS explores vertices in first-in, first-out order.'},
 {'prompt':'What is the average-case lookup time in a hash table?','options':['O(log n)','O(n)','O(1)','O(n log n)'],'answer':'O(1)','explanation':'With a good hash function and controlled load factor, lookup is expected constant time.'},
 {'prompt':'Which traversal of a BST returns keys in sorted order?','options':['Preorder','Inorder','Postorder','Level order'],'answer':'Inorder','explanation':'Left, root, right visits BST keys in ascending order.'},
 {'prompt':'Which algorithm finds shortest paths from one source when all edge weights are non-negative?','options':['Dijkstra','Kruskal','DFS','Topological sort'],'answer':'Dijkstra','explanation':'Dijkstra greedily finalizes the closest unsettled vertex.'},
 {'prompt':'What is the worst-case time complexity of merge sort?','options':['O(n)','O(log n)','O(n log n)','O(n²)'],'answer':'O(n log n)','explanation':'There are log n levels, each doing O(n) merging work.'},
 {'prompt':'Which technique is most associated with 0/1 Knapsack?','options':['Dynamic programming','Binary search','BFS','Union-find'],'answer':'Dynamic programming','explanation':'DP tracks best value for each capacity and item prefix.'},
 {'prompt':'What does a disjoint-set union structure support efficiently?','options':['Range minimum','Connectivity queries','String matching','Sorting'],'answer':'Connectivity queries','explanation':'DSU supports find and union operations.'},
 {'prompt':'A min-heap exposes which element at its root?','options':['Largest','Smallest','Most recent','Median'],'answer':'Smallest','explanation':'The min-heap property places the minimum at the root.'},
 {'prompt':'What is the space complexity of an iterative DFS using an explicit stack in the worst case?','options':['O(1)','O(log n)','O(V)','O(V²)'],'answer':'O(V)','explanation':'The stack may hold up to O(V) vertices.'}],
'probability':[
 {'prompt':'A fair coin is tossed twice. What is the probability of exactly one head?','options':['1/4','1/2','3/4','1'],'answer':'1/2','explanation':'HT and TH are 2 of the 4 equally likely outcomes.'},
 {'prompt':'A fair six-sided die is rolled. What is the probability of getting a prime number?','options':['1/3','1/2','2/3','5/6'],'answer':'1/2','explanation':'The prime faces are 2, 3 and 5: three of six.'},
 {'prompt':'Two fair dice are rolled. What is the probability their sum is 7?','options':['1/12','1/9','1/6','1/4'],'answer':'1/6','explanation':'Six of 36 ordered outcomes sum to 7.'},
 {'prompt':'A bag has 3 red and 2 blue balls. One is drawn. Probability it is blue?','options':['1/5','2/5','3/5','1/2'],'answer':'2/5','explanation':'There are 2 blue balls among 5 total.'},
 {'prompt':'If P(A)=0.4 and P(B)=0.5 and A,B are independent, what is P(A and B)?','options':['0.1','0.2','0.4','0.9'],'answer':'0.2','explanation':'For independent events, multiply probabilities.'},
 {'prompt':'A fair coin is tossed until the first head. What is the probability the first head occurs on toss 3?','options':['1/8','1/4','1/2','3/8'],'answer':'1/8','explanation':'The sequence is TTH, with probability (1/2)^3.'},
 {'prompt':'A card is drawn from a standard deck. Probability it is an ace?','options':['1/52','1/26','1/13','1/4'],'answer':'1/13','explanation':'There are 4 aces among 52 cards.'},
 {'prompt':'If an event has probability 0.7, what is the probability it does not occur?','options':['0.2','0.3','0.7','1.7'],'answer':'0.3','explanation':'Complement probability is 1 - 0.7.'},
 {'prompt':'A fair die is rolled twice. Probability of getting at least one 6?','options':['1/6','5/18','11/36','1/3'],'answer':'11/36','explanation':'1 - P(no six) = 1 - (5/6)^2 = 11/36.'},
 {'prompt':'The expected value of a fair six-sided die roll is?','options':['3','3.5','4','4.5'],'answer':'3.5','explanation':'(1+2+3+4+5+6)/6 = 3.5.'}],
'logic':[
 {'prompt':'You have 8 identical-looking balls; one is heavier. Minimum balance-scale weighings needed to guarantee finding it?','options':['1','2','3','4'],'answer':'2','explanation':'Split 3-3; then weigh 1-1 within the heavier group (or remaining pair).'},
 {'prompt':'A farmer has 17 sheep. All but 9 run away. How many remain?','options':['8','9','17','0'],'answer':'9','explanation':'“All but 9” means 9 remain.'},
 {'prompt':'What comes next: 2, 6, 12, 20, 30, ?','options':['36','40','42','44'],'answer':'42','explanation':'Terms are n(n+1): 1×2, 2×3, ... 6×7.'},
 {'prompt':'If 5 machines make 5 items in 5 minutes, how long do 100 machines take to make 100 items?','options':['5 minutes','20 minutes','100 minutes','500 minutes'],'answer':'5 minutes','explanation':'Each machine makes one item in five minutes.'},
 {'prompt':'A clock shows 3:15. What is the smaller angle between the hands?','options':['0°','7.5°','15°','22.5°'],'answer':'7.5°','explanation':'Minute hand is at 90°, hour hand at 97.5°.'},
 {'prompt':'You overtake the person in second place in a race. What place are you in?','options':['First','Second','Third','Cannot tell'],'answer':'Second','explanation':'You take the position of the person you overtook.'},
 {'prompt':'Which number is the odd one out: 16, 25, 36, 49, 63?','options':['16','25','49','63'],'answer':'63','explanation':'The others are perfect squares.'},
 {'prompt':'A room has 4 corners. A cat sits in each corner. Each cat sees 3 cats. How many cats?','options':['4','8','12','16'],'answer':'4','explanation':'Each of the four cats sees the other three.'},
 {'prompt':'If yesterday was Monday, what day is the day after tomorrow?','options':['Tuesday','Wednesday','Thursday','Friday'],'answer':'Thursday','explanation':'Yesterday Monday means today Tuesday; day after tomorrow is Thursday.'},
 {'prompt':'A bat and ball cost $1.10 total. The bat costs $1 more than the ball. Ball cost?','options':['$0.05','$0.10','$0.15','$0.20'],'answer':'$0.05','explanation':'Ball is 5 cents; bat is $1.05.'}],
'quant':[
 {'prompt':'What is 15% of 240?','options':['24','30','36','40'],'answer':'36','explanation':'0.15 × 240 = 36.'},
 {'prompt':'An asset rises 20% then falls 20%. Net change?','options':['No change','4% decrease','4% increase','2% decrease'],'answer':'4% decrease','explanation':'1.2 × 0.8 = 0.96, a 4% loss.'},
 {'prompt':'If the mean of 4 numbers is 12, their sum is?','options':['16','24','36','48'],'answer':'48','explanation':'Mean × count = sum.'},
 {'prompt':'A train travels 150 km in 2.5 hours. Average speed?','options':['50 km/h','55 km/h','60 km/h','65 km/h'],'answer':'60 km/h','explanation':'150 / 2.5 = 60.'},
 {'prompt':'What is the next prime number after 31?','options':['33','35','37','39'],'answer':'37','explanation':'37 is the next integer with no divisors other than 1 and itself.'},
 {'prompt':'If x:y = 3:5 and x+y=40, what is y?','options':['15','20','25','30'],'answer':'25','explanation':'Eight parts equal 40, so each part is 5; y=25.'},
 {'prompt':'A price of 800 is discounted by 25%. Sale price?','options':['550','575','600','650'],'answer':'600','explanation':'75% of 800 is 600.'},
 {'prompt':'What is the median of 2, 5, 9, 12, 20?','options':['5','9','10','12'],'answer':'9','explanation':'The middle value in sorted order is 9.'},
 {'prompt':'A value doubles in 3 years at compound growth. What is its total percentage increase?','options':['50%','100%','150%','200%'],'answer':'100%','explanation':'Doubling means the final value is twice the initial value.'},
 {'prompt':'If 8 workers finish a task in 6 days, how many days for 12 equally productive workers?','options':['3','4','5','9'],'answer':'4','explanation':'Work is 48 worker-days; 48/12 = 4 days.'}]
}
ARENAS=set(QUESTIONS.keys())

def user_json(u): return {'id':u.id,'username':u.username,'rating':u.rating,'wins':u.wins,'losses':u.losses,'matches':u.wins+u.losses}
def get_match_payload(m,u,s):
    opponent=s.get(User, m.player2 if u.id==m.player1 else m.player1)
    mine=m.score1 if u.id==m.player1 else m.score2
    theirs=m.score2 if u.id==m.player1 else m.score1
    qs=QUESTIONS[m.arena]
    return {'id':m.id,'arena':m.arena,'status':m.status,'score':mine,'opponent_score':theirs,'opponent':user_json(opponent),'question_index':m.current_question,'total_questions':10,'question':{k:v for k,v in qs[m.current_question].items() if k!='answer' and k!='explanation'} if m.status=='active' and m.current_question<10 else None,'winner_id':m.winner_id}

@app.get('/health')
def health():
    try:
        redis_client.ping()
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        return {'status':'ok','redis':'ok','database':'ok'}
    except RedisError:
        raise HTTPException(503,'Redis is unavailable')
    except SQLAlchemyError as exc:
        logging.exception('Database health check failed')
        raise HTTPException(503,'Database is unavailable') from exc

@app.post('/auth/register')
def register(data:RegisterIn,s:Session=Depends(db)):
    if data.username.lower() == 'mindclash_ai': raise HTTPException(400,'This username is reserved')
    if s.query(User).filter((User.email==data.email.lower())|(User.username==data.username)).first(): raise HTTPException(409,'Username or email already exists')
    u=User(username=data.username,email=data.email.lower(),password_hash=pwd.hash(data.password))
    s.add(u); s.commit(); s.refresh(u)
    return {'token':token_for(u),'user':user_json(u)}

@app.post('/auth/login')
def login(data:LoginIn,s:Session=Depends(db)):
    u=s.query(User).filter(User.email==data.email.lower()).first()
    if not u or not pwd.verify(data.password,u.password_hash): raise HTTPException(401,'Incorrect email or password')
    return {'token':token_for(u),'user':user_json(u)}

@app.get('/me')
def me(u:User=Depends(current_user)): return user_json(u)

@app.get('/leaderboard')
def leaderboard(s:Session=Depends(db)):
    return [user_json(u) for u in s.query(User).filter(User.username!='MindClash_AI').order_by(User.rating.desc()).limit(50).all()]

@app.post('/matchmaking/join')
def join_queue(data:QueueIn,u:User=Depends(current_user),s:Session=Depends(db)):
    arena=data.arena.lower()
    if arena not in ARENAS: raise HTTPException(400,'Unknown arena')
    active=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
    if active: return {'matched':True,'match':get_match_payload(active,u,s)}
    try:
        for other_arena in ARENAS:
            if other_arena != arena:
                redis_client.zrem(f'mindclash:queue:{other_arena}',str(u.id))
                redis_client.hdel(f'mindclash:queued_at:{other_arena}',str(u.id))
        paired_with=redis_client.get(f'mindclash:paired:{u.id}')
        if paired_with:
            pending=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
            if pending: return {'matched':True,'match':get_match_payload(pending,u,s)}
            return {'matched':False,'message':'Opponent found; preparing your match…'}
        result=redis_client.eval(PAIR_SCRIPT, 2, f'mindclash:queue:{arena}', f'mindclash:queued_at:{arena}', str(u.id), str(u.rating), str(int(datetime.now(timezone.utc).timestamp())), '100')
    except RedisError:
        raise HTTPException(503,'Matchmaking is temporarily unavailable. Start Redis and retry.')
    opponent_id=result[0]
    if not opponent_id:
        return {'matched':False,'message':'Searching for a similarly rated opponent','rating_window':int(result[1]),'waiting_seconds':int(result[2])}
    opponent=s.get(User,int(opponent_id))
    if not opponent:
        return {'matched':False,'message':'Opponent disconnected; keep searching'}
    # Redis has already claimed both queue entries atomically. The database stores the match.
    m=Match(id=secrets.token_urlsafe(12),player1=u.id,player2=opponent.id,arena=arena)
    s.add(m)
    try:
        s.commit(); s.refresh(m)
    except Exception:
        s.rollback()
        raise HTTPException(503,'Could not create the match. Please search again.')
    return {'matched':True,'match':get_match_payload(m,u,s)}

@app.post('/matchmaking/leave')
def leave_queue(u:User=Depends(current_user)):
    try:
        for arena in ARENAS:
            redis_client.zrem(f'mindclash:queue:{arena}',str(u.id))
            redis_client.hdel(f'mindclash:queued_at:{arena}',str(u.id))
        redis_client.delete(f'mindclash:paired:{u.id}')
    except RedisError:
        raise HTTPException(503,'Matchmaking service is unavailable')
    return {'ok':True}

@app.post('/matchmaking/bot')
async def play_bot(data:QueueIn,u:User=Depends(current_user),s:Session=Depends(db)):
    arena=data.arena.lower()
    if arena not in ARENAS: raise HTTPException(400,'Unknown arena')
    active=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
    if active: return {'matched':True,'match':get_match_payload(active,u,s)}
    try:
        for queued_arena in ARENAS:
            redis_client.zrem(f'mindclash:queue:{queued_arena}',str(u.id))
            redis_client.hdel(f'mindclash:queued_at:{queued_arena}',str(u.id))
    except RedisError:
        raise HTTPException(503,'Matchmaking service is unavailable')
    bot=s.query(User).filter_by(username='MindClash_AI').first()
    if not bot:
        bot=User(username='MindClash_AI',email='mindclash-ai@local.invalid',password_hash=pwd.hash(secrets.token_urlsafe(32)))
        s.add(bot); s.commit(); s.refresh(bot)
    m=Match(id=secrets.token_urlsafe(12),player1=u.id,player2=bot.id,arena=arena)
    s.add(m); s.commit(); s.refresh(m)
    if os.getenv('MINDCLASH_USE_QUEUE_WORKER') == 'true':
        try:
            enqueue_bot_match(m.id, bot.id, u.rating)
        except AzureError as exc:
            s.delete(m)
            s.commit()
            raise HTTPException(503,'Could not start the practice match') from exc
    else:
        asyncio.create_task(run_bot_match(m.id, bot.id, u.rating))
    return {'matched':True,'match':get_match_payload(m,u,s),'bot':True}

@app.get('/matches/{match_id}')
def match_state(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    return get_match_payload(m,u,s)

def apply_match_result(m, s):
    """Apply one Elo update and win/loss increment for a completed match."""
    p1=s.get(User,m.player1); p2=s.get(User,m.player2)
    if 'MindClash_AI' in (p1.username,p2.username):
        return
    expected1=1/(1+10**((p2.rating-p1.rating)/400))
    actual1=1.0 if m.winner_id==m.player1 else (0.0 if m.winner_id==m.player2 else 0.5)
    delta=round(32*(actual1-expected1))
    p1.rating+=delta; p2.rating-=delta
    if actual1==1.0: p1.wins+=1; p2.losses+=1
    elif actual1==0.0: p2.wins+=1; p1.losses+=1


def finish_match_if_needed(m, s):
    if m.status == 'finished': return
    if m.score1 >= 10 or m.score2 >= 10 or m.current_question >= 10:
        winner_id=m.player1 if m.score1 > m.score2 else (m.player2 if m.score2 > m.score1 else None)
        # Claim completion atomically so a simultaneous forfeit cannot apply Elo twice.
        claimed=s.query(Match).filter(Match.id==m.id,Match.status=='active').update(
            {Match.status:'finished',Match.winner_id:winner_id,Match.finished_at:datetime.utcnow()},
            synchronize_session=False
        )
        if claimed:
            s.flush(); s.refresh(m)
            apply_match_result(m,s)
        else:
            s.refresh(m)

@app.post('/matches/{match_id}/forfeit')
async def forfeit_match(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if m.status == 'active':
        winner_id=m.player2 if u.id==m.player1 else m.player1
        # Only one request can complete an active match and apply its rating change.
        claimed=s.query(Match).filter(Match.id==match_id,Match.status=='active').update(
            {Match.status:'finished',Match.winner_id:winner_id,Match.finished_at:datetime.utcnow()},
            synchronize_session=False
        )
        if claimed:
            s.expire(m); s.refresh(m)
            apply_match_result(m,s)
            s.commit(); s.refresh(m)
            await manager.publish(m,s)
        else:
            s.rollback()
            m=s.get(Match,match_id)
    return {'match':get_match_payload(m,u,s),'message':'You forfeited the match.' if m.winner_id and m.winner_id!=u.id else 'The match has already finished.'}


@app.post('/matches/{match_id}/socket-ticket')
def socket_ticket(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    ticket=secrets.token_urlsafe(32)
    try:
        redis_client.set(f'mindclash:socket:{match_id}:{ticket}',str(u.id),ex=30)
    except RedisError:
        raise HTTPException(503,'Live updates are temporarily unavailable')
    return {'ticket':ticket}

@app.websocket('/ws/matches/{match_id}')
async def match_socket(websocket:WebSocket, match_id:str, ticket:str):
    try:
        value=redis_client.getdel(f'mindclash:socket:{match_id}:{ticket}')
    except RedisError:
        await websocket.close(code=1013); return
    if value is None:
        await websocket.close(code=4401); return
    uid=int(value)
    initialize_database()
    session=SessionLocal()
    try:
        m=session.get(Match,match_id)
        if not m or uid not in (m.player1,m.player2):
            await websocket.close(code=4404); return
        await manager.connect(match_id,uid,websocket)
        await websocket.send_json({'type':'match_update','match':get_match_payload(m,session.get(User,uid),session)})
        while True:
            await websocket.receive_text()  # client may send ping frames as text
    except WebSocketDisconnect:
        manager.disconnect(match_id,websocket)
    finally:
        session.close()

@app.post('/matches/{match_id}/answer')
async def submit_answer(match_id:str,data:AnswerIn,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if m.status!='active': raise HTTPException(409,'Match has ended')
    if data.question_index!=m.current_question: raise HTTPException(409,'This question is no longer active')
    if data.question_index < 0 or data.question_index >= 10: raise HTTPException(400,'Invalid question index')
    correct=data.answer.strip().casefold()==QUESTIONS[m.arena][m.current_question]['answer'].casefold()
    s.add(Submission(match_id=match_id,user_id=u.id,question_index=data.question_index,answer=data.answer[:500],correct=correct))
    if correct:
        # Conditional UPDATE is the atomic claim: only one correct submission can advance a question.
        score_col = Match.score1 if u.id==m.player1 else Match.score2
        result=s.query(Match).filter(Match.id==match_id,Match.status=='active',Match.current_question==data.question_index).update({score_col:score_col+1,Match.current_question:Match.current_question+1},synchronize_session=False)
        if result:
            s.flush(); s.refresh(m); finish_match_if_needed(m,s)
            s.commit(); s.refresh(m)
            await manager.publish(m,s)
            return {'correct':True,'message':'Correct! You claimed the point.','match':get_match_payload(m,u,s),'explanation':QUESTIONS[m.arena][data.question_index]['explanation']}
        s.rollback()
        m=s.get(Match,match_id)
        return {'correct':True,'message':'Correct, but your opponent claimed this question first.','match':get_match_payload(m,u,s)}
    s.commit()
    return {'correct':False,'message':'Not quite. Try again!','match':get_match_payload(m,u,s)}

async def run_bot_match(match_id, bot_id, player_rating):
    """A lightweight practice opponent. It solves at a rating-adjusted pace; no LLM/API key needed."""
    accuracy=max(0.35,min(0.82,0.58+(player_rating-1000)/2500))
    while True:
        await asyncio.sleep(random.randint(6,11))
        session=SessionLocal()
        try:
            m=session.get(Match,match_id)
            if not m or m.status!='active': return
            question_index=m.current_question
            if random.random() <= accuracy:
                score_col=Match.score2 if m.player2==bot_id else Match.score1
                claimed=session.query(Match).filter(Match.id==match_id,Match.status=='active',Match.current_question==question_index).update({score_col:score_col+1,Match.current_question:Match.current_question+1},synchronize_session=False)
                if claimed:
                    session.flush(); session.refresh(m); finish_match_if_needed(m,session)
                    session.commit(); session.refresh(m)
                    await manager.publish(m,session)
                    if m.status=='finished': return
        except Exception:
            session.rollback()
            logging.exception('Bot match %s failed', match_id)
            raise
        finally:
            session.close()

@app.get('/matches/{match_id}/result')
def result(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if m.status!='finished': raise HTTPException(409,'Match is still active')
    return {'match':get_match_payload(m,u,s),'winner':user_json(s.get(User,m.winner_id)) if m.winner_id else None,'draw':m.winner_id is None}

STATIC_DIR = Path(__file__).parent / 'static'
if STATIC_DIR.is_dir():
    static_root = STATIC_DIR.resolve()

    @app.get('/{asset_path:path}', include_in_schema=False)
    def serve_frontend(asset_path: str):
        requested = (static_root / asset_path).resolve()
        if not requested.is_relative_to(static_root):
            raise HTTPException(404, 'Not found')
        target = requested if requested.is_file() else static_root / 'index.html'
        if not target.is_file():
            raise HTTPException(404, 'Not found')
        return FileResponse(target)
