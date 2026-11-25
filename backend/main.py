from datetime import datetime, timedelta, timezone
from typing import Optional
import asyncio, os, random, secrets

from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
import redis
from redis.exceptions import RedisError

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./mindclash.db')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql+psycopg://', 1)
elif DATABASE_URL.startswith('postgresql://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+psycopg://', 1)
engine = create_engine(DATABASE_URL, connect_args={'check_same_thread': False} if DATABASE_URL.startswith('sqlite') else {})
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

Base.metadata.create_all(bind=engine)
app=FastAPI(title='MindClash API', version='0.1.0')
app.add_middleware(CORSMiddleware, allow_origins=os.getenv('CORS_ORIGINS','http://localhost:5173,http://127.0.0.1:5173').split(','), allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

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
local reserve_prefix = ARGV[5]
local reservation = ARGV[6]
local active_prefix = ARGV[7]
for i=3,#KEYS,2 do
  redis.call('ZREM', KEYS[i], self_id)
  redis.call('HDEL', KEYS[i+1], self_id)
end
if redis.call('EXISTS', active_prefix..self_id) == 1 then
  return {'__ACTIVE__', '0', '0'}
end
if redis.call('EXISTS', reserve_prefix..self_id) == 1 then
  return {'__RESERVED__', '0', '0'}
end
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
  if candidate ~= self_id and redis.call('EXISTS', reserve_prefix..candidate) == 0 and redis.call('EXISTS', active_prefix..candidate) == 0 then
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
  redis.call('SET', reserve_prefix..self_id, reservation, 'EX', 30)
  redis.call('SET', reserve_prefix..best_id, reservation, 'EX', 30)
  redis.call('SET', active_prefix..self_id, reservation, 'EX', 30)
  redis.call('SET', active_prefix..best_id, reservation, 'EX', 30)
  redis.call('SET', 'mindclash:paired:'..self_id, best_id, 'EX', 30)
  redis.call('SET', 'mindclash:paired:'..best_id, self_id, 'EX', 30)
  return {best_id, tostring(window), tostring(waited), reservation}
end
redis.call('EXPIRE', queue, 900)
redis.call('EXPIRE', times, 900)
return {'', tostring(window), tostring(waited)}
"""

RELEASE_RESERVATIONS_SCRIPT = """
local reservation = ARGV[1]
local user_id = ARGV[2]
local other_id = ARGV[3]
local user_key = 'mindclash:reserve:'..user_id
local other_key = 'mindclash:reserve:'..other_id
if redis.call('GET', user_key) == reservation then redis.call('DEL', user_key) end
if redis.call('GET', other_key) == reservation then redis.call('DEL', other_key) end
if redis.call('GET', 'mindclash:paired:'..user_id) == other_id then redis.call('DEL', 'mindclash:paired:'..user_id) end
if redis.call('GET', 'mindclash:paired:'..other_id) == user_id then redis.call('DEL', 'mindclash:paired:'..other_id) end
return 1
"""

MATCH_IDLE_SECONDS = 300

def release_reservations(reservation, user_id, other_id):
    try:
        redis_client.eval(RELEASE_RESERVATIONS_SCRIPT, 0, reservation, str(user_id), str(other_id))
    except RedisError:
        # Reservations have a short TTL, so a Redis outage cannot leave a permanent lock.
        pass

def clear_tentative_active(reservation, user_id, other_id):
    script = """
    for i=1,2 do
      local key='mindclash:active:'..ARGV[i]
      if redis.call('GET', key) == ARGV[3] then redis.call('DEL', key) end
    end
    return 1
    """
    try:
        redis_client.eval(script, 0, str(user_id), str(other_id), reservation)
    except RedisError:
        pass

def promote_active_markers(reservation, match_id, *user_ids):
    script = """
    for i=1,#ARGV-1 do
      local key='mindclash:active:'..ARGV[i]
      if redis.call('GET', key) == ARGV[#ARGV-1] then
        redis.call('SET', key, ARGV[#ARGV], 'EX', 600)
      end
    end
    return 1
    """
    try:
        redis_client.eval(script, 0, *(str(uid) for uid in user_ids), reservation, match_id)
    except RedisError:
        pass

def remember_active_match(match_id, *user_ids):
    try:
        for uid in user_ids:
            redis_client.set(f'mindclash:active:{uid}', match_id, ex=600)
    except RedisError:
        pass

def clear_active_markers(*user_ids):
    try:
        redis_client.delete(*(f'mindclash:active:{uid}' for uid in user_ids))
    except RedisError:
        pass

def match_last_activity(m, s):
    latest_submission = s.query(Submission.created_at).filter(Submission.match_id == m.id).order_by(Submission.created_at.desc()).first()
    return latest_submission[0] if latest_submission else m.created_at

def expire_idle_match(m, s, requester_id=None):
    """Forfeit an inactive match after the idle window and apply its result once."""
    if not m or m.status != 'active':
        return False
    last_activity = match_last_activity(m, s)
    if (datetime.utcnow() - last_activity).total_seconds() < MATCH_IDLE_SECONDS:
        return False
    # The player who last submitted gets the win; if neither submitted, the player
    # making a new matchmaking request is treated as having left the old match.
    latest = s.query(Submission.user_id).filter(Submission.match_id == m.id).order_by(Submission.created_at.desc()).first()
    winner_id = latest[0] if latest else (m.player2 if requester_id == m.player1 else (m.player1 if requester_id == m.player2 else None))
    claimed = s.query(Match).filter(Match.id == m.id, Match.status == 'active').update(
        {Match.status: 'finished', Match.winner_id: winner_id, Match.finished_at: datetime.utcnow()},
        synchronize_session=False
    )
    if claimed:
        s.flush(); s.refresh(m)
        apply_match_result(m, s)
        s.commit(); s.refresh(m)
        clear_active_markers(m.player1, m.player2)
        return True
    s.rollback()
    return False

def db():
    s=SessionLocal()
    try: yield s
    finally: s.close()

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
 {'prompt':'A cache must support lookup, insertion, and removing the least recently used item in O(1). Which combination gives the key operations?','options':['Hash map plus doubly linked list','Min-heap plus array','Binary search tree only','Queue plus stack'],'answer':'Hash map plus doubly linked list','explanation':'The map finds a node instantly; the list moves and removes that node in constant time while keeping recency order.'},
 {'prompt':'In a word-transformation puzzle, each move changes one letter and every move has equal cost. Which search finds the fewest moves?','options':['BFS from the start word','DFS from the start word','Sort the dictionary','Greedy choose the closest-looking word'],'answer':'BFS from the start word','explanation':'Treat words as graph nodes and one-letter changes as edges. BFS reaches each layer by increasing number of moves.'},
 {'prompt':'Two lines hold water; moving either line inward shrinks the width. Which line should you move to search for a larger area?','options':['The shorter line','The taller line','Either one at random','Both lines together'],'answer':'The shorter line','explanation':'The shorter height limits the area. Moving the taller side cannot improve that limiting height and only reduces width.'},
 {'prompt':'Every row and column of a matrix is sorted ascending. How can you search for a target in O(rows + columns)?','options':['Start at the top-right and eliminate a row or column each step','Binary-search every row','Scan the full matrix','Sort all cells again'],'answer':'Start at the top-right and eliminate a row or column each step','explanation':'At the top-right, a value that is too large eliminates its column; one too small eliminates its row.'},
 {'prompt':'You need to group words that are anagrams of one another. Which key captures the property that matters?','options':['A character-frequency signature','The first character','The original word length only','The reversed word'],'answer':'A character-frequency signature','explanation':'Anagrams have the same count of every character, even when their character order differs.'},
 {'prompt':'You need to merge K already-sorted lists without scanning all K heads each time. What structure keeps the next choice efficient?','options':['A min-heap containing the current head of each list','A stack for each list','A hash set of all nodes','A max-heap of list lengths'],'answer':'A min-heap containing the current head of each list','explanation':'The smallest current head is the next output. After removing it, add the next value from that same list.'},
 {'prompt':'A sequence contains positive and negative values. You need its maximum-sum contiguous subarray. What should you keep while scanning once?','options':['The best sum ending at this position and the global best','A sorted copy of all values','The smallest prefix only','A heap of all subarrays'],'answer':'The best sum ending at this position and the global best','explanation':'Either extend the previous best-ending segment or start fresh here; discard a negative prefix that can only hurt future sums.'},
 {'prompt':'A data structure must support insert, delete, and choosing a random stored item, all in O(1) average time. What is the key combination?','options':['Array plus value-to-index hash map','Balanced tree plus queue','Linked list plus min-heap','Stack plus binary search'],'answer':'Array plus value-to-index hash map','explanation':'The array supports random indexing; the map locates values. Delete by swapping with the last array element and updating its index.'},
 {'prompt':'An array has n+1 values, each between 1 and n, so at least one value repeats. How can you find a duplicate without modifying the array or using extra space?','options':['Treat each value as a next index and use cycle detection','Sort the array in place','Count values in a hash map','Compare every pair'],'answer':'Treat each value as a next index and use cycle detection','explanation':'The mapping from index to value forms a linked structure; a repeated value creates a cycle, which slow and fast pointers can locate.'},
 {'prompt':'A binary search tree may be unbalanced. You need the Kth smallest value without storing every value. What traversal idea helps?','options':['Inorder traversal and stop after K nodes','Preorder traversal and sort afterward','Level order and stop at depth K','Reverse inorder and count leaves'],'answer':'Inorder traversal and stop after K nodes','explanation':'Inorder visits BST values in sorted order. An iterative stack lets you stop as soon as the Kth node is visited.'},
 {'prompt':'A sorted array may contain negative and positive values. How do you find whether two values sum to a target in one pass?','options':['Check every pair','Use two pointers from both ends','Use a sliding window','Sort it again'],'answer':'Use two pointers from both ends','explanation':'The sorted order tells you which pointer to move: too small means move left forward; too large means move right backward.'},
 {'prompt':'You need the number of contiguous subarrays summing to K; values can be negative. Which idea handles this safely?','options':['Expand a positive-only sliding window','Track prefix sums and their frequencies','Sort the array first','Use a min-heap'],'answer':'Track prefix sums and their frequencies','explanation':'If prefix[j] - prefix[i] = K, that subarray sums to K. A frequency map finds matching earlier prefixes, even with negative values.'},
 {'prompt':'Find the longest substring with no repeated character. What should a sliding window remember?','options':['The last index of each character','Only the total character count','The smallest character','A sorted copy of the text'],'answer':'The last index of each character','explanation':'When a duplicate appears, jump the window start past that character’s previous index; this keeps the window valid in linear time.'},
 {'prompt':'A list of time intervals may overlap. What is the key first step for merging them?','options':['Sort by interval start','Sort by interval length','Use a stack in input order','Compare every interval pair'],'answer':'Sort by interval start','explanation':'After sorting starts, any interval that overlaps the current merged interval can be absorbed by extending its end.'},
 {'prompt':'You need the K most frequent values from a very large list. Which approach avoids sorting every value by frequency?','options':['Keep a size-K min-heap','Use DFS','Use a queue of all values','Use binary search'],'answer':'Keep a size-K min-heap','explanation':'Keep only the best K frequencies seen so far; the heap root is the weakest value that may be replaced.'},
 {'prompt':'Numbers arrive one at a time, and you must report the Kth largest after each arrival. What is the useful invariant?','options':['A min-heap containing the largest K seen','A max-heap containing every value','A sorted linked list of all values','A stack of the latest K values'],'answer':'A min-heap containing the largest K seen','explanation':'The heap root is the Kth largest: discard values smaller than it and replace the root when a larger value arrives.'},
 {'prompt':'A linked list may contain a cycle, and you want to detect it without extra memory. Which idea works?','options':['Two pointers moving at different speeds','Sort node values','Store every node in an array','Reverse the list twice'],'answer':'Two pointers moving at different speeds','explanation':'If a cycle exists, the faster pointer eventually laps and meets the slower pointer inside the cycle.'},
 {'prompt':'When reversing a singly linked list in place, what must you save before changing the current node’s link?','options':['The next node','The list length','The previous node’s value','The tail twice'],'answer':'The next node','explanation':'Save next first; then point current backward. Otherwise the rest of the list becomes unreachable.'},
 {'prompt':'A graph is unweighted. You need the fewest edges from a source to every reachable vertex. What is the key idea?','options':['Explore in layers with BFS','Use DFS and stop at the first path','Sort the vertices','Use a minimum spanning tree'],'answer':'Explore in layers with BFS','explanation':'BFS visits vertices by distance in edges, so the first visit to a vertex is a shortest path.'},
 {'prompt':'A grid contains islands made of adjacent land cells. What is the simplest way to count connected islands?','options':['Flood-fill each unvisited land cell','Sort all cells by row','Use binary search on the grid','Run Dijkstra from every cell'],'answer':'Flood-fill each unvisited land cell','explanation':'Each DFS or BFS marks one whole connected component; the number of starts is the number of islands.'},
 {'prompt':'Courses have prerequisite relationships. How can you tell whether all courses can be completed?','options':['Topologically process zero-indegree courses','Run Dijkstra from course zero','Sort by course number','Use a stack of all edges'],'answer':'Topologically process zero-indegree courses','explanation':'Repeatedly remove courses with no remaining prerequisites. If some remain, they are trapped in a cycle.'},
 {'prompt':'You need to detect a cycle in a directed graph during DFS. What state is essential?','options':['A recursion-stack or three-color state','Only the parent vertex','The shortest distance so far','The degree of each node only'],'answer':'A recursion-stack or three-color state','explanation':'An edge to a node currently on the DFS path closes a directed cycle; a merely visited node may be finished already.'},
 {'prompt':'A network changes as edges are added, and you repeatedly ask whether two nodes are connected. Which structure fits?','options':['Disjoint-set union','A binary search tree','A monotonic queue','A prefix-sum array'],'answer':'Disjoint-set union','explanation':'Union joins components and find checks their representatives; path compression and union by size make these operations nearly constant amortized time.'},
 {'prompt':'An array stays fixed while you answer many range-sum queries. What should you precompute?','options':['Prefix sums','A heap of values','A graph of indices','A stack of suffixes'],'answer':'Prefix sums','explanation':'The sum from L to R is prefix[R+1] - prefix[L], so each query takes constant time after linear preprocessing.'},
 {'prompt':'You must add a value to many ranges, then read the final array once. What technique makes the updates efficient?','options':['Difference array','Segment tree with lazy propagation only','BFS','Binary search'],'answer':'Difference array','explanation':'Record +value at the range start and -value just after its end; one prefix pass applies all range updates.'},
 {'prompt':'A stream asks for the maximum in every window of width K. Which structure keeps the work linear?','options':['A decreasing deque of candidate indices','A min-heap of all values','A sorted list rebuilt per window','A stack of window sums'],'answer':'A decreasing deque of candidate indices','explanation':'Remove expired indices from the front and smaller values from the back; the front is always the window maximum.'},
 {'prompt':'For each daily temperature, find how many days until a warmer day. What pattern is useful?','options':['A monotonic decreasing stack of unresolved days','A queue sorted by temperature','A prefix sum','A union-find structure'],'answer':'A monotonic decreasing stack of unresolved days','explanation':'A warmer temperature resolves earlier cooler days at the top of the stack, each index entering and leaving once.'},
 {'prompt':'You can attend as many non-overlapping events as possible. What greedy choice is safe?','options':['Choose the event that ends earliest','Choose the event that starts earliest','Choose the longest event','Choose the event with the most attendees'],'answer':'Choose the event that ends earliest','explanation':'Finishing earliest leaves the most remaining time for future events; repeat among events that start after it ends.'},
 {'prompt':'You need the minimum number of rooms for overlapping meetings. After sorting by start time, what should a min-heap track?','options':['The end time of each occupied room','The start time of each meeting','All meeting durations in descending order','The room with the fewest meetings'],'answer':'The end time of each occupied room','explanation':'Reuse the room that becomes free earliest if its end is no later than the next start; otherwise allocate another room.'},
 {'prompt':'A problem asks for the smallest capacity that can ship packages within D days. What clue suggests binary search on the answer?','options':['Feasibility changes monotonically as capacity increases','The input is already sorted','The answer must be a graph node','Every choice is independent'],'answer':'Feasibility changes monotonically as capacity increases','explanation':'If capacity C works, every larger capacity also works. Binary-search the boundary and use a greedy check for each candidate.'},
 {'prompt':'In 0/1 knapsack, each item can be used at most once. What prevents a one-dimensional DP from reusing the same item?','options':['Iterate capacities downward','Iterate capacities upward','Sort items by weight','Use BFS over values'],'answer':'Iterate capacities downward','explanation':'Descending capacity reads states from before the current item update, so the item cannot feed into itself more than once.'},
 {'prompt':'For the longest increasing subsequence, what does the tails array represent in the O(n log n) method?','options':['Smallest ending value for each subsequence length','The actual subsequence in final order','The number of smaller values to the left','The largest value seen at each index'],'answer':'Smallest ending value for each subsequence length','explanation':'Smaller tails leave more room to extend. Binary search finds which length the current value can improve.'},
 {'prompt':'You need the minimum number of coins to make an amount, with reusable coin values. Why is dynamic programming a natural fit?','options':['Each amount depends on smaller amounts','The coins arrive in sorted order','A greedy choice is always optimal','The state is a graph cycle'],'answer':'Each amount depends on smaller amounts','explanation':'For amount A, try each coin c and use best[A-c] + 1. Repeated subamounts are reused instead of recomputed.'},
 {'prompt':'Find the longest palindromic substring. What observation gives a simple O(n²) approach?','options':['Every palindrome expands around a center','Palindromes are sorted lexicographically','Only even-length strings matter','A hash table stores all substrings'],'answer':'Every palindrome expands around a center','explanation':'Try each character and each gap as a center, then expand while the ends match; there are only O(n) centers.'},
 {'prompt':'A dictionary contains many words, and queries ask whether any word starts with a prefix. Which structure matches the query shape?','options':['Trie','Heap','Disjoint-set union','Fenwick tree'],'answer':'Trie','explanation':'A trie shares common prefixes, so prefix lookup follows only the characters in the query.'},
 {'prompt':'You need the number of inversions in an array: pairs i < j where A[i] > A[j]. Which strategy achieves O(n log n)?','options':['Count cross pairs during merge sort','Compare every pair','Use a sliding window','Build a maximum spanning tree'],'answer':'Count cross pairs during merge sort','explanation':'When a right-half value is smaller than the current left value, it forms inversions with all remaining left-half values.'},
 {'prompt':'A tree question asks for the lowest common ancestor of two nodes. What recursive result makes the solution work?','options':['Return a found target upward; combine left and right results','Return the deeper node immediately','Sort all node values','Run BFS from every leaf'],'answer':'Return a found target upward; combine left and right results','explanation':'If one target is found in each subtree, the current node is their first shared ancestor; otherwise propagate the one-sided result.'},
 {'prompt':'You need to validate whether a sequence could be a BST preorder traversal. What useful bound-based idea avoids rebuilding the tree?','options':['Track the allowed value range as traversal advances','Sort the sequence and compare it','Use a queue by levels','Count duplicate values'],'answer':'Track the allowed value range as traversal advances','explanation':'Each move into a right subtree raises a lower bound; any later value below it violates the BST ordering.'},
 {'prompt':'A problem asks for the shortest path with some negative edge weights but no negative cycle. Which algorithmic idea handles this?','options':['Repeatedly relax every edge','Use ordinary BFS','Use Dijkstra without changes','Use a minimum spanning tree'],'answer':'Repeatedly relax every edge','explanation':'Bellman-Ford lets improvements propagate across up to V-1 edges and can also detect a negative cycle with one extra pass.'},
 {'prompt':'A recursive search explores combinations, but many branches cannot reach the target. What should you add?','options':['Pruning using a bound that proves a branch cannot help','A larger recursion depth','A sort after every choice','A queue of all possible answers'],'answer':'Pruning using a bound that proves a branch cannot help','explanation':'Backtracking is efficient when you stop exploring as soon as the partial choice cannot produce a valid or better answer.'},
 {'prompt':'A network has capacities on its edges, and you need the maximum amount that can flow from a source to a sink. Which idea reveals the bottleneck?','options':['Build flow through augmenting paths in a residual graph','Use shortest paths on the original graph only','Choose the highest-capacity edge greedily','Topologically sort the vertices'],'answer':'Build flow through augmenting paths in a residual graph','explanation':'Residual edges show where flow can still be increased or rerouted; when no augmenting path remains, max-flow equals the capacity of a minimum cut.'},
 {'prompt':'A Boolean formula is made of clauses with two literals each. What graph insight can determine whether it is satisfiable?','options':['Build an implication graph and inspect strongly connected components','Use a minimum spanning tree','Try every assignment with no pruning','Run Dijkstra from each variable'],'answer':'Build an implication graph and inspect strongly connected components','explanation':'For 2-SAT, a variable and its negation in the same strongly connected component imply contradictory reachability, so no assignment can satisfy the formula.'},
 {'prompt':'In a sorted array, you need the first position whose value is at least a target. What binary-search invariant helps?','options':['Keep the boundary between values below the target and values at least the target','Search only for an exact match','Move left whenever the middle is larger','Check every element in order'],'answer':'Keep the boundary between values below the target and values at least the target','explanation':'Lower-bound search narrows the first index that satisfies a monotonic condition, even when the target is absent.'},
 {'prompt':'A sorted array has been rotated, and all values are distinct. How can you still search in logarithmic time?','options':['At each step identify the sorted half, then decide whether the target lies inside it','Sort a copy before searching','Use a sliding window','Scan outward from the midpoint'],'answer':'At each step identify the sorted half, then decide whether the target lies inside it','explanation':'At least one half around the midpoint remains sorted; its range tells you which half can be discarded.'},
 {'prompt':'A sorted array contains duplicates. You need the first and last index of a target in O(log n). What should you do?','options':['Run lower-bound and upper-bound searches','Stop at the first exact match','Use a hash set','Scan from both ends until they meet'],'answer':'Run lower-bound and upper-bound searches','explanation':'Two boundary searches find the first value not less than the target and the first value greater than it.'},
 {'prompt':'An expression contains brackets of several types. How can you detect whether they close in the right order?','options':['Push opening brackets onto a stack and match each closer with the top','Count opening and closing brackets only','Sort all brackets by type','Use a queue of opening brackets'],'answer':'Push opening brackets onto a stack and match each closer with the top','explanation':'The most recently opened bracket must close first, which is exactly the last-in, first-out rule of a stack.'},
 {'prompt':'A stack must support push, pop, and retrieving its minimum in O(1). What extra information is useful?','options':['Store the minimum so far alongside each pushed value','Keep all values in a sorted array','Re-scan the stack after every operation','Use a queue instead'],'answer':'Store the minimum so far alongside each pushed value','explanation':'Each entry can remember the smallest value beneath and including it, so popping restores the previous minimum immediately.'},
 {'prompt':'You need to implement a FIFO queue using stacks. Which transfer rule preserves queue order?','options':['Move items to the output stack only when it is empty','Move every item after each enqueue','Always pop from the input stack','Sort the two stacks after each operation'],'answer':'Move items to the output stack only when it is empty','explanation':'Reversing the input stack once makes its oldest item the output stack top; leaving it there amortizes each item to O(1) moves.'},
 {'prompt':'A tree should be visited one depth level at a time. Which traversal naturally does this?','options':['BFS with a queue','DFS with a stack only','Inorder traversal','Binary search'],'answer':'BFS with a queue','explanation':'A queue processes nodes in arrival order, so all nodes at one depth are handled before the next depth.'},
 {'prompt':'You need the maximum number of nodes on a root-to-leaf path. What recursive relationship captures tree height?','options':['One plus the larger child height','The sum of all node values','The smaller child height only','The number of leaves'],'answer':'One plus the larger child height','explanation':'A deepest path continues through the taller child, adding one edge or node for the current root.'},
 {'prompt':'A binary tree must be checked for height balance at every node. How can you avoid recalculating heights repeatedly?','options':['Return height and balance status together in one postorder traversal','Run a full height search from every node','Sort the node values','Check only the root children'],'answer':'Return height and balance status together in one postorder traversal','explanation':'Each subtree reports its height once; its parent can compare child heights and propagate an unbalanced result upward.'},
 {'prompt':'An undirected graph must be split into two groups so every edge crosses groups. What traversal can check this?','options':['Color neighbors with opposite colors using BFS or DFS','Sort vertices by degree','Run Dijkstra from every vertex','Build a minimum spanning tree'],'answer':'Color neighbors with opposite colors using BFS or DFS','explanation':'A conflict occurs if an edge joins two vertices assigned the same color; that means the graph contains an odd cycle.'},
 {'prompt':'You need a deep copy of a graph that may contain cycles. What prevents infinite recursion and duplicate copies?','options':['Record each original-to-copy mapping before visiting its neighbors','Copy only leaf vertices','Sort neighbors before copying','Use a queue without a visited map'],'answer':'Record each original-to-copy mapping before visiting its neighbors','explanation':'The map both reuses an existing copy when a cycle returns and ensures each original vertex is copied once.'},
 {'prompt':'A weighted graph has only nonnegative edge costs. Which method finds shortest distances from one start efficiently?','options':['Repeatedly finalize the unsettled vertex with the smallest known distance','Use BFS regardless of edge weights','Choose the largest edge first','Topologically sort every graph'],'answer':'Repeatedly finalize the unsettled vertex with the smallest known distance','explanation':'With nonnegative edges, a smallest tentative distance cannot later be improved; a min-priority queue makes the selection efficient.'},
 {'prompt':'You need a minimum-cost set of edges connecting every vertex in an undirected weighted graph. What is the key idea behind Kruskal’s method?','options':['Add edges from lightest to heaviest, skipping ones that close a cycle','Always choose the next edge touching the source','Take the heaviest edge from each component','Run shortest paths from every vertex'],'answer':'Add edges from lightest to heaviest, skipping ones that close a cycle','explanation':'A disjoint-set structure detects whether an edge joins different components; safe edges are added until all vertices connect.'},
 {'prompt':'A trie stores words and you need to know how many stored words share a prefix. What should each trie node maintain?','options':['A count of words passing through that node','The full sorted word list','The longest word in the whole trie','A hash of every suffix'],'answer':'A count of words passing through that node','explanation':'Incrementing path counts during insertion lets a prefix query return its matching-word count when traversal reaches that node.'},
 {'prompt':'A pattern contains a wildcard that can match any sequence of characters. Which approach handles repeated overlapping choices safely?','options':['Dynamic programming over text and pattern positions','Greedy matching without backtracking or saved state','Sort the text characters','Use a stack of distinct characters'],'answer':'Dynamic programming over text and pattern positions','explanation':'Each state records whether a text prefix matches a pattern prefix; a wildcard can either consume nothing or another character.'},
 {'prompt':'You need the minimum edits to turn one word into another using insert, delete, and replace. What does a DP cell represent?','options':['The minimum edits between prefixes ending at those two positions','The number of matching letters anywhere','The longest word in the dictionary','The cost of sorting both words'],'answer':'The minimum edits between prefixes ending at those two positions','explanation':'The last operation is an insertion, deletion, or replacement, so each cell depends on neighboring prefix states.'},
 {'prompt':'A string must be split into valid dictionary words. What dynamic-programming state makes the decision manageable?','options':['Whether each prefix can be segmented into dictionary words','The most frequent character so far','The sorted order of dictionary entries','The longest dictionary word only'],'answer':'Whether each prefix can be segmented into dictionary words','explanation':'A prefix is segmentable if some earlier segmentable boundary is followed by a dictionary word ending here.'},
 {'prompt':'Houses in a row have values, and adjacent houses cannot both be chosen. What state is enough while scanning left to right?','options':['Best total using the previous house or skipping it','A sorted list of all house values','The number of houses remaining','The last two chosen house values only'],'answer':'Best total using the previous house or skipping it','explanation':'For each house, compare taking its value plus the best result two positions back against skipping it.'},
 {'prompt':'A robot moves only right or down through a grid with blocked cells. How can you count valid routes without listing them?','options':['Store the number of ways to reach each cell from its top and left neighbors','Run DFS and keep every full path','Sort the grid by cell value','Use a max-heap of coordinates'],'answer':'Store the number of ways to reach each cell from its top and left neighbors','explanation':'Every route into a cell arrives from above or from the left; blocked cells contribute zero ways.'},
 {'prompt':'Each job has a value and a start/end time. You want the maximum total value from non-overlapping jobs. What strategy fits?','options':['Sort by finish time and use DP with the last compatible job','Always choose the shortest job','Choose jobs by highest value alone','Use a queue in input order'],'answer':'Sort by finish time and use DP with the last compatible job','explanation':'For each job, compare skipping it with taking its value plus the best compatible schedule found before it.'},
 {'prompt':'You need the Kth largest item in an unsorted array but do not need the whole array sorted. What can reduce average work?','options':['Partition around a pivot and continue only on the side containing rank K','Use merge sort on every value','Run binary search on array indices','Build every possible subset'],'answer':'Partition around a pivot and continue only on the side containing rank K','explanation':'Quickselect places a pivot in its final rank, then discards the irrelevant partition instead of sorting both sides.'},
 {'prompt':'Two sorted arrays are too large to merge, and you need their combined median in logarithmic time. What is the key idea?','options':['Binary-search a partition where left-side values do not exceed right-side values','Scan both arrays until reaching the middle','Sort the concatenated arrays','Use a hash map of values'],'answer':'Binary-search a partition where left-side values do not exceed right-side values','explanation':'A valid partition puts half the elements on each side and lets boundary values determine the median.'},
 {'prompt':'You need range-sum queries and point updates on an array. Which compact structure supports both in logarithmic time?','options':['Fenwick tree over partial prefix sums','A plain prefix-sum array with no updates','A stack of values','A sorted copy of the array'],'answer':'Fenwick tree over partial prefix sums','explanation':'Each tree position summarizes a range determined by its lowest set bit, so an update and a prefix query touch only logarithmically many ranges.'},
 {'prompt':'An array receives point updates and range-minimum queries. What structure stores summaries for reusable intervals?','options':['Segment tree','A single prefix sum','A queue of updates','A trie'],'answer':'Segment tree','explanation':'Each internal node combines summaries of its two child intervals; an update refreshes one path and a query visits only relevant intervals.'},
 {'prompt':'You need to find a pattern in text without rechecking characters that already matched. What does the KMP prefix table tell you?','options':['How far the pattern can shift while preserving a matched prefix','The alphabetically next character','The number of occurrences in a sorted list','The longest suffix of the text overall'],'answer':'How far the pattern can shift while preserving a matched prefix','explanation':'The table reuses the longest proper prefix that is also a suffix after a mismatch, avoiding a restart from zero.'},
 {'prompt':'A rolling-hash substring search reports a possible match. What must you do to guarantee correctness?','options':['Compare the actual substring after a hash match','Assume equal hashes always mean equal strings','Sort both strings','Restart the search from the beginning'],'answer':'Compare the actual substring after a hash match','explanation':'Different strings can collide under a finite hash, so verifying candidate matches prevents a collision from becoming a false answer.'},
 {'prompt':'You need the longest palindromic substring in linear time. Which stronger technique reuses information from earlier centers?','options':['Maintain the rightmost known palindrome and mirror radii around its center','Try every substring independently','Sort characters by frequency','Use BFS over all substrings'],'answer':'Maintain the rightmost known palindrome and mirror radii around its center','explanation':'Manacher’s method initializes a new center from its mirrored radius inside the current boundary, then expands only beyond known territory.'},
 {'prompt':'Every value appears twice except one, and the values can be XORed. Why does XOR reveal the single value?','options':['Equal pairs cancel because x XOR x is zero','XOR sorts values by size','The largest value always remains','XOR counts how many values are odd'],'answer':'Equal pairs cancel because x XOR x is zero','explanation':'XOR is associative and commutative, so all duplicate pairs cancel and leave the unpaired value.'},
 {'prompt':'You need to generate every subset of a small set of n items. What does each n-bit mask represent?','options':['Which items are included in one subset','The sorted order of all items','A path through a graph','The frequency of each item'],'answer':'Which items are included in one subset','explanation':'Each bit independently records whether its item is present, giving one mask for each of the 2^n subsets.'},
 {'prompt':'You need all unique triples summing to a target in an array. After sorting and fixing one value, what reduces the search?','options':['Use two pointers and skip equal values after recording a triple','Try every remaining triple with three loops','Use a stack of prefixes','Binary-search every possible pair separately'],'answer':'Use two pointers and skip equal values after recording a triple','explanation':'Sorted order lets the left or right pointer move according to whether the sum is too small or too large; skipping duplicates avoids repeated triples.'}
],
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
 {'prompt':'The expected value of a fair six-sided die roll is?','options':['3','3.5','4','4.5'],'answer':'3.5','explanation':'(1+2+3+4+5+6)/6 = 3.5.'},
 {'prompt':'Three fair coins are tossed. What is the probability of exactly two heads?','options':['1/8','1/4','3/8','1/2'],'answer':'3/8','explanation':'There are three outcomes with exactly two heads among eight equally likely sequences.'},
 {'prompt':'A card is drawn from a standard deck. What is the probability it is a heart or a king?','options':['4/13','15/52','17/52','1/3'],'answer':'4/13','explanation':'There are 13 hearts and 4 kings, but the king of hearts is counted twice, giving 16/52 = 4/13.'},
 {'prompt':'A bag has 3 red and 2 blue balls. Two are drawn without replacement. What is the probability both are red?','options':['1/5','3/10','2/5','1/2'],'answer':'3/10','explanation':'The chance is (3/5) × (2/4) = 3/10.'},
 {'prompt':'A fair coin is tossed four times. What is the probability of at least one tail?','options':['1/16','7/8','15/16','1/2'],'answer':'15/16','explanation':'Use the complement: only one outcome has no tails, so the probability is 1 - 1/16.'},
 {'prompt':'A fair die is rolled. Given that the result is even, what is the probability it is greater than 3?','options':['1/3','1/2','2/3','1'],'answer':'2/3','explanation':'The even outcomes are 2, 4, and 6; two of these are greater than 3.'},
 {'prompt':'A box contains 4 green and 6 yellow tokens. One token is chosen, replaced, then another is chosen. What is the probability both are green?','options':['2/25','4/25','1/5','4/10'],'answer':'4/25','explanation':'Replacement keeps the draws independent, so multiply (4/10) × (4/10).'},
 {'prompt':'A game pays 10 points with probability 1/4 and 0 points otherwise. What is its expected score?','options':['2.5','4','5','10'],'answer':'2.5','explanation':'Expected value is 10 × 1/4 + 0 × 3/4 = 2.5.'},
 {'prompt':'Two fair dice are rolled. What is the probability the sum is at least 10?','options':['1/12','1/6','1/4','1/3'],'answer':'1/6','explanation':'Sums 10, 11, and 12 have 3, 2, and 1 outcomes, for 6 of 36 total.'},
 {'prompt':'A number is chosen uniformly from 1 through 20. What is the probability it is divisible by 3?','options':['1/5','3/10','1/3','7/20'],'answer':'3/10','explanation':'The six qualifying numbers are 3, 6, 9, 12, 15, and 18, so 6/20 = 3/10.'},
 {'prompt':'Two cards are drawn without replacement from a standard deck. What is the probability both are aces?','options':['1/169','1/221','1/52','1/13'],'answer':'1/221','explanation':'The probability is (4/52) × (3/51) = 1/221.'},
 {'prompt':'Events A and B are mutually exclusive, with probabilities 0.2 and 0.35. What is P(A or B)?','options':['0.07','0.15','0.55','0.7'],'answer':'0.55','explanation':'Mutually exclusive events cannot happen together, so add their probabilities.'}],
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
 {'prompt':'A bat and ball cost $1.10 total. The bat costs $1 more than the ball. Ball cost?','options':['$0.05','$0.10','$0.15','$0.20'],'answer':'$0.05','explanation':'Ball is 5 cents; bat is $1.05.'},
 {'prompt':'What comes next in the sequence 1, 1, 2, 3, 5, 8, ...?','options':['11','12','13','15'],'answer':'13','explanation':'Each term is the sum of the two preceding terms.'},
 {'prompt':'Five people each shake hands with every other person once. How many handshakes occur?','options':['5','8','10','20'],'answer':'10','explanation':'Each handshake is a pair, so there are 5 × 4 / 2 = 10 pairs.'},
 {'prompt':'A farmer has 12 cows. All but 5 leave the field. How many stay?','options':['5','7','12','0'],'answer':'5','explanation':'“All but 5” means five remain.'},
 {'prompt':'A clock takes 5 seconds to strike 6 times. At the same pace, how long does it take to strike 12 times?','options':['10 seconds','11 seconds','12 seconds','15 seconds'],'answer':'11 seconds','explanation':'Six strikes have five gaps. Twelve strikes have eleven gaps, each one second long.'},
 {'prompt':'A number is doubled, then 6 is added, giving 20. What was the original number?','options':['5','6','7','8'],'answer':'7','explanation':'Undo the addition and doubling: (20 - 6) / 2 = 7.'},
 {'prompt':'A snail climbs a 10-meter pole. Each day it climbs 3 meters and each night slips 2 meters. On which day does it reach the top?','options':['Day 7','Day 8','Day 9','Day 10'],'answer':'Day 8','explanation':'After seven nights it is at 7 meters; on day eight it climbs the remaining 3 meters and does not slide back.'},
 {'prompt':'What is the smaller angle between clock hands at 6:00?','options':['90°','120°','180°','360°'],'answer':'180°','explanation':'At 6:00 the hands point in opposite directions.'},
 {'prompt':'A two-digit number has digits that sum to 9. Reversing its digits makes it 27 larger. What is the original number?','options':['36','45','54','63'],'answer':'36','explanation':'The digits differ by 3 and sum to 9, so they are 3 and 6; reversing 36 gives 63, which is 27 larger.'},
 {'prompt':'Three boxes are labeled Apples, Oranges, and Mixed, but every label is wrong. Which box should you sample from first to identify all boxes?','options':['The box labeled Mixed','The box labeled Apples','The box labeled Oranges','Any box; labels are not useful'],'answer':'The box labeled Mixed','explanation':'Since its label is wrong, that box contains only one fruit type. One sample identifies it, and the other two follow by elimination.'},
 {'prompt':'A snail is at the bottom of a 6-meter well. It climbs 2 meters by day and slips 1 meter by night. On which day does it get out?','options':['Day 4','Day 5','Day 6','Day 7'],'answer':'Day 5','explanation':'After four nights it is at 4 meters; on day five it climbs the final 2 meters and exits.'},
 {'prompt':'What is the next number: 2, 5, 10, 17, 26, ...?','options':['35','36','37','38'],'answer':'37','explanation':'The differences are 3, 5, 7, and 9; the next difference is 11.'}],
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
 {'prompt':'If 8 workers finish a task in 6 days, how many days for 12 equally productive workers?','options':['3','4','5','9'],'answer':'4','explanation':'Work is 48 worker-days; 48/12 = 4 days.'},
 {'prompt':'A product costs 500 before tax. A 10% tax is added. What is the total price?','options':['510','540','550','600'],'answer':'550','explanation':'Ten percent of 500 is 50, so the total is 550.'},
 {'prompt':'What is the simple interest on 2,000 at 5% per year for 3 years?','options':['100','250','300','350'],'answer':'300','explanation':'Simple interest is principal × rate × time = 2000 × 0.05 × 3.'},
 {'prompt':'A ratio is 2:3 and the total is 45. What is the larger share?','options':['18','24','27','30'],'answer':'27','explanation':'There are five parts, each worth 9; the larger share is 3 × 9.'},
 {'prompt':'A car travels at 72 km/h for 2.5 hours. How far does it travel?','options':['144 km','160 km','180 km','200 km'],'answer':'180 km','explanation':'Distance equals speed times time: 72 × 2.5 = 180.'},
 {'prompt':'A rectangle has perimeter 30 and length 9. What is its width?','options':['5','6','7','12'],'answer':'6','explanation':'Two lengths and two widths total 30, so the width is (30 - 18) / 2 = 6.'},
 {'prompt':'A quantity increases from 80 to 100. What is the percentage increase?','options':['20%','25%','30%','80%'],'answer':'25%','explanation':'The increase is 20, which is 20/80 = 25% of the starting value.'},
 {'prompt':'The average of 6, 8, and x is 10. What is x?','options':['12','14','16','18'],'answer':'16','explanation':'The three values must total 30, so x = 30 - 6 - 8 = 16.'},
 {'prompt':'A fair die is rolled 60 times. What is the expected number of sixes?','options':['6','10','12','15'],'answer':'10','explanation':'Each roll has a 1/6 chance of six, so the expected count is 60 × 1/6.'},
 {'prompt':'A tank is 3/5 full. After adding 20 liters it is full. What is the tank capacity?','options':['40 liters','45 liters','50 liters','60 liters'],'answer':'50 liters','explanation':'The added 20 liters fills the remaining 2/5, so the full tank holds 20 × 5/2 = 50 liters.'},
 {'prompt':'A shirt marked 1,200 is discounted by 15%. What is the sale price?','options':['960','1,000','1,020','1,080'],'answer':'1,020','explanation':'The discount is 180, leaving 1,020 to pay.'}]
}
ARENAS=set(QUESTIONS.keys())

DSA_DIFFICULTIES=[7,5,4,3,1,6,3,7,8,4,2,6,4,3,4,4,3,1,1,2,4,5,5,1,5,6,5,4,5,6,7,8,5,4,3,7,7,9,9,6,9,10,
                  3,4,4,1,3,3,3,2,4,5,5,6,6,5,7,6,5,3,3,7,5,9,6,7,7,6,8,3,4,4]
if len(DSA_DIFFICULTIES) != len(QUESTIONS['dsa']):
    raise RuntimeError('Every DSA question must have a difficulty rating')
for question, difficulty in zip(QUESTIONS['dsa'], DSA_DIFFICULTIES):
    question['difficulty']=difficulty

def difficulty_for_rating(rating):
    """Map Elo to a 1-10 question level; a new 1000-rated player starts at level 3."""
    level=3+int((rating-1000+75)//150)
    return max(1,min(10,level))

def questions_for_match(m, rating):
    questions=QUESTIONS[m.arena]
    # Match IDs give both players the same randomized questions and options while
    # producing a different draw and order for each new match.
    rng=random.Random(f'{m.id}:{m.arena}')
    if m.arena == 'dsa' and len(questions) > 10:
        target=difficulty_for_rating(rating)
        by_level={level:[] for level in range(1,11)}
        for question in questions:
            by_level[question['difficulty']].append(question)
        for group in by_level.values():
            rng.shuffle(group)
        selected=[]
        levels=sorted(by_level, key=lambda level:abs(level-target))
        index=0
        while len(selected)<10 and index<len(levels):
            distance=abs(levels[index]-target)
            nearest=[]
            while index<len(levels) and abs(levels[index]-target)==distance:
                nearest.append(levels[index]); index+=1
            rng.shuffle(nearest)
            for level in nearest:
                selected.extend(by_level[level][:10-len(selected)])
                if len(selected)==10:
                    break
        questions=selected
    else:
        questions=list(questions)
    rng.shuffle(questions)
    randomized=[]
    for question in questions[:10]:
        item=dict(question)
        item['options']=list(question['options'])
        rng.shuffle(item['options'])
        randomized.append(item)
    return randomized

def question_rating_for_match(player1, player2):
    if player1.username == 'MindClash_AI':
        return player2.rating
    if player2.username == 'MindClash_AI':
        return player1.rating
    return (player1.rating+player2.rating)/2

def user_json(u): return {'id':u.id,'username':u.username,'rating':u.rating,'wins':u.wins,'losses':u.losses,'matches':u.wins+u.losses}
def get_match_payload(m,u,s):
    opponent=s.get(User, m.player2 if u.id==m.player1 else m.player1)
    mine=m.score1 if u.id==m.player1 else m.score2
    theirs=m.score2 if u.id==m.player1 else m.score1
    match_rating=question_rating_for_match(u, opponent)
    qs=questions_for_match(m, match_rating)
    return {'id':m.id,'arena':m.arena,'status':m.status,'score':mine,'opponent_score':theirs,'opponent':user_json(opponent),'question_index':m.current_question,'total_questions':10,'question':{k:v for k,v in qs[m.current_question].items() if k!='answer' and k!='explanation'} if m.status=='active' and m.current_question<10 else None,'winner_id':m.winner_id}

@app.get('/health')
def health():
    try:
        redis_client.ping()
        return {'status':'ok','redis':'ok'}
    except RedisError:
        raise HTTPException(503,'Redis is unavailable')

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
    if active:
        if not expire_idle_match(active, s, u.id):
            remember_active_match(active.id, u.id)
            return {'matched':True,'match':get_match_payload(active,u,s)}
    try:
        paired_with=redis_client.get(f'mindclash:paired:{u.id}')
        if paired_with:
            pending=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
            if pending: return {'matched':True,'match':get_match_payload(pending,u,s)}
            return {'matched':False,'message':'Opponent found; preparing your match…'}
        reservation=secrets.token_urlsafe(18)
        other_keys=[]
        for other_arena in ARENAS:
            if other_arena != arena:
                other_keys.extend([f'mindclash:queue:{other_arena}', f'mindclash:queued_at:{other_arena}'])
        result=redis_client.eval(PAIR_SCRIPT, 2+len(other_keys), f'mindclash:queue:{arena}', f'mindclash:queued_at:{arena}', *other_keys, str(u.id), str(u.rating), str(int(datetime.now(timezone.utc).timestamp())), '100', 'mindclash:reserve:', reservation, 'mindclash:active:')
    except RedisError:
        raise HTTPException(503,'Matchmaking is temporarily unavailable. Start Redis and retry.')
    opponent_id=result[0]
    if opponent_id == '__ACTIVE__':
        active=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
        if active: return {'matched':True,'match':get_match_payload(active,u,s)}
        return {'matched':False,'message':'Preparing your match…'}
    if opponent_id == '__RESERVED__':
        return {'matched':False,'message':'Preparing your match…'}
    if not opponent_id:
        return {'matched':False,'message':'Searching for a similarly rated opponent','rating_window':int(result[1]),'waiting_seconds':int(result[2])}
    opponent=s.get(User,int(opponent_id))
    if not opponent:
        clear_tentative_active(result[3], u.id, opponent_id)
        release_reservations(result[3], u.id, opponent_id)
        return {'matched':False,'message':'Opponent disconnected; keep searching'}
    # Redis has already claimed both queue entries atomically. The database stores the match.
    m=Match(id=secrets.token_urlsafe(12),player1=u.id,player2=opponent.id,arena=arena)
    s.add(m)
    try:
        s.commit(); s.refresh(m)
    except Exception:
        s.rollback()
        clear_tentative_active(result[3], u.id, opponent.id)
        release_reservations(result[3], u.id, opponent.id)
        raise HTTPException(503,'Could not create the match. Please search again.')
    promote_active_markers(result[3], m.id, u.id, opponent.id)
    release_reservations(result[3], u.id, opponent.id)
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
    if active:
        if not expire_idle_match(active, s, u.id):
            remember_active_match(active.id, u.id)
            return {'matched':True,'match':get_match_payload(active,u,s)}
    reservation=secrets.token_urlsafe(18)
    try:
        if not redis_client.set(f'mindclash:reserve:{u.id}', reservation, nx=True, ex=30):
            active=s.query(Match).filter(Match.status=='active', (Match.player1==u.id)|(Match.player2==u.id)).first()
            if active: return {'matched':True,'match':get_match_payload(active,u,s)}
            raise HTTPException(409,'Another matchmaking request is being prepared. Please retry.')
        redis_client.set(f'mindclash:active:{u.id}', reservation, ex=30)
        for queued_arena in ARENAS:
            redis_client.zrem(f'mindclash:queue:{queued_arena}',str(u.id))
            redis_client.hdel(f'mindclash:queued_at:{queued_arena}',str(u.id))
    except RedisError:
        clear_tentative_active(reservation, u.id, u.id)
        release_reservations(reservation, u.id, u.id)
        raise HTTPException(503,'Matchmaking service is unavailable')
    try:
        bot=s.query(User).filter_by(username='MindClash_AI').first()
        if not bot:
            bot=User(username='MindClash_AI',email='mindclash-ai@local.invalid',password_hash=pwd.hash(secrets.token_urlsafe(32)))
            s.add(bot); s.flush()
        m=Match(id=secrets.token_urlsafe(12),player1=u.id,player2=bot.id,arena=arena)
        s.add(m); s.commit(); s.refresh(m)
    except Exception:
        s.rollback()
        clear_tentative_active(reservation, u.id, u.id)
        release_reservations(reservation, u.id, u.id)
        raise HTTPException(503,'Could not create the practice match. Please retry.')
    promote_active_markers(reservation, m.id, u.id)
    release_reservations(reservation, u.id, u.id)
    asyncio.create_task(run_bot_match(m.id, bot.id, u.rating))
    return {'matched':True,'match':get_match_payload(m,u,s),'bot':True}

@app.get('/matches/{match_id}')
def match_state(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if expire_idle_match(m, s):
        clear_active_markers(m.player1, m.player2)
    return get_match_payload(m,u,s)

def apply_match_result(m, s):
    """Apply Elo only to human matches, and update the completed match's record."""
    p1=s.get(User,m.player1); p2=s.get(User,m.player2)
    actual1=1.0 if m.winner_id==m.player1 else (0.0 if m.winner_id==m.player2 else 0.5)
    if 'MindClash_AI' in (p1.username,p2.username):
        return
    expected1=1/(1+10**((p2.rating-p1.rating)/400))
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
            clear_active_markers(m.player1, m.player2)
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
    session=SessionLocal()
    try:
        m=session.get(Match,match_id)
        if not m or uid not in (m.player1,m.player2):
            await websocket.close(code=4404); return
        await manager.connect(match_id,uid,websocket)
        await websocket.send_json({'type':'match_update','match':get_match_payload(m,session.get(User,uid),session)})
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                m=session.get(Match,match_id)
                if expire_idle_match(m, session):
                    clear_active_markers(m.player1, m.player2)
                    await manager.publish(m, session)
                    return
    except WebSocketDisconnect:
        manager.disconnect(match_id,websocket)
    finally:
        session.close()

@app.post('/matches/{match_id}/answer')
async def submit_answer(match_id:str,data:AnswerIn,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if expire_idle_match(m, s, u.id):
        clear_active_markers(m.player1, m.player2)
        await manager.publish(m, s)
        raise HTTPException(409,'Match ended after five minutes without activity')
    if m.status!='active': raise HTTPException(409,'Match has ended')
    if data.question_index!=m.current_question: raise HTTPException(409,'This question is no longer active')
    if data.question_index < 0 or data.question_index >= 10: raise HTTPException(400,'Invalid question index')
    player1=s.get(User,m.player1); player2=s.get(User,m.player2)
    match_rating=question_rating_for_match(player1, player2)
    qs=questions_for_match(m, match_rating)
    correct=data.answer.strip().casefold()==qs[m.current_question]['answer'].casefold()
    s.add(Submission(match_id=match_id,user_id=u.id,question_index=data.question_index,answer=data.answer[:500],correct=correct))
    if correct:
        # Conditional UPDATE is the atomic claim: only one correct submission can advance a question.
        score_col = Match.score1 if u.id==m.player1 else Match.score2
        result=s.query(Match).filter(Match.id==match_id,Match.status=='active',Match.current_question==data.question_index).update({score_col:score_col+1,Match.current_question:Match.current_question+1},synchronize_session=False)
        if result:
            s.flush(); s.refresh(m); finish_match_if_needed(m,s)
            s.commit(); s.refresh(m)
            if m.status=='finished': clear_active_markers(m.player1, m.player2)
            else: remember_active_match(m.id, m.player1, m.player2)
            await manager.publish(m,s)
            return {'correct':True,'message':'Correct! You claimed the point.','match':get_match_payload(m,u,s),'explanation':qs[data.question_index]['explanation']}
        s.rollback()
        m=s.get(Match,match_id)
        return {'correct':True,'message':'Correct, but your opponent claimed this question first.','match':get_match_payload(m,u,s)}
    s.commit()
    remember_active_match(m.id, m.player1, m.player2)
    return {'correct':False,'message':'Not quite. Try again!','match':get_match_payload(m,u,s)}

async def run_bot_match(match_id, bot_id, player_rating):
    """A lightweight practice opponent. It solves at a rating-adjusted pace; no LLM/API key needed."""
    accuracy=max(0.35,min(0.82,0.58+(player_rating-1000)/2500))
    while True:
        lookup=SessionLocal()
        try:
            pending=lookup.get(Match,match_id)
            if not pending or pending.status!='active' or pending.current_question>=10: return
            question_index=pending.current_question
            player1=lookup.get(User,pending.player1); player2=lookup.get(User,pending.player2)
            match_rating=question_rating_for_match(player1,player2)
            question=questions_for_match(pending,match_rating)[question_index]
            difficulty=question.get('difficulty',3)
        finally:
            lookup.close()
        # The bot pauses to "think"; harder DSA questions add a little extra time.
        await asyncio.sleep(random.uniform(12,22)+max(0,difficulty-3)*0.75)
        session=SessionLocal()
        try:
            m=session.get(Match,match_id)
            if not m or m.status!='active': return
            # If the player advanced while the bot was thinking, start a fresh delay
            # for the new question instead of answering it immediately.
            if m.current_question!=question_index: continue
            if expire_idle_match(m, session):
                clear_active_markers(m.player1, m.player2)
                await manager.publish(m, session)
                return
            question_index=m.current_question
            if random.random() <= accuracy:
                score_col=Match.score2 if m.player2==bot_id else Match.score1
                claimed=session.query(Match).filter(Match.id==match_id,Match.status=='active',Match.current_question==question_index).update({score_col:score_col+1,Match.current_question:Match.current_question+1},synchronize_session=False)
                if claimed:
                    session.flush(); session.refresh(m); finish_match_if_needed(m,session)
                    session.commit(); session.refresh(m)
                    if m.status=='finished': clear_active_markers(m.player1, m.player2)
                    else: remember_active_match(m.id, m.player1, m.player2)
                    await manager.publish(m,session)
                    if m.status=='finished': return
        except Exception:
            session.rollback()
        finally:
            session.close()

@app.get('/matches/{match_id}/result')
def result(match_id:str,u:User=Depends(current_user),s:Session=Depends(db)):
    m=s.get(Match,match_id)
    if not m or u.id not in (m.player1,m.player2): raise HTTPException(404,'Match not found')
    if m.status!='finished': raise HTTPException(409,'Match is still active')
    return {'match':get_match_payload(m,u,s),'winner':user_json(s.get(User,m.winner_id)) if m.winner_id else None,'draw':m.winner_id is None}
