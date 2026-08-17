"""BuzzFlow — Agency OS (multi-tenant)
- Any business owner can CREATE their own workspace (becomes its one and only Owner)
- Employees JOIN a workspace via QR / invite link — never as Owner
- Every workspace's data (team, clients, tasks, updates, logo, report number)
  is completely walled off from every other workspace.
"""
import os, sqlite3, hashlib, secrets, json, io
from datetime import datetime, date, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def today_ist():
    return now_ist().date()
from functools import wraps
from flask import Flask, request, jsonify, session, send_from_directory, g

DATABASE_URL = os.environ.get('DATABASE_URL', '')   # Neon/Postgres in production
USE_PG = DATABASE_URL.startswith(('postgres://', 'postgresql://'))
if USE_PG:
    import psycopg2
    import psycopg2.extras

DB = os.path.join(os.path.dirname(__file__), 'buzzflow.db')
app = Flask(__name__, static_folder='static', static_url_path='')
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))


class PgConn:
    """Small adapter so the same sqlite-style code runs on Postgres:
    translates '?' placeholders, exposes .execute/.fetchone/.fetchall/.commit,
    and returns rows accessible by column name."""
    def __init__(self, url):
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = False

    class _Cur:
        def __init__(self, cur):
            self.cur = cur
        @property
        def lastrowid(self):
            row = self.cur.fetchone()
            return row['id'] if row else None
        def fetchone(self):
            return self.cur.fetchone()
        def fetchall(self):
            return self.cur.fetchall()
        def __iter__(self):
            return iter(self.cur.fetchall())

    _ID_TABLES = ('workspaces', 'users', 'clients', 'quotas', 'tasks', 'updates', 'services')

    def execute(self, sql, params=()):
        q = sql.replace('?', '%s')
        qs = q.lstrip()
        if qs.upper().startswith('INSERT') and 'RETURNING' not in q.upper():
            # only tables that actually have an id column
            target = qs.split()[2].split('(')[0].lower()
            if target in PgConn._ID_TABLES:
                q += ' RETURNING id'
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(q, params)
        except Exception:
            self.conn.rollback()
            raise
        return PgConn._Cur(cur)

    def executescript(self, script):
        cur = self.conn.cursor()
        cur.execute(script)
        self.conn.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

SERVICE_TYPES = ["Reel", "Post", "Story", "Shoot", "Influencer", "Meta Ad", "Blog/Article", "Website", "SEO"]

# ---------------- db ----------------
def db():
    if 'db' not in g:
        if USE_PG:
            g.db = PgConn(DATABASE_URL)
        else:
            g.db = sqlite3.connect(DB)
            g.db.row_factory = sqlite3.Row
            g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    d = g.pop('db', None)
    if d: d.close()

def _is_unique_violation(ex):
    if isinstance(ex, sqlite3.IntegrityError):
        return True
    return ex.__class__.__name__ == 'UniqueViolation' or 'unique' in str(ex).lower()

def hashpw(pw, salt):
    return hashlib.pbkdf2_hmac('sha256', pw.encode(), salt.encode(), 100_000).hex()

SCHEMA = """
    CREATE TABLE IF NOT EXISTS workspaces(
      id {PK}, name TEXT NOT NULL, invite_code TEXT NOT NULL, created_at TEXT);
    CREATE TABLE IF NOT EXISTS users(
      id {PK}, workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      name TEXT NOT NULL, email TEXT UNIQUE NOT NULL,
      salt TEXT NOT NULL, pw TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN('owner','admin','employee')),
      title TEXT DEFAULT '', active INTEGER DEFAULT 1, created_at TEXT);
    CREATE TABLE IF NOT EXISTS clients(
      id {PK}, workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      name TEXT NOT NULL, package TEXT DEFAULT '', fee BIGINT DEFAULT 0,
      contact_name TEXT DEFAULT '', contact_phone TEXT DEFAULT '', notes TEXT DEFAULT '',
      status TEXT DEFAULT 'active' CHECK(status IN('active','paused','ended')), created_at TEXT);
    CREATE TABLE IF NOT EXISTS quotas(
      id {PK}, client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
      service TEXT NOT NULL, monthly_target INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS tasks(
      id {PK}, workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      title TEXT NOT NULL, client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
      service TEXT DEFAULT 'Other', qty INTEGER DEFAULT 1,
      assignee_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      status TEXT DEFAULT 'todo' CHECK(status IN('todo','doing','review','done')),
      priority TEXT DEFAULT 'medium' CHECK(priority IN('low','medium','high','urgent')),
      due TEXT, notes TEXT DEFAULT '', created_by INTEGER, created_at TEXT, done_at TEXT);
    CREATE TABLE IF NOT EXISTS updates(
      id {PK}, workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
      text TEXT NOT NULL, created_at TEXT);
    CREATE TABLE IF NOT EXISTS tokens(
      token TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      created_at TEXT);
    CREATE TABLE IF NOT EXISTS wsettings(
      workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      key TEXT NOT NULL, value TEXT, PRIMARY KEY(workspace_id, key));
    CREATE TABLE IF NOT EXISTS services(
      id {PK}, workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      name TEXT NOT NULL);
"""

def init_db():
    if USE_PG:
        conn = PgConn(DATABASE_URL)
        conn.executescript(SCHEMA.format(PK='BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY'))
        conn.close()
    else:
        conn = sqlite3.connect(DB)
        conn.executescript(SCHEMA.format(PK='INTEGER PRIMARY KEY'))
        conn.commit()
        conn.close()

def get_ws_setting(wsid, key, default=None):
    r = db().execute("SELECT value FROM wsettings WHERE workspace_id=? AND key=?", (wsid, key)).fetchone()
    return r['value'] if r else default

def set_ws_setting(wsid, key, value):
    db().execute("""INSERT INTO wsettings(workspace_id,key,value) VALUES(?,?,?)
                    ON CONFLICT(workspace_id,key) DO UPDATE SET value=?""", (wsid, key, value, value))
    db().commit()

# ---------------- auth plumbing ----------------
def login_required(f):
    @wraps(f)
    def w(*a, **k):
        if 'uid' not in session: return jsonify(error="auth required"), 401
        return f(*a, **k)
    return w

def role_required(*roles):
    def deco(f):
        @wraps(f)
        def w(*a, **k):
            if 'uid' not in session: return jsonify(error="auth required"), 401
            if session.get('role') not in roles: return jsonify(error="forbidden"), 403
            return f(*a, **k)
        return w
    return deco

def me():
    return db().execute("SELECT * FROM users WHERE id=?", (session['uid'],)).fetchone()

def wsid():
    return session['ws']

@app.before_request
def token_auth():
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        row = db().execute("""SELECT u.id, u.role, u.workspace_id FROM tokens k
                              JOIN users u ON u.id=k.user_id
                              WHERE k.token=? AND u.active=1""", (auth[7:],)).fetchone()
        if row:
            session['uid'], session['role'], session['ws'] = row['id'], row['role'], row['workspace_id']

def issue_token(uid):
    token = secrets.token_hex(24)
    db().execute("INSERT INTO tokens(token,user_id,created_at) VALUES(?,?,?)",
                 (token, uid, now_ist().isoformat()))
    db().commit()
    return token

def userdict(u):
    return dict(id=u['id'], name=u['name'], role=u['role'], title=u['title'], email=u['email'])

def parse_join_token(tok):
    """'12.CODE' or a full URL containing '#join=12.CODE' -> (wsid, code) or None"""
    tok = (tok or '').strip()
    if '#join=' in tok:
        tok = tok.split('#join=')[-1]
    if '.' not in tok:
        return None
    a, b = tok.split('.', 1)
    if not a.isdigit() or not b:
        return None
    try:
        from urllib.parse import unquote
        return int(a), unquote(b)
    except Exception:
        return None

# ---------------- auth endpoints ----------------
@app.get('/api/join-info')
def join_info():
    p = parse_join_token(request.args.get('token', ''))
    if not p: return jsonify(error="Invalid invite link"), 400
    w = db().execute("SELECT id,name,invite_code FROM workspaces WHERE id=?", (p[0],)).fetchone()
    if not w or w['invite_code'] != p[1]:
        return jsonify(error="Invalid or expired invite link"), 404
    return jsonify(workspace_name=w['name'])

@app.post('/api/signup')
def signup():
    d = request.json or {}
    mode = d.get('mode') or 'create'
    name = (d.get('name') or '').strip()
    email = (d.get('email') or '').strip().lower()
    pw = d.get('password') or ''
    title = (d.get('title') or '').strip()
    if not name or not email or len(pw) < 4:
        return jsonify(error="Name, email and a password (min 4 chars) are required"), 400
    if '@' not in email or '.' not in email.split('@')[-1]:
        return jsonify(error="Please enter a valid email"), 400

    if mode == 'create':
        ws_name = (d.get('workspace_name') or '').strip()
        code = (d.get('invite_code') or '').strip()
        if not ws_name:
            return jsonify(error="Enter your company / workspace name"), 400
        if len(code) < 4:
            return jsonify(error="Set a team invite code (min 4 characters)"), 400
        role, title = 'owner', (title or 'Owner')
        cur = db().execute("INSERT INTO workspaces(name,invite_code,created_at) VALUES(?,?,?)",
                           (ws_name, code, now_ist().isoformat()))
        ws = cur.lastrowid
    else:  # join
        p = parse_join_token(d.get('join_token') or '')
        if not p:
            return jsonify(error="Paste the invite link or scan the QR from your owner"), 400
        w = db().execute("SELECT * FROM workspaces WHERE id=?", (p[0],)).fetchone()
        if not w or w['invite_code'] != p[1]:
            return jsonify(error="Invalid invite — ask your owner for a fresh link/QR"), 403
        role, ws = 'employee', w['id']
        title = title or 'Team Member'

    salt = secrets.token_hex(8)
    try:
        cur = db().execute("""INSERT INTO users(workspace_id,name,email,salt,pw,role,title,created_at)
                              VALUES(?,?,?,?,?,?,?,?)""",
                           (ws, name, email, salt, hashpw(pw, salt), role, title, now_ist().isoformat()))
        db().commit()
    except Exception as ex:
        if not _is_unique_violation(ex):
            raise
        if mode == 'create':
            db().execute("DELETE FROM workspaces WHERE id=?", (ws,)); db().commit()
        return jsonify(error="This email is already registered — please sign in"), 400
    uid = cur.lastrowid
    session['uid'], session['role'], session['ws'] = uid, role, ws
    u = db().execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    return jsonify(token=issue_token(uid), user=userdict(u))

@app.post('/api/login')
def login():
    d = request.json or {}
    u = db().execute("SELECT * FROM users WHERE email=? AND active=1",
                     ((d.get('email') or '').strip().lower(),)).fetchone()
    if not u or hashpw(d.get('password',''), u['salt']) != u['pw']:
        return jsonify(error="Invalid email or password"), 401
    session['uid'], session['role'], session['ws'] = u['id'], u['role'], u['workspace_id']
    return jsonify(token=issue_token(u['id']), user=userdict(u))

@app.post('/api/logout')
def logout():
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        db().execute("DELETE FROM tokens WHERE token=?", (auth[7:],)); db().commit()
    session.clear(); return jsonify(ok=True)

@app.get('/api/me')
@login_required
def whoami():
    return jsonify(user=userdict(me()))

# ---------------- invite / QR ----------------
@app.get('/api/invite-code')
@role_required('owner')
def invite_get():
    from urllib.parse import quote
    w = db().execute("SELECT * FROM workspaces WHERE id=?", (wsid(),)).fetchone()
    token = f"{w['id']}.{quote(w['invite_code'])}"
    return jsonify(invite_code=w['invite_code'], workspace_id=w['id'], join_token=token,
                   workspace_name=w['name'])

@app.put('/api/invite-code')
@role_required('owner')
def invite_set():
    code = ((request.json or {}).get('invite_code') or '').strip()
    if len(code) < 4: return jsonify(error="Code must be at least 4 characters"), 400
    db().execute("UPDATE workspaces SET invite_code=? WHERE id=?", (code, wsid()))
    db().commit()
    return jsonify(ok=True)

# ---------------- company profile (per workspace) ----------------
@app.get('/api/company')
def company_get():
    if 'uid' not in session:
        return jsonify(company={})
    raw = get_ws_setting(wsid(), 'company')
    return jsonify(company=json.loads(raw) if raw else {})

@app.put('/api/company')
@role_required('owner')
def company_put():
    d = request.json or {}
    data = {k: (d.get(k) or '').strip() for k in
            ('name','tagline','phone','email','address','website','gst','report_phone')}
    logo = d.get('logo') or ''
    if logo and not logo.startswith('data:image/'):
        return jsonify(error="Invalid logo image"), 400
    if len(logo) > 900_000:
        return jsonify(error="Logo too large — please use a smaller image"), 400
    data['logo'] = logo
    set_ws_setting(wsid(), 'company', json.dumps(data))
    return jsonify(ok=True)

# ---------------- dashboard ----------------
@app.get('/api/dashboard')
@login_required
def dashboard():
    d = db(); role = session['role']; uid = session['uid']; ws = wsid()
    month = today_ist().strftime('%Y-%m')
    where_emp = "" if role in ('owner','admin') else f" AND t.assignee_id={uid}"
    counts = {r['status']: r['n'] for r in d.execute(
        f"SELECT status, COUNT(*) n FROM tasks t WHERE t.workspace_id=?{where_emp} GROUP BY status", (ws,))}
    overdue = d.execute(f"""SELECT COUNT(*) n FROM tasks t WHERE t.workspace_id=? AND t.status!='done'
                            AND t.due < ?{where_emp}""", (ws, today_ist().isoformat())).fetchone()['n']
    total = sum(counts.values()) or 1
    done = counts.get('done', 0)
    out = dict(counts=counts, overdue=overdue, completion=round(done*100/total),
               month=today_ist().strftime('%B %Y'))
    if role in ('owner','admin'):
        out['clients'] = d.execute("SELECT COUNT(*) n FROM clients WHERE workspace_id=? AND status='active'", (ws,)).fetchone()['n']
        out['team'] = d.execute("SELECT COUNT(*) n FROM users WHERE workspace_id=? AND active=1", (ws,)).fetchone()['n']
        if role == 'owner':
            out['revenue'] = d.execute("SELECT COALESCE(SUM(fee),0) s FROM clients WHERE workspace_id=? AND status='active'", (ws,)).fetchone()['s']
    rows = d.execute("""SELECT service, SUM(qty) n FROM tasks WHERE workspace_id=? AND status='done'
                        AND substr(done_at,1,7)=? GROUP BY service""", (ws, month)).fetchall()
    out['delivered'] = {r['service']: r['n'] for r in rows}
    targets = d.execute("""SELECT q.service, SUM(q.monthly_target) n FROM quotas q
                           JOIN clients c ON c.id=q.client_id
                           WHERE c.workspace_id=? AND c.status='active' GROUP BY q.service""", (ws,)).fetchall()
    out['targets'] = {r['service']: r['n'] for r in targets}
    return jsonify(out)

# ---------------- AI insights ----------------
def plural(s):
    return (s[:-1] + "ies") if s.endswith("y") else (s + "s")

@app.get('/api/insights')
@login_required
def insights():
    d = db(); ws = wsid(); today = today_ist(); month = today.strftime('%Y-%m')
    days_in_month = (date(today.year + (today.month==12), (today.month%12)+1, 1) - timedelta(days=1)).day
    pace = today.day / days_in_month
    tips = []
    rows = d.execute("""SELECT t.title, t.due, u.name a, c.name cl FROM tasks t
        LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
        WHERE t.workspace_id=? AND t.status!='done' AND t.due < ? ORDER BY t.due LIMIT 5""",
        (ws, today.isoformat())).fetchall()
    for r in rows:
        tips.append(dict(level="red", icon="⚠️", text=f"Overdue: “{r['title']}” for {r['cl'] or '—'} — assigned to {r['a'] or 'nobody'}, was due {r['due']}."))
    for c in d.execute("SELECT * FROM clients WHERE workspace_id=? AND status='active'", (ws,)).fetchall():
        for q in d.execute("SELECT service, monthly_target FROM quotas WHERE client_id=?", (c['id'],)).fetchall():
            if q['monthly_target'] <= 0: continue
            done = d.execute("""SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE client_id=? AND service=?
                AND status='done' AND substr(done_at,1,7)=?""", (c['id'], q['service'], month)).fetchone()['n']
            expected = q['monthly_target'] * pace
            if done >= q['monthly_target']:
                tips.append(dict(level="green", icon="✅", text=f"{c['name']}: {q['service']} quota complete ({done}/{q['monthly_target']}). Consider upsell."))
            elif done < expected * 0.6 and pace > 0.3:
                tips.append(dict(level="red", icon="🔥", text=f"{c['name']} at risk: only {done}/{q['monthly_target']} {plural(q['service'])} done, {round(pace*100)}% of month gone."))
            elif done < expected:
                tips.append(dict(level="amber", icon="⏳", text=f"{c['name']} slightly behind on {plural(q['service'])} ({done}/{q['monthly_target']})."))
    rows = d.execute("""SELECT u.name, COUNT(*) n FROM tasks t JOIN users u ON u.id=t.assignee_id
        WHERE t.workspace_id=? AND t.status IN('todo','doing','review') GROUP BY u.id ORDER BY n DESC""", (ws,)).fetchall()
    if len(rows) >= 2 and rows[0]['n'] >= rows[-1]['n'] * 2 and rows[0]['n'] >= 4:
        tips.append(dict(level="amber", icon="⚖️", text=f"Workload imbalance: {rows[0]['name']} has {rows[0]['n']} open tasks vs {rows[-1]['name']}'s {rows[-1]['n']}. Consider redistributing."))
    yest = (now_ist() - timedelta(days=1)).date().isoformat()
    quiet = d.execute("""SELECT name FROM users WHERE workspace_id=? AND role='employee' AND active=1 AND id NOT IN
        (SELECT DISTINCT user_id FROM updates WHERE workspace_id=? AND substr(created_at,1,10) >= ?)""",
        (ws, ws, yest)).fetchall()
    if quiet:
        tips.append(dict(level="amber", icon="💬", text="No update in 24h from: " + ", ".join(r['name'] for r in quiet) + "."))
    n = d.execute("SELECT COUNT(*) n FROM tasks WHERE workspace_id=? AND assignee_id IS NULL AND status!='done'", (ws,)).fetchone()['n']
    if n: tips.append(dict(level="red", icon="🎯", text=f"{n} task(s) have no assignee — assign them now."))
    pend = d.execute("""SELECT u.name, COUNT(*) n, COALESCE(SUM(t.qty),0) q FROM tasks t
        JOIN users u ON u.id=t.assignee_id WHERE t.workspace_id=? AND t.status!='done'
        GROUP BY u.id ORDER BY n DESC LIMIT 3""", (ws,)).fetchall()
    for p in pend:
        tips.append(dict(level="amber", icon="🏃", text=f"{p['name']} has {p['n']} pending task(s) covering {p['q']} deliverable(s) — cheer them on!"))
    if not tips:
        tips.append(dict(level="green", icon="🌿", text="All clear! Add clients and tasks to get insights."))
    order = {"red":0, "amber":1, "green":2}
    tips.sort(key=lambda t: order[t['level']])
    return jsonify(insights=tips[:12])

# ---------------- WhatsApp daily report ----------------
@app.get('/api/report/daily')
@role_required('owner','admin')
def daily_report():
    d = db(); ws = wsid()
    raw = get_ws_setting(ws, 'company')
    company = json.loads(raw) if raw else {}
    phone = ''.join(ch for ch in (company.get('report_phone') or '') if ch.isdigit())
    if not phone:
        return jsonify(error="Set the WhatsApp report number first (Settings → Company profile)"), 400
    if len(phone) == 10:
        phone = '91' + phone
    today = today_ist(); tstr = today.isoformat(); month = today.strftime('%Y-%m')
    nice = today.strftime('%d %b %Y')
    co = company.get('name') or 'BuzzFlow'
    L = [f"📊 *{co} Daily Report — {nice}*", ""]
    done_today = d.execute("""SELECT t.title,t.qty,t.service,u.name a,c.name cl FROM tasks t
        LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
        WHERE t.workspace_id=? AND t.status='done' AND substr(t.done_at,1,10)=?""", (ws, tstr)).fetchall()
    total_q = sum(r['qty'] for r in done_today)
    L.append(f"✅ *Completed today:* {len(done_today)} task(s), {total_q} deliverable(s)")
    overdue = d.execute("SELECT COUNT(*) n FROM tasks WHERE workspace_id=? AND status!='done' AND due<?",
                        (ws, tstr)).fetchone()['n']
    L.append(f"🔥 *Overdue:* {overdue}")
    quotas = d.execute("""SELECT q.service, SUM(q.monthly_target) tgt FROM quotas q
        JOIN clients c ON c.id=q.client_id WHERE c.workspace_id=? AND c.status='active'
        GROUP BY q.service""", (ws,)).fetchall()
    if quotas:
        parts = []
        for q in quotas:
            done = d.execute("""SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE workspace_id=? AND service=?
                AND status='done' AND substr(done_at,1,7)=?""", (ws, q['service'], month)).fetchone()['n']
            parts.append(f"{plural(q['service'])} {done}/{q['tgt']}")
        L.append("📦 *Month:* " + " · ".join(parts))
    L.append("")
    L.append("👥 *EMPLOYEE-WISE:*")
    emps = d.execute("SELECT id,name FROM users WHERE workspace_id=? AND active=1 AND role!='owner' ORDER BY name", (ws,)).fetchall()
    if not emps:
        L.append("• No team members yet.")
    for e in emps:
        done_rows = d.execute("""SELECT t.qty,t.service,c.name cl FROM tasks t
            LEFT JOIN clients c ON c.id=t.client_id
            WHERE t.assignee_id=? AND t.status='done' AND substr(t.done_at,1,10)=?""", (e['id'], tstr)).fetchall()
        pending = d.execute("SELECT COUNT(*) n FROM tasks WHERE assignee_id=? AND status!='done'", (e['id'],)).fetchone()['n']
        if done_rows:
            done_txt = ", ".join(f"{r['qty']} {plural(r['service']) if r['qty']>1 else r['service']}" +
                                 (f" ({r['cl']})" if r['cl'] else "") for r in done_rows)
            L.append(f"• *{e['name']}* — ✅ {done_txt} completed · {pending} pending")
        else:
            L.append(f"• *{e['name']}* — ❌ nothing completed today · {pending} pending")
    L.append("")
    posted = d.execute("""SELECT DISTINCT u.name FROM updates up JOIN users u ON u.id=up.user_id
        WHERE up.workspace_id=? AND substr(up.created_at,1,10)=?""", (ws, tstr)).fetchall()
    silent = d.execute("""SELECT name FROM users WHERE workspace_id=? AND active=1 AND role='employee' AND id NOT IN
        (SELECT user_id FROM updates WHERE workspace_id=? AND substr(created_at,1,10)=?)""", (ws, ws, tstr)).fetchall()
    L.append("💬 *Updates posted:* " + (", ".join(r['name'] for r in posted) or "none"))
    if silent:
        L.append("🔕 *Silent today:* " + ", ".join(r['name'] for r in silent))
    L.append("")
    L.append("_Sent via BuzzFlow — Agency OS_")
    return jsonify(text="\n".join(L), phone=phone)

# ---------------- calendar ----------------
@app.get('/api/calendar')
@login_required
def calendar_data():
    d = db(); ws = wsid()
    month = request.args.get('month') or today_ist().strftime('%Y-%m')
    due = d.execute("""SELECT t.id,t.title,t.status,t.qty,t.service,t.due AS "day",u.name assignee,c.name client
        FROM tasks t LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
        WHERE t.workspace_id=? AND substr(t.due,1,7)=?""", (ws, month)).fetchall()
    done = d.execute("""SELECT t.id,t.title,t.status,t.qty,t.service,substr(t.done_at,1,10) AS "day",u.name assignee,c.name client
        FROM tasks t LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
        WHERE t.workspace_id=? AND t.done_at IS NOT NULL AND substr(t.done_at,1,7)=?""", (ws, month)).fetchall()
    ups = d.execute("""SELECT up.id, substr(up.created_at,1,10) AS "day", up.text, u.name, c.name client
        FROM updates up JOIN users u ON u.id=up.user_id LEFT JOIN clients c ON c.id=up.client_id
        WHERE up.workspace_id=? AND substr(up.created_at,1,7)=?""", (ws, month)).fetchall()
    return jsonify(month=month, due=[dict(r) for r in due], done=[dict(r) for r in done],
                   updates=[dict(r) for r in ups], today=today_ist().isoformat())

# ---------------- clients ----------------
def clean_service(s):
    s = (s or 'Other').strip()[:40]
    return s if s else 'Other'

@app.get('/api/clients')
@login_required
def clients_list():
    d = db(); ws = wsid(); month = today_ist().strftime('%Y-%m')
    rows = d.execute("SELECT * FROM clients WHERE workspace_id=? ORDER BY status='active' DESC, name", (ws,)).fetchall()
    out = []
    for c in rows:
        item = dict(id=c['id'], name=c['name'], package=c['package'], status=c['status'],
                    contact_name=c['contact_name'], contact_phone=c['contact_phone'], notes=c['notes'])
        if session['role'] == 'owner': item['fee'] = c['fee']
        qs = d.execute("SELECT service, monthly_target FROM quotas WHERE client_id=?", (c['id'],)).fetchall()
        prog = []
        for q in qs:
            done = d.execute("""SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE client_id=? AND service=?
                AND status='done' AND substr(done_at,1,7)=?""", (c['id'], q['service'], month)).fetchone()['n']
            prog.append(dict(service=q['service'], target=q['monthly_target'], done=done))
        item['quotas'] = prog
        out.append(item)
    return jsonify(clients=out)

@app.post('/api/clients')
@role_required('owner','admin')
def clients_add():
    d = request.json or {}
    if not (d.get('name') or '').strip(): return jsonify(error="name required"), 400
    cur = db().execute("""INSERT INTO clients(workspace_id,name,package,fee,contact_name,contact_phone,notes,created_at)
                          VALUES(?,?,?,?,?,?,?,?)""",
        (wsid(), d['name'].strip(), d.get('package',''), int(d.get('fee') or 0), d.get('contact_name',''),
         d.get('contact_phone',''), d.get('notes',''), now_ist().isoformat()))
    cid = cur.lastrowid
    for q in d.get('quotas', []):
        if int(q.get('target') or 0) > 0:
            svc = clean_service(q.get('service'))
            learn_service(svc)
            db().execute("INSERT INTO quotas(client_id,service,monthly_target) VALUES(?,?,?)",
                         (cid, svc, int(q['target'])))
    db().commit()
    return jsonify(id=cid)

def own_client(cid):
    return db().execute("SELECT * FROM clients WHERE id=? AND workspace_id=?", (cid, wsid())).fetchone()

@app.put('/api/clients/<int:cid>')
@role_required('owner','admin')
def clients_edit(cid):
    if not own_client(cid): return jsonify(error="not found"), 404
    d = request.json or {}
    db().execute("""UPDATE clients SET name=?,package=?,fee=?,contact_name=?,contact_phone=?,notes=?,status=?
                    WHERE id=? AND workspace_id=?""",
        (d.get('name'), d.get('package',''), int(d.get('fee') or 0), d.get('contact_name',''),
         d.get('contact_phone',''), d.get('notes',''), d.get('status','active'), cid, wsid()))
    if 'quotas' in d:
        db().execute("DELETE FROM quotas WHERE client_id=?", (cid,))
        for q in d['quotas']:
            if int(q.get('target') or 0) > 0:
                db().execute("INSERT INTO quotas(client_id,service,monthly_target) VALUES(?,?,?)",
                             (cid, clean_service(q.get('service')), int(q['target'])))
    db().commit()
    return jsonify(ok=True)

@app.post('/api/clients/<int:cid>/quotas')
@role_required('owner','admin')
def quota_upsert(cid):
    if not own_client(cid): return jsonify(error="not found"), 404
    d = request.json or {}
    svc = clean_service(d.get('service'))
    target = int(d.get('target') or 0)
    if target <= 0: return jsonify(error="Enter a monthly target"), 400
    learn_service(svc)
    ex = db().execute("SELECT id FROM quotas WHERE client_id=? AND LOWER(service)=LOWER(?)", (cid, svc)).fetchone()
    if ex:
        db().execute("UPDATE quotas SET monthly_target=? WHERE id=?", (target, ex['id']))
    else:
        db().execute("INSERT INTO quotas(client_id,service,monthly_target) VALUES(?,?,?)", (cid, svc, target))
    db().commit()
    return jsonify(ok=True)

@app.delete('/api/clients/<int:cid>')
@role_required('owner')
def clients_del(cid):
    if not own_client(cid): return jsonify(error="not found"), 404
    db().execute("DELETE FROM clients WHERE id=? AND workspace_id=?", (cid, wsid())); db().commit()
    return jsonify(ok=True)

# ---------------- tasks ----------------
@app.get('/api/tasks')
@login_required
def tasks_list():
    d = db()
    q = """SELECT t.*, u.name assignee, c.name client FROM tasks t
           LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
           WHERE t.workspace_id=?"""
    args = [wsid()]
    if request.args.get('mine') == '1':
        q += " AND t.assignee_id=?"; args.append(session['uid'])
    q += " ORDER BY CASE WHEN t.due IS NULL OR t.due='' THEN 1 ELSE 0 END, t.due, CASE t.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END"
    rows = [dict(r) for r in d.execute(q, args).fetchall()]
    return jsonify(tasks=rows, today=today_ist().isoformat())

@app.post('/api/tasks')
@login_required
def tasks_add():
    d = request.json or {}
    if not (d.get('title') or '').strip(): return jsonify(error="title required"), 400
    if session['role'] == 'employee':
        d['assignee_id'] = session['uid']
    cid = d.get('client_id')
    if cid and not own_client(cid): return jsonify(error="client not found"), 404
    aid = d.get('assignee_id')
    if aid:
        au = db().execute("SELECT 1 FROM users WHERE id=? AND workspace_id=?", (aid, wsid())).fetchone()
        if not au: return jsonify(error="assignee not found"), 404
    status = d.get('status','todo')
    done_at = now_ist().isoformat() if status == 'done' else None
    learn_service(clean_service(d.get('service')))
    cur = db().execute("""INSERT INTO tasks(workspace_id,title,client_id,service,qty,assignee_id,status,priority,due,notes,created_by,created_at,done_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (wsid(), d['title'].strip(), cid, clean_service(d.get('service')), int(d.get('qty') or 1),
         aid, status, d.get('priority','medium'), d.get('due'),
         d.get('notes',''), session['uid'], now_ist().isoformat(), done_at))
    db().commit()
    return jsonify(id=cur.lastrowid)

@app.put('/api/tasks/<int:tid>')
@login_required
def tasks_edit(tid):
    d = request.json or {}; conn = db()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND workspace_id=?", (tid, wsid())).fetchone()
    if not t: return jsonify(error="not found"), 404
    if session['role'] == 'employee' and t['assignee_id'] != session['uid']:
        return jsonify(error="forbidden"), 403
    fields = {}
    allowed = ['title','client_id','service','qty','status','priority','due','notes'] + \
              (['assignee_id'] if session['role'] in ('owner','admin') else [])
    for k in allowed:
        if k in d: fields[k] = d[k]
    if 'service' in fields:
        fields['service'] = clean_service(fields['service'])
        learn_service(fields['service'])
    if fields.get('client_id') and not own_client(fields['client_id']):
        return jsonify(error="client not found"), 404
    if fields.get('assignee_id'):
        au = conn.execute("SELECT 1 FROM users WHERE id=? AND workspace_id=?", (fields['assignee_id'], wsid())).fetchone()
        if not au: return jsonify(error="assignee not found"), 404
    if fields.get('status') == 'done' and t['status'] != 'done':
        fields['done_at'] = now_ist().isoformat()
    if fields.get('status') and fields['status'] != 'done' and t['status'] == 'done':
        fields['done_at'] = None
    if fields:
        sets = ",".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE tasks SET {sets} WHERE id=?", (*fields.values(), tid))
        conn.commit()
    return jsonify(ok=True)

@app.delete('/api/tasks/<int:tid>')
@role_required('owner','admin')
def tasks_del(tid):
    t = db().execute("SELECT 1 FROM tasks WHERE id=? AND workspace_id=?", (tid, wsid())).fetchone()
    if not t: return jsonify(error="not found"), 404
    db().execute("DELETE FROM tasks WHERE id=?", (tid,)); db().commit()
    return jsonify(ok=True)

# ---------------- updates ----------------
@app.get('/api/updates')
@login_required
def updates_list():
    rows = db().execute("""SELECT up.*, u.name, u.title, c.name client FROM updates up
        JOIN users u ON u.id=up.user_id LEFT JOIN clients c ON c.id=up.client_id
        WHERE up.workspace_id=? ORDER BY up.created_at DESC LIMIT 60""", (wsid(),)).fetchall()
    return jsonify(updates=[dict(r) for r in rows])

@app.post('/api/updates')
@login_required
def updates_add():
    d = request.json or {}
    if not (d.get('text') or '').strip(): return jsonify(error="text required"), 400
    cid = d.get('client_id')
    if cid and not own_client(cid): return jsonify(error="client not found"), 404
    db().execute("INSERT INTO updates(workspace_id,user_id,client_id,text,created_at) VALUES(?,?,?,?,?)",
                 (wsid(), session['uid'], cid, d['text'].strip(), now_ist().isoformat()))
    db().commit()
    return jsonify(ok=True)

# ---------------- team ----------------
@app.get('/api/team')
@login_required
def team_list():
    d = db(); ws = wsid()
    rows = d.execute("""SELECT id,name,email,role,title,active FROM users WHERE workspace_id=?
                        ORDER BY role='owner' DESC, role='admin' DESC, name""", (ws,)).fetchall()
    out = []
    for u in rows:
        open_n = d.execute("SELECT COUNT(*) n FROM tasks WHERE assignee_id=? AND status!='done'", (u['id'],)).fetchone()['n']
        done_m = d.execute("SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE assignee_id=? AND status='done' AND substr(done_at,1,7)=?",
                           (u['id'], today_ist().strftime('%Y-%m'))).fetchone()['n']
        out.append(dict(id=u['id'], name=u['name'], email=u['email'], role=u['role'], title=u['title'],
                        active=u['active'], open_tasks=open_n, delivered_month=done_m))
    return jsonify(team=out)

@app.post('/api/team')
@role_required('owner','admin')
def team_add():
    d = request.json or {}
    if not all((d.get(k) or '').strip() for k in ('name','email','password')):
        return jsonify(error="name, email, password required"), 400
    salt = secrets.token_hex(8)
    try:
        db().execute("""INSERT INTO users(workspace_id,name,email,salt,pw,role,title,created_at)
                        VALUES(?,?,?,?,?,?,?,?)""",
            (wsid(), d['name'].strip(), d['email'].strip().lower(), salt, hashpw(d['password'], salt),
             'employee', d.get('title',''), now_ist().isoformat()))
        db().commit()
    except Exception as ex:
        if not _is_unique_violation(ex):
            raise
        return jsonify(error="email already exists"), 400
    return jsonify(ok=True)

def own_user(uid):
    return db().execute("SELECT * FROM users WHERE id=? AND workspace_id=?", (uid, wsid())).fetchone()

@app.put('/api/team/<int:uid>')
@role_required('owner','admin')
def team_edit(uid):
    d = request.json or {}; conn = db()
    u = own_user(uid)
    if not u: return jsonify(error="not found"), 404
    if u['role'] == 'owner' and session['role'] != 'owner': return jsonify(error="forbidden"), 403
    conn.execute("UPDATE users SET name=?, title=?, active=? WHERE id=?",
                 (d.get('name', u['name']), d.get('title', u['title']), int(d.get('active', u['active'])), uid))
    if d.get('password'):
        salt = secrets.token_hex(8)
        conn.execute("UPDATE users SET salt=?, pw=? WHERE id=?", (salt, hashpw(d['password'], salt), uid))
    conn.commit()
    return jsonify(ok=True)

@app.put('/api/team/<int:uid>/role')
@role_required('owner')
def team_role(uid):
    new_role = ((request.json or {}).get('role') or '').strip()
    if new_role not in ('admin', 'employee'):
        return jsonify(error="Role must be admin or employee"), 400
    u = own_user(uid)
    if not u: return jsonify(error="not found"), 404
    if u['role'] == 'owner': return jsonify(error="the owner role cannot be changed"), 403
    db().execute("UPDATE users SET role=? WHERE id=?", (new_role, uid))
    db().execute("DELETE FROM tokens WHERE user_id=?", (uid,))
    db().commit()
    return jsonify(ok=True)

@app.delete('/api/team/<int:uid>')
@role_required('owner','admin')
def team_del(uid):
    conn = db()
    u = own_user(uid)
    if not u: return jsonify(error="not found"), 404
    if uid == session['uid']: return jsonify(error="you cannot delete yourself"), 400
    if u['role'] == 'owner': return jsonify(error="the owner account cannot be deleted"), 403
    if u['role'] == 'admin' and session['role'] != 'owner':
        return jsonify(error="only the owner can delete a manager"), 403
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit()
    return jsonify(ok=True)

# ---------------- work report (data + excel) ----------------
def build_report_data():
    d = db(); ws = wsid()
    month = today_ist().strftime('%Y-%m')
    month_name = today_ist().strftime('%B %Y')
    # per-employee summary
    emps = d.execute("""SELECT id,name,title,role FROM users WHERE workspace_id=? AND active=1
                        ORDER BY role='owner' DESC, name""", (ws,)).fetchall()
    emp_rows = []
    for e in emps:
        done_m = d.execute("""SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE assignee_id=? AND status='done'
                              AND substr(done_at,1,7)=?""", (e['id'], month)).fetchone()['n']
        open_n = d.execute("SELECT COUNT(*) n FROM tasks WHERE assignee_id=? AND status!='done'", (e['id'],)).fetchone()['n']
        overdue_n = d.execute("""SELECT COUNT(*) n FROM tasks WHERE assignee_id=? AND status!='done'
                                 AND due < ?""", (e['id'], today_ist().isoformat())).fetchone()['n']
        svc = d.execute("""SELECT service, SUM(qty) n FROM tasks WHERE assignee_id=? AND status='done'
                           AND substr(done_at,1,7)=? GROUP BY service ORDER BY n DESC""", (e['id'], month)).fetchall()
        emp_rows.append(dict(name=e['name'], title=e['title'] or '', role=e['role'],
                             delivered=done_m, open=open_n, overdue=overdue_n,
                             services=', '.join(f"{r['n']} {r['service']}" for r in svc) or '—'))
    # per-client progress
    cli_rows = []
    for c in d.execute("SELECT * FROM clients WHERE workspace_id=? AND status='active' ORDER BY name", (ws,)).fetchall():
        for q in d.execute("SELECT service, monthly_target FROM quotas WHERE client_id=?", (c['id'],)).fetchall():
            done = d.execute("""SELECT COALESCE(SUM(qty),0) n FROM tasks WHERE client_id=? AND service=?
                AND status='done' AND substr(done_at,1,7)=?""", (c['id'], q['service'], month)).fetchone()['n']
            cli_rows.append(dict(client=c['name'], service=q['service'],
                                 done=done, target=q['monthly_target'],
                                 pct=round(done*100/q['monthly_target']) if q['monthly_target'] else 0))
    # all tasks detail
    task_rows = [dict(r) for r in d.execute("""SELECT t.title, t.service, t.qty, t.status, t.priority, t.due,
        substr(t.done_at,1,10) done_on, u.name assignee, c.name client
        FROM tasks t LEFT JOIN users u ON u.id=t.assignee_id LEFT JOIN clients c ON c.id=t.client_id
        WHERE t.workspace_id=? ORDER BY CASE WHEN t.due IS NULL THEN 1 ELSE 0 END, t.due""", (ws,)).fetchall()]
    totals = dict(
        total_tasks=len(task_rows),
        done=len([t for t in task_rows if t['status']=='done']),
        open=len([t for t in task_rows if t['status']!='done']),
        overdue=len([t for t in task_rows if t['status']!='done' and t['due'] and t['due'] < today_ist().isoformat()]),
        delivered_month=sum(e['delivered'] for e in emp_rows))
    raw = get_ws_setting(ws, 'company')
    co = json.loads(raw) if raw else {}
    return dict(month=month_name, company=co.get('name') or 'BuzzFlow',
                generated=now_ist().strftime('%d %b %Y, %I:%M %p IST'),
                totals=totals, employees=emp_rows, clients=cli_rows, tasks=task_rows)

@app.get('/api/report/work')
@role_required('owner','admin')
def work_report_data():
    return jsonify(build_report_data())

@app.get('/api/report/work.xlsx')
@role_required('owner','admin')
def work_report_xlsx():
    import base64
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    r = build_report_data()
    wb = Workbook()
    head_fill = PatternFill('solid', fgColor='FF5A1F')
    head_font = Font(color='FFFFFF', bold=True)
    def sheet(ws_, title, headers, rows):
        ws_.title = title
        ws_.append(headers)
        for cell in ws_[1]:
            cell.fill = head_fill; cell.font = head_font
            cell.alignment = Alignment(horizontal='center')
        for row in rows: ws_.append(row)
        for col in ws_.columns:
            width = max(len(str(c.value or '')) for c in col) + 3
            ws_.column_dimensions[col[0].column_letter].width = min(width, 44)
    s1 = wb.active
    sheet(s1, 'Summary',
          ['Report', 'Month', 'Generated', 'Total tasks', 'Done', 'Open', 'Overdue', 'Delivered (month)'],
          [[r['company'] + ' — Work Report', r['month'], r['generated'],
            r['totals']['total_tasks'], r['totals']['done'], r['totals']['open'],
            r['totals']['overdue'], r['totals']['delivered_month']]])
    sheet(wb.create_sheet(), 'Employees',
          ['Name', 'Title', 'Role', 'Delivered this month', 'Open tasks', 'Overdue', 'Work breakdown'],
          [[e['name'], e['title'], e['role'], e['delivered'], e['open'], e['overdue'], e['services']] for e in r['employees']])
    sheet(wb.create_sheet(), 'Client quotas',
          ['Client', 'Service', 'Done', 'Target', '% complete'],
          [[c['client'], c['service'], c['done'], c['target'], str(c['pct']) + '%'] for c in r['clients']])
    sheet(wb.create_sheet(), 'All tasks',
          ['Task', 'Client', 'Service', 'Qty', 'Assignee', 'Status', 'Priority', 'Due', 'Done on'],
          [[t['title'], t['client'] or '', t['service'], t['qty'], t['assignee'] or '',
            t['status'], t['priority'], t['due'] or '', t['done_on'] or ''] for t in r['tasks']])
    buf = io.BytesIO()
    wb.save(buf)
    return jsonify(filename=f"BuzzFlow-Report-{today_ist().isoformat()}.xlsx",
                   b64=base64.b64encode(buf.getvalue()).decode())

# ---------------- services (per-workspace list) ----------------
def ws_services():
    if not get_ws_setting(wsid(), 'svc_seeded'):
        for n in SERVICE_TYPES:
            ex = db().execute("SELECT 1 FROM services WHERE workspace_id=? AND LOWER(name)=LOWER(?)",
                              (wsid(), n)).fetchone()
            if not ex:
                db().execute("INSERT INTO services(workspace_id,name) VALUES(?,?)", (wsid(), n))
        db().commit()
        set_ws_setting(wsid(), 'svc_seeded', '1')
    return db().execute("SELECT id,name FROM services WHERE workspace_id=? ORDER BY name", (wsid(),)).fetchall()

def learn_service(name):
    """Auto-add manually typed services to the workspace list."""
    name = (name or '').strip()[:40]
    if not name or name == 'Other':
        return
    ex = db().execute("SELECT 1 FROM services WHERE workspace_id=? AND LOWER(name)=LOWER(?)",
                      (wsid(), name)).fetchone()
    if not ex:
        db().execute("INSERT INTO services(workspace_id,name) VALUES(?,?)", (wsid(), name))
        db().commit()

@app.get('/api/services')
@login_required
def services_list():
    return jsonify(services=[dict(r) for r in ws_services()])

@app.post('/api/services')
@role_required('owner','admin')
def services_add():
    name = ((request.json or {}).get('name') or '').strip()[:40]
    if not name: return jsonify(error="name required"), 400
    ex = db().execute("SELECT 1 FROM services WHERE workspace_id=? AND LOWER(name)=LOWER(?)", (wsid(), name)).fetchone()
    if ex: return jsonify(error="Service already exists"), 400
    cur = db().execute("INSERT INTO services(workspace_id,name) VALUES(?,?)", (wsid(), name))
    db().commit()
    return jsonify(id=cur.lastrowid)

@app.delete('/api/services/<int:sid>')
@role_required('owner','admin')
def services_del(sid):
    r = db().execute("SELECT 1 FROM services WHERE id=? AND workspace_id=?", (sid, wsid())).fetchone()
    if not r: return jsonify(error="not found"), 404
    db().execute("DELETE FROM services WHERE id=?", (sid,)); db().commit()
    return jsonify(ok=True)

@app.get('/api/meta')
def meta():
    return jsonify(services=SERVICE_TYPES)

@app.get('/')
def index():
    return send_from_directory('static', 'index.html')

init_db()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
