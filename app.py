"""MyTube - 무료 호스팅(Render 등)용 버전
영상/썸네일/계정/댓글 데이터를 전부 S3 호환 저장소(Backblaze B2 등)에 보관해서
서버가 재시작/잠들어도 데이터가 사라지지 않아요.
환경변수가 없으면 ./data 폴더에 저장하는 '로컬 모드'로 동작해요 (테스트용).
"""
import json
import os
import re
import threading
import time
import uuid

from flask import (Flask, abort, redirect, render_template_string, request,
                   send_from_directory, session)
from werkzeug.security import check_password_hash, generate_password_hash

# ───────── 설정 (환경변수) ─────────
ADMINS = {a.strip() for a in os.environ.get("ADMINS", "하준").split(",") if a.strip()}
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "100"))
def env(name):
    """환경변수 앞뒤의 공백/줄바꿈/따옴표를 자동으로 제거 (붙여넣기 실수 방지)"""
    v = os.environ.get(name, "").strip().strip("\"'").strip()
    return v or None


S3_ENDPOINT = env("S3_ENDPOINT")      # 예: https://s3.us-west-004.backblazeb2.com
S3_REGION = env("S3_REGION")          # 예: us-west-004
S3_KEY_ID = env("S3_KEY_ID")
S3_SECRET = env("S3_SECRET")
S3_BUCKET = env("S3_BUCKET")
SECRET = env("SECRET_KEY") or os.urandom(32).hex()
ALLOWED = {"mp4", "webm", "mkv", "mov", "m4v", "3gp"}
URL_TTL = 60 * 60 * 6    # 영상 주소 유효시간(6시간)
BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data")


# ───────── 저장소 (B2/S3 또는 로컬) ─────────
class LocalStore:
    mode = "local"

    def _p(self, key):
        p = os.path.join(DATA_DIR, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def get_bytes(self, key):
        p = os.path.join(DATA_DIR, key)
        if not os.path.exists(p):
            return None
        with open(p, "rb") as f:
            return f.read()

    def put_bytes(self, key, data, ctype="application/octet-stream"):
        with open(self._p(key), "wb") as f:
            f.write(data)

    def put_file(self, key, fileobj, ctype="application/octet-stream"):
        with open(self._p(key), "wb") as f:
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

    def delete(self, key):
        try:
            os.remove(os.path.join(DATA_DIR, key))
        except OSError:
            pass

    def url(self, key):
        return "/local/" + key


class S3Store:
    mode = "s3"

    def __init__(self):
        import boto3
        from botocore.config import Config
        self.c = boto3.client(
            "s3", endpoint_url=S3_ENDPOINT, region_name=S3_REGION,
            aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET,
            config=Config(signature_version="s3v4",
                          s3={"addressing_style": "path"}))

    def get_bytes(self, key):
        from botocore.exceptions import ClientError
        try:
            return self.c.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("NoSuchKey", "404", "NotFound"):
                return None
            raise

    def put_bytes(self, key, data, ctype="application/octet-stream"):
        self.c.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=ctype)

    def put_file(self, key, fileobj, ctype="application/octet-stream"):
        self.c.upload_fileobj(fileobj, S3_BUCKET, key, ExtraArgs={"ContentType": ctype})

    def delete(self, key):
        try:
            self.c.delete_object(Bucket=S3_BUCKET, Key=key)
        except Exception:
            pass

    def url(self, key):
        # 버킷은 비공개 → 임시 서명 주소로 B2에서 직접 스트리밍 (서버 트래픽 안 씀)
        return self.c.generate_presigned_url(
            "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=URL_TTL)


def _diag():
    """로그에 설정 점검 결과를 출력 (비밀값 전체는 절대 출력하지 않음)"""
    if not any([S3_ENDPOINT, S3_REGION, S3_KEY_ID, S3_SECRET, S3_BUCKET]):
        return
    kid, sec = S3_KEY_ID or "", S3_SECRET or ""
    print(f"S3 설정 확인 | endpoint={S3_ENDPOINT} | region={S3_REGION} | bucket={S3_BUCKET}"
          f" | KEY_ID 길이={len(kid)} 앞3글자={kid[:3]!r}"
          f" | SECRET 길이={len(sec)} 앞1글자={sec[:1]!r}")
    if kid.startswith("K") or len(sec) < 30:
        print("⚠ KEY_ID와 SECRET이 서로 바뀐 것 같아요 (keyID는 짧고, applicationKey는 K로 시작하는 긴 값)")
    if len(kid) not in (12, 25):
        print("⚠ KEY_ID 길이가 이상해요. B2 keyID는 보통 25자예요 (keyName/버킷이름/applicationKey를 넣은 건 아닌지 확인)")
    if S3_ENDPOINT and not S3_ENDPOINT.startswith("https://"):
        print("⚠ S3_ENDPOINT는 https:// 로 시작해야 해요")
    if S3_ENDPOINT and S3_REGION and S3_REGION not in S3_ENDPOINT:
        print("⚠ S3_REGION이 S3_ENDPOINT 안의 지역과 달라요")


_diag()

if all([S3_ENDPOINT, S3_REGION, S3_KEY_ID, S3_SECRET, S3_BUCKET]):
    store = S3Store()
else:
    store = LocalStore()
    print("⚠ S3 환경변수 없음 → 로컬 모드(./data 에 저장)")


# ───────── 데이터(db/users): 메모리 캐시 + 저장소에 비동기 저장 ─────────
LOCK = threading.RLock()
_cache = {}
_dirty = set()
_ev = threading.Event()


def _state(name):
    with LOCK:
        if name not in _cache:
            raw = store.get_bytes(f"state/{name}.json")
            _cache[name] = json.loads(raw.decode("utf-8")) if raw else {}
        return _cache[name]


def _flush(name):
    with LOCK:
        data = json.dumps(_cache[name], ensure_ascii=False).encode("utf-8")
    store.put_bytes(f"state/{name}.json", data, "application/json")


def _writer():
    while True:
        _ev.wait()
        time.sleep(0.5)          # 짧은 시간 동안 변경을 모아서 한 번에 저장
        _ev.clear()
        with LOCK:
            names = list(_dirty)
            _dirty.clear()
        for n in names:
            try:
                _flush(n)
            except Exception as e:
                print("저장 실패, 재시도:", e)
                with LOCK:
                    _dirty.add(n)
                _ev.set()
                time.sleep(5)


threading.Thread(target=_writer, daemon=True).start()


def _save_state(name, sync=False):
    if sync:
        _flush(name)
    else:
        with LOCK:
            _dirty.add(name)
        _ev.set()


load = lambda: _state("db")
save = lambda d=None: _save_state("db")
load_users = lambda: _state("users")
save_users = lambda d=None, sync=False: _save_state("users", sync)

app = Flask(__name__)
app.secret_key = SECRET
app.config["MAX_CONTENT_LENGTH"] = (MAX_UPLOAD_MB + 2) * 1024 * 1024
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 30


def current():
    name = session.get("u")
    if not name:
        return None
    u = load_users().get(name)
    if not u:
        return None
    return {"name": name, "admin": name in ADMINS,
            "following": u.get("following", [])}


def ago(ts):
    s = int(time.time() - ts)
    for sec, name in ((86400, "일"), (3600, "시간"), (60, "분")):
        if s >= sec:
            return f"{s // sec}{name} 전"
    return "방금 전"


# ───────── 화면 ─────────
LAYOUT = """<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title }} - MyTube</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#0f0f0f;color:#f1f1f1;font-family:system-ui,sans-serif}
a{color:inherit;text-decoration:none}
header{position:sticky;top:0;z-index:5;background:#0f0f0fee;display:flex;gap:10px;
 align-items:center;padding:10px 14px;border-bottom:1px solid #272727;flex-wrap:wrap}
.logo{font-weight:800;font-size:20px;white-space:nowrap}
.logo b{background:#f00;border-radius:6px;padding:1px 6px;margin-right:4px}
form.s{flex:1;display:flex;min-width:140px}
form.s input{flex:1;min-width:0;background:#121212;border:1px solid #303030;color:#fff;
 padding:8px 14px;border-radius:20px 0 0 20px;outline:0}
form.s button{background:#222;border:1px solid #303030;color:#fff;padding:0 14px;
 border-radius:0 20px 20px 0}
.up{background:#272727;padding:8px 12px;border-radius:20px;font-size:14px;white-space:nowrap}
main{padding:14px;max-width:1400px;margin:auto}
.grid{display:grid;gap:18px;grid-template-columns:repeat(auto-fill,minmax(280px,1fr))}
.th{width:100%;aspect-ratio:16/9;background:#222;border-radius:12px;object-fit:cover;display:block}
.ph{display:flex;align-items:center;justify-content:center;font-size:34px;color:#555}
.card h3{margin:8px 0 2px;font-size:15px;line-height:1.3}
.meta{color:#aaa;font-size:13px}
.watch{max-width:1000px;margin:auto}
.watch video{width:100%;max-height:75vh;background:#000;border-radius:12px}
.watch h1{font-size:19px;margin:12px 0 6px}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:10px 0}
.btn{background:#272727;border:0;color:#fff;padding:8px 14px;border-radius:18px;font-size:14px}
.btn.red{background:#3a1515;color:#ff8080}
.btn.on{background:#fff;color:#000}
.btn.sub{background:#f00}
.desc{background:#272727;border-radius:12px;padding:12px;white-space:pre-wrap;font-size:14px}
.box{max-width:420px;margin:30px auto;background:#181818;padding:20px;border-radius:14px}
.box input,.box textarea{width:100%;margin:6px 0 14px;background:#121212;color:#fff;
 border:1px solid #303030;border-radius:8px;padding:10px}
.err{background:#3a1515;color:#ff8080;padding:10px;border-radius:8px;margin-bottom:12px}
.empty{text-align:center;color:#aaa;margin-top:80px}
.cm{display:flex;gap:10px;justify-content:space-between;padding:10px 0;border-bottom:1px solid #222}
.cm p{margin:4px 0 0;white-space:pre-wrap;word-break:break-word;font-size:14px}
.cm form{margin:0}
.cm .x{background:none;border:0;color:#888;font-size:13px}
.cform{display:flex;gap:8px;margin:12px 0}
.cform input{flex:1;min-width:0;background:#121212;border:1px solid #303030;color:#fff;
 padding:10px 14px;border-radius:20px;outline:0}
.chan{display:flex;gap:14px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
.avatar{width:64px;height:64px;border-radius:50%;background:#f00;display:flex;
 align-items:center;justify-content:center;font-size:28px;font-weight:700}
h2.t{margin:0 0 14px;font-size:18px}
</style></head><body>
<header>
 <a class="logo" href="/"><b>▶</b>MyTube</a>
 <form class="s" action="/"><input name="q" placeholder="검색" value="{{ q or '' }}"><button>🔍</button></form>
 {% if me %}
  <a class="up" href="/following">구독</a>
  <a class="up" href="/upload">＋ 업로드</a>
  <a class="up" href="/u/{{ me.name }}">👤 {{ me.name }}{% if me.admin %} ★{% endif %}</a>
  <a class="up" href="/logout">로그아웃</a>
 {% else %}
  <a class="up" href="/login">로그인</a>
  <a class="up" style="background:#f00" href="/register">가입</a>
 {% endif %}
</header>
<main>{{ body|safe }}</main>
</body></html>"""

GRID = """
{% macro thumb(v) %}{% if v.get('thumb') %}<img class="th" src="{{ url('thumbs/' ~ v.thumb) }}" loading="lazy" alt="">{% else %}<div class="th ph">▶</div>{% endif %}{% endmacro %}
{% macro grid(vids) %}
<div class="grid">
{% for v in vids %}
 <a class="card" href="/watch/{{ v.id }}">
  {{ thumb(v) }}
  <h3>{{ v.title }}</h3>
  <div class="meta">{{ v.get('owner') or '익명' }} · 조회수 {{ v.views }}회 · {{ ago(v.ts) }}</div>
 </a>
{% endfor %}</div>
{% endmacro %}
"""

HOME = GRID + """
{% if heading %}<h2 class="t">{{ heading }}</h2>{% endif %}
{% if vids %}{{ grid(vids) }}
{% else %}<div class="empty">{{ empty_msg or '영상이 없어요.' }}</div>{% endif %}
"""

CHANNEL = GRID + """
<div class="chan">
 <div class="avatar">{{ name[0] }}</div>
 <div style="flex:1">
  <h2 style="margin:0">{{ name }}{% if is_admin %} ★{% endif %}</h2>
  <div class="meta">구독자 {{ followers }}명 · 영상 {{ vids|length }}개</div>
 </div>
 {% if me and me.name != name %}
 <form method="post" action="/follow/{{ name }}">
  <input type="hidden" name="next" value="/u/{{ name }}">
  <button class="btn {{ 'on' if following else 'sub' }}">{{ '구독 중' if following else '구독' }}</button>
 </form>
 {% endif %}
</div>
{% if vids %}{{ grid(vids) }}{% else %}<div class="empty">올린 영상이 없어요.</div>{% endif %}
"""

WATCH = GRID + """
<div class="watch">
 <video src="{{ url('videos/' ~ v.file) }}" {% if v.get('thumb') %}poster="{{ url('thumbs/' ~ v.thumb) }}"{% endif %}
  controls playsinline preload="metadata"></video>
 <h1>{{ v.title }}</h1>
 <div class="row" style="justify-content:space-between">
  <div>
   <a href="/u/{{ v.get('owner') }}"><b>{{ v.get('owner') or '익명' }}</b></a>
   <div class="meta">조회수 {{ v.views }}회 · {{ ago(v.ts) }}</div>
  </div>
  {% if v.get('owner') and me and me.name != v.owner %}
  <form method="post" action="/follow/{{ v.owner }}">
   <input type="hidden" name="next" value="/watch/{{ v.id }}">
   <button class="btn {{ 'on' if following else 'sub' }}">{{ '구독 중' if following else '구독' }}</button>
  </form>
  {% endif %}
 </div>
 <div class="row">
  <form method="post" action="/like/{{ v.id }}">
   <button class="btn {{ 'on' if liked else '' }}">👍 {{ likes }}</button></form>
  {% if can_delete %}
  <form method="post" action="/delete/{{ v.id }}" onsubmit="return confirm('삭제할까요?')">
   <button class="btn red">🗑 삭제</button></form>
  {% endif %}
 </div>
 {% if v.desc %}<div class="desc">{{ v.desc }}</div>{% endif %}

 <h3 style="margin-top:26px">댓글 {{ comments|length }}개</h3>
 {% if me %}
 <form class="cform" method="post" action="/comment/{{ v.id }}">
  <input name="text" maxlength="500" placeholder="댓글 추가..." required>
  <button class="btn">등록</button>
 </form>
 {% else %}<p class="meta"><a href="/login?next=/watch/{{ v.id }}"><u>로그인</u>하면 댓글을 쓸 수 있어요.</a></p>{% endif %}
 {% for c in comments %}
 <div class="cm">
  <div>
   <a href="/u/{{ c.user }}"><b style="font-size:13px">{{ c.user }}</b></a>
   <span class="meta"> · {{ ago(c.ts) }}</span>
   <p>{{ c.text }}</p>
  </div>
  {% if me and (me.admin or me.name == c.user or me.name == v.get('owner')) %}
  <form method="post" action="/comment/{{ v.id }}/{{ c.id }}/delete" onsubmit="return confirm('댓글을 삭제할까요?')">
   <button class="x">삭제</button></form>
  {% endif %}
 </div>
 {% endfor %}
</div>
<h3 style="margin-top:28px">다른 영상</h3>
{{ grid(others) }}
"""

UPLOAD = """
<form class="box" style="max-width:520px" method="post" enctype="multipart/form-data" id="uf">
 <h2 style="margin-top:0">영상 업로드</h2>
 제목<input name="title" required>
 설명<textarea name="desc" rows="3"></textarea>
 영상 파일 <span class="meta">(최대 {{ max_mb }}MB · 작을수록 좋아요)</span>
 <input type="file" name="video" id="vf" accept="video/*" required>
 <input type="file" name="thumb" id="th" hidden>
 <button class="btn" id="ub" style="background:#f00;width:100%;padding:12px">업로드</button>
</form>
<script>
const MAX={{ max_mb }}*1024*1024, vf=document.getElementById('vf'), th=document.getElementById('th');
vf.addEventListener('change',()=>{
 const f=vf.files[0]; if(!f) return;
 if(f.size>MAX){alert('파일이 너무 커요 (최대 {{ max_mb }}MB)'); vf.value=''; return;}
 // 썸네일을 브라우저에서 만들어서 같이 올려요 (서버 부담 0)
 const v=document.createElement('video'); v.muted=true; v.playsInline=true; v.preload='metadata';
 const u=URL.createObjectURL(f); v.src=u;
 v.onloadedmetadata=()=>{v.currentTime=Math.min(1,(v.duration||2)/2)};
 v.onseeked=()=>{
  try{
   const c=document.createElement('canvas'); c.width=480;
   c.height=Math.round(480*v.videoHeight/v.videoWidth)||270;
   c.getContext('2d').drawImage(v,0,0,c.width,c.height);
   c.toBlob(b=>{ if(!b) return;
     const dt=new DataTransfer(); dt.items.add(new File([b],'t.jpg',{type:'image/jpeg'}));
     th.files=dt.files; URL.revokeObjectURL(u);},'image/jpeg',0.7);
  }catch(e){}
 };
});
document.getElementById('uf').addEventListener('submit',()=>{
 const b=document.getElementById('ub'); b.textContent='업로드 중... 잠시만요'; b.disabled=true;
 setTimeout(()=>{},0);
});
</script>
"""

AUTH = """
<form class="box" method="post">
 <h2 style="margin-top:0">{{ '회원가입' if register else '로그인' }}</h2>
 {% if error %}<div class="err">{{ error }}</div>{% endif %}
 아이디<input name="username" required autocomplete="username" autocapitalize="none">
 비밀번호<input name="password" type="password" required minlength="4"
  autocomplete="{{ 'new-password' if register else 'current-password' }}">
 {% if register %}비밀번호 확인<input name="password2" type="password" required>{% endif %}
 <input type="hidden" name="next" value="{{ nxt }}">
 <button class="btn" style="background:#f00;width:100%;padding:12px">
  {{ '가입하기' if register else '로그인' }}</button>
 <p class="meta" style="text-align:center">
  {% if register %}이미 계정이 있나요? <a href="/login"><u>로그인</u></a>
  {% else %}계정이 없나요? <a href="/register"><u>가입</u></a>{% endif %}</p>
</form>
"""


def page(title, tpl, q="", **ctx):
    me = current()
    body = render_template_string(tpl, ago=ago, me=me, url=store.url, **ctx)
    return render_template_string(LAYOUT, title=title, body=body, q=q, me=me)


def sorted_vids(db):
    return sorted(db.values(), key=lambda v: v["ts"], reverse=True)


def safe_next(n):
    return n if n and n.startswith("/") and not n.startswith("//") else "/"


@app.errorhandler(413)
def too_big(e):
    return f"파일이 너무 커요. 최대 {MAX_UPLOAD_MB}MB까지 올릴 수 있어요.", 413


@app.route("/healthz")
def healthz():
    return "ok"   # 슬립 방지용 핑 주소


@app.route("/local/<path:key>")
def local_file(key):
    if store.mode != "local" or key.startswith("state/"):
        abort(404)
    return send_from_directory(DATA_DIR, key, conditional=True)


# ───────── 계정 ─────────
@app.route("/register", methods=["GET", "POST"])
def register():
    nxt = safe_next(request.values.get("next"))
    if request.method == "GET":
        return page("가입", AUTH, register=True, nxt=nxt)
    name = request.form.get("username", "").strip()
    pw, pw2 = request.form.get("password", ""), request.form.get("password2", "")
    err = None
    with LOCK:
        users = load_users()
        if not re.fullmatch(r"[A-Za-z0-9_가-힣]{2,20}", name):
            err = "아이디는 2~20자의 한글/영문/숫자/_ 만 가능해요."
        elif name.lower() in {u.lower() for u in users}:
            err = "이미 사용 중인 아이디예요."
        elif len(pw) < 4:
            err = "비밀번호는 4자 이상이어야 해요."
        elif pw != pw2:
            err = "비밀번호가 서로 달라요."
        if not err:
            users[name] = {"hash": generate_password_hash(pw), "following": [],
                           "ts": time.time()}
            save_users(sync=True)     # 가입 정보는 바로 저장
    if err:
        return page("가입", AUTH, register=True, nxt=nxt, error=err)
    session.permanent = True
    session["u"] = name
    return redirect(nxt)


@app.route("/login", methods=["GET", "POST"])
def login():
    nxt = safe_next(request.values.get("next"))
    if request.method == "GET":
        return page("로그인", AUTH, register=False, nxt=nxt)
    name = request.form.get("username", "").strip()
    u = load_users().get(name)
    if not u or not check_password_hash(u["hash"], request.form.get("password", "")):
        return page("로그인", AUTH, register=False, nxt=nxt,
                    error="아이디 또는 비밀번호가 틀렸어요.")
    session.permanent = True
    session["u"] = name
    return redirect(nxt)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


# ───────── 영상 ─────────
@app.route("/")
def home():
    q = request.args.get("q", "").strip().lower()
    vids = sorted_vids(load())
    if q:
        vids = [v for v in vids if q in v["title"].lower() or q in v["desc"].lower()]
    return page("홈", HOME, q=q, vids=vids,
                empty_msg="영상이 없어요. 로그인하고 첫 영상을 올려보세요!")


@app.route("/following")
def following_feed():
    me = current()
    if not me:
        return redirect("/login?next=/following")
    vids = [v for v in sorted_vids(load()) if v.get("owner") in me["following"]]
    return page("구독", HOME, vids=vids, heading="구독한 채널의 영상",
                empty_msg="구독한 채널의 영상이 없어요. 채널을 구독해 보세요!")


@app.route("/u/<name>")
def channel(name):
    users = load_users()
    if name not in users:
        abort(404)
    me = current()
    followers = sum(1 for u in users.values() if name in u.get("following", []))
    vids = [v for v in sorted_vids(load()) if v.get("owner") == name]
    return page(name, CHANNEL, name=name, vids=vids, followers=followers,
                is_admin=name in ADMINS,
                following=bool(me and name in me["following"]))


@app.route("/watch/<vid>")
def watch(vid):
    with LOCK:
        db = load()
        v = db.get(vid) or abort(404)
        v["views"] += 1
        save()
        snapshot = dict(db)
    me = current()
    likers = v.get("likers", [])
    can_delete = bool(me and (me["admin"] or me["name"] == v.get("owner")))
    others = [o for o in sorted_vids(snapshot) if o["id"] != vid][:12]
    return page(v["title"], WATCH, v=v, others=others, likes=len(likers),
                liked=bool(me and me["name"] in likers), can_delete=can_delete,
                comments=v.get("comments", []),
                following=bool(me and v.get("owner") in me["following"]))


@app.route("/upload", methods=["GET", "POST"])
def upload():
    me = current()
    if not me:
        return redirect("/login?next=/upload")
    if request.method == "GET":
        return page("업로드", UPLOAD, max_mb=MAX_UPLOAD_MB)
    f = request.files.get("video")
    if not f or not f.filename:
        return redirect("/upload")
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED:
        return "지원하지 않는 형식입니다 (mp4, webm, mkv, mov, m4v, 3gp)", 400
    vid = uuid.uuid4().hex[:10]
    fname = f"{vid}.{ext}"
    store.put_file(f"videos/{fname}", f.stream, f.mimetype or "video/mp4")
    thumb = None
    t = request.files.get("thumb")
    if t and t.filename:
        data = t.read(2 * 1024 * 1024)
        if data[:3] == b"\xff\xd8\xff":   # 진짜 JPEG인지 확인
            thumb = f"{vid}.jpg"
            store.put_bytes(f"thumbs/{thumb}", data, "image/jpeg")
    with LOCK:
        db = load()
        db[vid] = {
            "id": vid, "file": fname, "owner": me["name"], "thumb": thumb,
            "title": request.form.get("title", "").strip()[:100] or "제목 없음",
            "desc": request.form.get("desc", "").strip()[:2000],
            "views": 0, "likers": [], "comments": [], "ts": time.time(),
        }
        save()
    return redirect(f"/watch/{vid}")


@app.route("/like/<vid>", methods=["POST"])
def like(vid):
    me = current()
    if not me:
        return redirect(f"/login?next=/watch/{vid}")
    with LOCK:
        db = load()
        v = db.get(vid) or abort(404)
        likers = v.setdefault("likers", [])
        if me["name"] in likers:
            likers.remove(me["name"])
        else:
            likers.append(me["name"])
        save()
    return redirect(f"/watch/{vid}")


@app.route("/delete/<vid>", methods=["POST"])
def delete(vid):
    me = current()
    with LOCK:
        db = load()
        v = db.get(vid) or abort(404)
        # 관리자 또는 영상을 올린 본인만 삭제 가능
        if not me or not (me["admin"] or me["name"] == v.get("owner")):
            abort(403)
        db.pop(vid)
        save()
    store.delete(f"videos/{v['file']}")
    if v.get("thumb"):
        store.delete(f"thumbs/{v['thumb']}")
    return redirect("/")


# ───────── 댓글 ─────────
@app.route("/comment/<vid>", methods=["POST"])
def comment(vid):
    me = current()
    if not me:
        return redirect(f"/login?next=/watch/{vid}")
    text = request.form.get("text", "").strip()[:500]
    with LOCK:
        db = load()
        v = db.get(vid) or abort(404)
        if text:
            v.setdefault("comments", []).insert(0, {
                "id": uuid.uuid4().hex[:8], "user": me["name"],
                "text": text, "ts": time.time()})
            save()
    return redirect(f"/watch/{vid}")


@app.route("/comment/<vid>/<cid>/delete", methods=["POST"])
def comment_delete(vid, cid):
    me = current()
    with LOCK:
        db = load()
        v = db.get(vid) or abort(404)
        c = next((c for c in v.get("comments", []) if c["id"] == cid), None)
        if not c:
            abort(404)
        if not me or not (me["admin"] or me["name"] == c["user"]
                          or me["name"] == v.get("owner")):
            abort(403)
        v["comments"].remove(c)
        save()
    return redirect(f"/watch/{vid}")


# ───────── 구독 ─────────
@app.route("/follow/<name>", methods=["POST"])
def follow(name):
    me = current()
    nxt = safe_next(request.form.get("next") or f"/u/{name}")
    if not me:
        return redirect(f"/login?next={nxt}")
    with LOCK:
        users = load_users()
        if name not in users or name == me["name"]:
            abort(400)
        fl = users[me["name"]].setdefault("following", [])
        if name in fl:
            fl.remove(name)
        else:
            fl.append(name)
        save_users()
    return redirect(nxt)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
