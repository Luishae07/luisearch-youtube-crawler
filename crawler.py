#!/usr/bin/env python3
"""Always-on YouTube video crawler for Luisearch.

Loops forever over an ever-refreshing set of search queries, scrapes
YouTube's search results pages (no API key -- parses the ytInitialData JSON
YouTube embeds in the page, same technique yt-dlp uses), and POSTs batches
to Luisearch's existing /api/crawl-ingest endpoint.

Runs as a Render free-tier Web Service (Background Worker needs a paid
plan) -- binds $PORT with a trivial health-check HTTP server while the real
crawl loop runs in a background thread, same pattern as luisearch-apify-
crawler's backfill_crawler.py on this same Render account.
"""
import http.server
import itertools
import json
import os
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

INGEST_URL = os.environ.get('INGEST_URL', '')
INGEST_TOKEN = os.environ.get('INGEST_TOKEN', '')
YOUTUBE_HOSTS = ('youtube.com', 'www.youtube.com', 'youtu.be', 'm.youtube.com')

TOPICS = [
    "python", "javascript", "typescript", "rust", "go programming", "java", "c++", "c#", "swift", "kotlin",
    "react", "vue", "angular", "next.js", "svelte", "node.js", "django", "flask", "spring boot", "ruby on rails",
    "docker", "kubernetes", "terraform", "ansible", "jenkins", "github actions", "ci cd", "devops", "aws", "azure",
    "google cloud", "linux", "bash scripting", "vim", "neovim", "git", "sql", "postgresql", "mongodb", "redis",
    "machine learning", "deep learning", "neural networks", "data science", "pandas", "numpy", "tensorflow", "pytorch",
    "computer vision", "natural language processing", "reinforcement learning", "algorithms", "data structures",
    "system design", "system architecture", "clean code", "design patterns", "microservices", "graphql", "rest api",
    "websockets", "cybersecurity", "ethical hacking", "penetration testing", "ctf", "networking", "cryptography",
    "reverse engineering", "malware analysis", "web3", "blockchain", "smart contracts", "solidity", "nft",
    "cryptocurrency", "bitcoin", "ethereum", "trading", "investing", "personal finance", "stock market", "real estate",
    "physics", "chemistry", "biology", "mathematics", "calculus", "linear algebra", "statistics", "astronomy",
    "space exploration", "geography", "history", "philosophy", "psychology", "economics", "sociology",
    "cooking", "baking", "grilling", "meal prep", "vegan recipes", "italian cooking", "japanese cooking",
    "fitness", "weight lifting", "yoga", "running", "home workout", "nutrition", "meditation", "mental health",
    "guitar", "piano", "drums", "singing", "music theory", "music production", "audio engineering",
    "photography", "portrait photography", "landscape photography", "video editing", "premiere pro",
    "graphic design", "photoshop", "illustrator", "figma", "ui design", "ux design", "3d modeling", "blender",
    "drawing", "digital art", "procreate", "animation", "unity", "unreal engine", "game development", "game design",
    "language learning", "spanish", "japanese", "french", "german", "mandarin", "korean", "italian",
    "travel vlog", "budget travel", "backpacking", "van life", "documentary", "true crime", "history documentary",
    "tech review", "smartphone review", "laptop review", "pc build", "gaming pc build", "product unboxing",
    "diy", "woodworking", "home improvement", "renovation", "gardening", "car repair", "motorcycle maintenance",
    "electric vehicles", "tesla", "drone", "3d printing", "arduino", "raspberry pi", "electronics",
    "startup", "entrepreneurship", "marketing", "seo", "social media marketing", "copywriting", "sales",
    "productivity", "study tips", "note taking", "time management", "public speaking", "interview tips",
    "resume writing", "job interview", "career advice", "freelancing", "remote work",
    "book review", "podcast", "stand up comedy", "movie review", "tv show review", "anime review",
    "speedrun", "esports", "retro gaming", "board games", "chess", "poker",
    "ai", "artificial intelligence", "chatgpt", "prompt engineering", "large language models", "generative ai",
    "quantum computing", "robotics", "self driving cars", "space race", "climate change", "renewable energy",
]
MODIFIERS = ["tutorial", "explained", "for beginners", "crash course", "full course", "guide", "tips", "review", "2026"]


def all_queries():
    qs = [f'{t} {m}' for t in TOPICS for m in MODIFIERS]
    random.shuffle(qs)
    return qs


def _matching_brace(s, open_pos):
    """Given the index of a '{' in s, returns the index just past its
    matching '}', properly skipping over string literals (so braces inside
    a title/description don't throw off the count)."""
    depth = 0
    i = open_pos
    in_string = False
    escape = False
    n = len(s)
    while i < n:
        c = s[i]
        if in_string:
            if escape:
                escape = False
            elif c == '\\':
                escape = True
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1
    return -1


def find_video_renderers(html):
    """Scans the raw page text for each `"videoRenderer":{...}` fragment and
    json.loads *only* that fragment, instead of parsing (and then
    recursively walking) the entire ytInitialData blob -- that blob is
    mostly unrelated UI config and can be 500KB+, so this skips the vast
    majority of the page's JSON entirely."""
    out = []
    marker = '"videoRenderer":'
    pos = 0
    while True:
        idx = html.find(marker, pos)
        if idx == -1:
            break
        brace = html.find('{', idx)
        if brace == -1:
            break
        end = _matching_brace(html, brace)
        if end == -1:
            break
        try:
            out.append(json.loads(html[brace:end]))
        except Exception:
            pass
        pos = end
    return out


def text_from_runs(v):
    if not isinstance(v, dict):
        return ''
    runs = v.get('runs')
    if isinstance(runs, list):
        return ''.join(r.get('text', '') for r in runs if isinstance(r, dict))
    return v.get('simpleText', '') or ''


def parse_video_renderer(vr):
    video_id = vr.get('videoId')
    if not video_id:
        return None
    title = text_from_runs(vr.get('title'))
    if not title:
        return None
    channel = text_from_runs(vr.get('ownerText'))
    duration = text_from_runs(vr.get('lengthText'))
    thumbs = (vr.get('thumbnail') or {}).get('thumbnails') or []
    thumbnail = thumbs[-1]['url'] if thumbs else ''
    description = text_from_runs(vr.get('descriptionSnippet'))
    return {
        'url': f'https://www.youtube.com/watch?v={video_id}',
        'title': title,
        'channel': channel,
        'duration': duration,
        'thumbnail': thumbnail,
        'description': description,
    }


def search_youtube(query):
    url = 'https://www.youtube.com/results?search_query=' + urllib.parse.quote_plus(query)
    req = urllib.request.Request(url, headers={
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'),
        'Accept-Language': 'en-US,en;q=0.9',
        # urllib's cookie handling is opt-in via CookieJar/HTTPCookieProcessor,
        # which also correctly scopes cookies per-domain across redirects
        # (unlike a raw header) -- see the opener setup in crawl_forever().
    })
    try:
        with _opener.open(req, timeout=20) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f'fetch failed for {query!r}: {e}', flush=True)
        return []
    renderers = find_video_renderers(html)
    if not renderers:
        print(f'no videoRenderer fragments for {query!r} (page shape changed, or blocked)', flush=True)
        return []
    videos = [parse_video_renderer(vr) for vr in renderers]
    return [v for v in videos if v]


def ingest_batch(videos):
    if not videos or not INGEST_URL or not INGEST_TOKEN:
        return
    body = json.dumps({'pages': [], 'videos': videos}).encode()
    req = urllib.request.Request(
        INGEST_URL, data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {INGEST_TOKEN}'})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f'ingest batch of {len(videos)}: {resp.status} {resp.read().decode()}', flush=True)
    except urllib.error.HTTPError as e:
        print(f'ingest failed: {e.code} {e.read().decode()}', flush=True)
    except Exception as e:
        print(f'ingest failed: {e}', flush=True)


_opener = None


def crawl_forever():
    global _opener
    jar = urllib.request.HTTPCookieProcessor()
    _opener = urllib.request.build_opener(jar)
    # Consent bypass -- without it YouTube redirects unrecognized-region
    # requests through a cookie-consent page that loops forever. A real
    # cookie jar (unlike a plain header) survives the cross-host redirect
    # to consent.youtube.com/consent.google.com the same way a browser's does.
    import http.cookiejar
    for domain in ('.youtube.com', '.google.com'):
        for name, value in (('CONSENT', 'YES+cb.20210328-17-p0.en+FX+888'), ('SOCS', 'CAI')):
            c = http.cookiejar.Cookie(
                0, name, value, None, False, domain, True, domain.startswith('.'), '/', False,
                False, None, False, None, None, {})
            jar.cookiejar.set_cookie(c)

    # Concurrency instead of one query at a time -- the bottleneck here is
    # network round-trip latency, not CPU, so N workers genuinely crawl ~N
    # times faster on a single small instance. 6 is a moderate number: fast
    # enough to matter, not so many that Render's free-tier CPU share or
    # YouTube's per-IP rate limit gets hit.
    batch = []
    batch_lock = threading.Lock()

    def worker(query):
        results = search_youtube(query)
        print(f'{query!r}: {len(results)} videos', flush=True)
        with batch_lock:
            batch.extend(results)
            if len(batch) >= 100:
                to_send, batch[:] = batch[:], []
                ingest_batch(to_send)

    # Executor.map() eagerly consumes its whole iterable up front (it builds
    # the full task list before returning anything) -- with an infinite
    # itertools.cycle() that hangs forever before doing any real work. Keep
    # a bounded rolling window of in-flight futures instead, topping it back
    # up to WORKERS every time one finishes.
    WORKERS = 50
    query_iter = itertools.cycle(all_queries())
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        in_flight = {pool.submit(worker, next(query_iter)) for _ in range(WORKERS)}
        while True:
            done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                fut.result()  # surface any worker exception instead of swallowing it
                in_flight.add(pool.submit(worker, next(query_iter)))


class HealthHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'luisearch-youtube-crawler: running\n')


if __name__ == '__main__':
    threading.Thread(target=crawl_forever, daemon=True).start()
    port = int(os.environ.get('PORT', 8080))
    http.server.HTTPServer(('0.0.0.0', port), HealthHandler).serve_forever()
