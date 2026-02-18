#!/usr/bin/env python3
"""Parlami Agent Dashboard — Flask app serving agent status, cron, reports, alerts."""

import json
import glob
import os
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from functools import wraps

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.secret_key = os.environ.get("SECRET_KEY", "parlami-dev-key-change-me")

DASH_USER = os.environ.get("DASH_USER", "admin")
DASH_PASS = os.environ.get("DASH_PASS", "changeme")


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

# Base directory for this app
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(BASE_DIR, "data")

# Try local paths first, fall back to sample data
REPORTS_DIR = os.path.expanduser("~/clawd/parlami/reports")
CRON_FILE = os.path.expanduser("~/.openclaw/cron/jobs.json")
DATA_DIR = os.path.expanduser("~/Projects/parlami_ai/data")
APPROVALS_FILE = os.path.join(DATA_DIR, "approvals.json")
TZ = "America/Phoenix"

# Load sample data for deployment fallback
_sample_reports = None
_sample_cron = None

def _load_sample_reports():
    global _sample_reports
    if _sample_reports is None:
        try:
            with open(os.path.join(SAMPLE_DIR, "sample_reports.json")) as f:
                _sample_reports = json.load(f)
        except Exception:
            _sample_reports = {}
    return _sample_reports

def _load_sample_cron():
    global _sample_cron
    if _sample_cron is None:
        try:
            with open(os.path.join(SAMPLE_DIR, "sample_cron.json")) as f:
                _sample_cron = json.load(f)
        except Exception:
            _sample_cron = {"jobs": []}
    return _sample_cron

# ─── Supabase Integration ─────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
_SUPABASE_AVAILABLE = bool(SUPABASE_URL and SUPABASE_SERVICE_KEY and _REQUESTS_AVAILABLE)


def _sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def _sb_get(table, params=None, limit=500):
    """Fetch rows from a Supabase table. Returns list or None on error."""
    if not _SUPABASE_AVAILABLE:
        return None
    try:
        p = {"limit": limit, **(params or {})}
        resp = _requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_sb_headers(),
            params=p,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data if isinstance(data, list) else None
        return None
    except Exception:
        return None


def _sb_upsert(table, data):
    """Upsert a row or list of rows into a Supabase table."""
    if not _SUPABASE_AVAILABLE:
        return None
    try:
        headers = {
            **_sb_headers(),
            "Prefer": "resolution=merge-duplicates,return=representation",
        }
        payload = data if isinstance(data, list) else [data]
        resp = _requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            json=payload,
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return resp.json()
        return None
    except Exception:
        return None


def _parse_json_field(val):
    """Parse a JSON string field that Supabase may return as str or already parsed."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return val


AGENT_PROFILES = {
    "marco": {
        "id": "marco", "emoji": "☕", "name": "Marco", "color": "#6366f1",
        "title": "Direttore di Marketing", "title_en": "Marketing Director", "gender": "Male",
        "tagline": "Orchestrates all agents, synthesizes reports, makes strategic decisions",
        "personality": "Marco is the calm, strategic leader of the Parlami team. He starts every morning with an espresso and a full review of all agent reports. Decisive but collaborative, he ensures every marketing dollar is spent wisely.",
        "specialty": "Marketing Strategy & Agent Coordination",
        "goals": [
            "Maximize enrollment for both schools",
            "Optimize marketing spend across all channels",
            "Coordinate agent actions for maximum impact",
            "Deliver actionable morning briefings"
        ],
        "tools": [
            {"name": "Agent Reports", "icon": "📋"},
            {"name": "Discord Notifications", "icon": "💬"},
            {"name": "Command Interface", "icon": "🎮"}
        ],
        "data_sources": ["All agent reports", "School enrollment data", "Budget tracking"],
        "constraints": [
            "Cannot modify ad spend without Maestro's approval on changes >$100/day",
            "Cannot fire agencies",
            "Must present data before making strategic pivots"
        ],
        "output": [
            {"channel": "Discord #marco", "frequency": "Daily 8:30 AM", "type": "Morning Briefing"}
        ]
    },
    "annunci": {
        "id": "annunci", "emoji": "📣", "name": "Annunci", "color": "#f59e0b",
        "title": "Stratega Pubblicitario", "title_en": "Advertising Strategist", "gender": "Male",
        "tagline": "Monitors and optimizes Google Ads campaigns, creates ad creatives, manages Airtable pipeline",
        "personality": "Annunci is a data-obsessed performance marketer who lives and breathes CPA targets. He's always testing new ad copy, tweaking bids, and hunting for wasted spend. Methodical but creative when it comes to ad assets.",
        "specialty": "Google Ads Campaign Management & Optimization",
        "goals": [
            "Achieve CPA <$50 across all campaigns",
            "Maintain CTR >3%",
            "Maximize conversions while minimizing waste",
            "Build and manage creative pipeline in Airtable"
        ],
        "tools": [
            {"name": "Google Ads API", "icon": "📊"},
            {"name": "Airtable API", "icon": "📋"},
            {"name": "Gemini AI", "icon": "🤖"},
            {"name": "Pillow", "icon": "🖼️"},
            {"name": "Google Drive", "icon": "📁"}
        ],
        "data_sources": ["Google Ads account", "Airtable Creative Pipeline", "Gemini AI for copy"],
        "constraints": [
            "Cannot increase daily budget >20% without approval",
            "Cannot launch new campaigns without approval",
            "Must pause campaigns with 0 conversions after 7 days"
        ],
        "output": [
            {"channel": "Discord #annunci", "frequency": "Daily 8:00 AM", "type": "Ads Report"},
            {"channel": "Airtable", "frequency": "Continuous", "type": "Creative Pipeline"}
        ]
    },
    "bussola": {
        "id": "bussola", "emoji": "📊", "name": "Bussola", "color": "#06b6d4",
        "title": "Analista di Dati", "title_en": "Data Analyst", "gender": "Female",
        "tagline": "Single source of truth for all performance data, validates conversion tracking",
        "personality": "Bussola is the team's truth-teller. She doesn't care about opinions — only what the data says. Meticulous and skeptical, she catches tracking errors before they become expensive mistakes. The compass that keeps everyone honest.",
        "specialty": "Analytics & Conversion Tracking Validation",
        "goals": [
            "Ensure data accuracy across all platforms",
            "Track sessions, conversions, and bounce rates",
            "Detect tracking failures before they impact decisions",
            "Validate conversion data between GA4 and Google Ads"
        ],
        "tools": [
            {"name": "GA4 API", "icon": "📈"},
            {"name": "Google Search Console", "icon": "🔎"}
        ],
        "data_sources": ["GA4 (service account)", "Google Search Console"],
        "constraints": [
            "Read-only — cannot modify tracking code or GTM",
            "Cannot change GA4 settings",
            "Reports only, no direct action on websites"
        ],
        "output": [
            {"channel": "Discord #bussola", "frequency": "Daily 7:30 AM", "type": "Analytics Report"}
        ]
    },
    "spia": {
        "id": "spia", "emoji": "🔍", "name": "Spia", "color": "#8b5cf6",
        "title": "Agente di Intelligence SEO", "title_en": "SEO Intelligence Officer", "gender": "Female",
        "tagline": "Monitors organic search performance, keyword rankings, technical SEO health",
        "personality": "Spia operates in the shadows of search engines, constantly monitoring keyword movements and competitor activities. She's sharp, detail-oriented, and always three steps ahead of algorithm changes.",
        "specialty": "SEO Intelligence & Keyword Monitoring",
        "goals": [
            "Improve organic rankings for target keywords",
            "Maintain indexation health across both sites",
            "Monitor Core Web Vitals",
            "Track competitor SEO movements"
        ],
        "tools": [
            {"name": "Google Search Console", "icon": "🔎"},
            {"name": "SEMrush API", "icon": "📊"},
            {"name": "web_fetch", "icon": "🌐"}
        ],
        "data_sources": ["Google Search Console", "SEMrush", "Page audits via web_fetch"],
        "constraints": [
            "Cannot modify website content directly",
            "Recommendations only — no direct implementation",
            "Must coordinate with Penna for content changes"
        ],
        "output": [
            {"channel": "Discord #spia", "frequency": "Daily 7:00 AM", "type": "SEO Report"}
        ]
    },
    "stella": {
        "id": "stella", "emoji": "⭐", "name": "Stella", "color": "#ec4899",
        "title": "Custode della Reputazione", "title_en": "Reputation Guardian", "gender": "Female",
        "tagline": "Monitors Google Business Profile, reviews, online reputation",
        "personality": "Stella is the guardian of the schools' public image. She monitors every review, tracks sentiment trends, and ensures families see the best version of each school. Warm but vigilant — she takes negative reviews personally.",
        "specialty": "Online Reputation & Review Management",
        "goals": [
            "Maintain 4.5+ star rating on Google",
            "Respond to all reviews within 24 hours",
            "Generate new positive reviews from happy families",
            "Monitor online mentions and sentiment"
        ],
        "tools": [
            {"name": "Google Business Profile API", "icon": "📍"},
            {"name": "Brave Search", "icon": "🔍"},
            {"name": "Web Scraping", "icon": "🕷️"}
        ],
        "data_sources": ["Google Business Profile (pending)", "Brave Search", "Review sites"],
        "constraints": [
            "Cannot respond to reviews without approval",
            "Cannot create fake reviews",
            "Must escalate negative reviews immediately"
        ],
        "output": [
            {"channel": "Discord #stella", "frequency": "Daily 7:00 AM", "type": "GMB Report"}
        ]
    },
    "zona": {
        "id": "zona", "emoji": "📍", "name": "Zona", "color": "#10b981",
        "title": "Specialista SEO Locale", "title_en": "Local SEO Specialist", "gender": "Male",
        "tagline": "Manages community pages, zip code targeting, local search rankings, GeoGrid monitoring",
        "personality": "Zona knows every neighborhood, zip code, and community around both schools. He's the local expert who ensures families searching nearby always find Parlami's schools first. Territorial and thorough.",
        "specialty": "Local SEO & Geographic Targeting",
        "goals": [
            "Get both schools in Google Maps top 3 for target keywords",
            "Expand community page coverage for all nearby zip codes",
            "Monitor and improve GeoGrid rankings",
            "Drive local organic traffic"
        ],
        "tools": [
            {"name": "Google Maps Search", "icon": "🗺️"},
            {"name": "Brave Search", "icon": "🔍"},
            {"name": "GeoGrid Scanner", "icon": "📡"},
            {"name": "web_fetch", "icon": "🌐"}
        ],
        "data_sources": ["Google Maps", "Brave Search", "GeoGrid scans", "Community page audits"],
        "constraints": [
            "Cannot publish new pages without Architetto building them",
            "Must verify indexing before expanding to new areas",
            "Coordinates with Spia for keyword strategy"
        ],
        "output": [
            {"channel": "Discord #zona", "frequency": "Daily 7:15 AM", "type": "Local SEO Check"},
            {"channel": "Discord #zona", "frequency": "Daily 5:00 AM", "type": "GeoGrid Scan"}
        ]
    },
    "architetto": {
        "id": "architetto", "emoji": "🏗️", "name": "Architetto", "color": "#f97316",
        "title": "Architetto Web", "title_en": "Web Architect & Builder", "gender": "Male",
        "tagline": "Builds, optimizes, and monitors every page — from schema markup to AI-ready indexing",
        "personality": "Architetto is both the builder and the guardian. He doesn't just monitor websites — he constructs pages optimized for search engines, AI crawlers, and real human parents. Every page he touches gets structured data, compelling CTAs, FAQ schema, review markup, and keyword-rich content. If it's on the web, Architetto built it right.",
        "specialty": "Website Development, Technical SEO & AI-Ready Page Architecture",
        "goals": [
            "Build and optimize pages for both search engines AND AI indexing (ChatGPT, Perplexity, Google SGE)",
            "Implement Schema.org structured data on every page (LocalBusiness, FAQ, Review, Course, Event)",
            "Create high-converting landing pages with clear CTAs and trust signals",
            "Add FAQ sections that answer parent questions AND feed AI knowledge graphs",
            "Embed review/testimonial schema so ratings appear in search results",
            "Optimize keyword targeting in titles, headers, meta descriptions, and content",
            "Maintain 100% uptime and all forms working",
            "Keep all pages loading under 3 seconds (Core Web Vitals)",
            "Ensure proper indexing — submit to Search Console, manage sitemaps"
        ],
        "tools": [
            {"name": "WordPress API", "icon": "📝"},
            {"name": "web_fetch", "icon": "🌐"},
            {"name": "Google Search Console", "icon": "🔎"},
            {"name": "Schema.org Markup Generator", "icon": "🏷️"},
            {"name": "HTML/CSS Builder", "icon": "🎨"},
            {"name": "Gemini AI (content generation)", "icon": "🤖"},
            {"name": "Core Web Vitals Checker", "icon": "⚡"}
        ],
        "data_sources": ["Hourly URL health checks", "Google Search Console indexing", "WordPress API", "Schema validation tools", "PageSpeed Insights"],
        "constraints": [
            "Cannot modify production websites without Maestro's approval",
            "All new pages require review before publishing",
            "Must follow brand guidelines for each school",
            "Schema markup must validate against Schema.org standards",
            "Cannot remove existing pages without approval"
        ],
        "output": [
            {"channel": "Discord alerts", "frequency": "On failure only", "type": "URL Health Check"},
            {"channel": "Hourly", "frequency": "Every hour", "type": "Silent health scan"},
            {"channel": "WordPress", "frequency": "As needed", "type": "New pages, landing pages, schema updates"},
            {"channel": "Discord #architetto", "frequency": "Weekly", "type": "Technical SEO audit"}
        ]
    },
    "penna": {
        "id": "penna", "emoji": "✍️", "name": "Penna", "color": "#a855f7",
        "title": "Scrittrice di Contenuti", "title_en": "Content Writer", "gender": "Female",
        "tagline": "Writes SEO blog posts, ad copy, landing page content",
        "personality": "Penna is a wordsmith who crafts content that speaks to parents' hearts while satisfying search engine algorithms. She writes with warmth and authority, always matching each school's unique brand voice. Creative but disciplined.",
        "specialty": "SEO Content Writing & Brand Voice",
        "goals": [
            "Publish weekly blog posts for each school",
            "Create keyword-optimized content based on Spia's research",
            "Write engaging, parent-focused copy",
            "Maintain consistent brand voice across all content"
        ],
        "tools": [
            {"name": "Gemini AI", "icon": "🤖"},
            {"name": "SEMrush", "icon": "📊"},
            {"name": "WordPress API", "icon": "📝"}
        ],
        "data_sources": ["Keyword data from Spia", "SEMrush", "Brand guidelines"],
        "constraints": [
            'Never use the word "daycare"',
            "Must match each school's brand voice",
            "All posts require review before publishing",
            "Must incorporate target keywords naturally"
        ],
        "output": [
            {"channel": "Discord #penna", "frequency": "Tue/Wed 2:00 PM", "type": "Weekly Blog Posts"}
        ]
    },
    "piazza": {
        "id": "piazza", "emoji": "📱", "name": "Piazza", "color": "#e11d48",
        "title": "Gestore dei Social Media", "title_en": "Social Media Manager", "gender": "Female",
        "tagline": "Manages social media presence across Facebook, Instagram, Google Business Posts",
        "personality": "Piazza is the social butterfly of the team — always in tune with trends, hashtags, and what makes parents stop scrolling. She crafts posts that feel authentic and warm, turning each school's social feed into a window for prospective families. Energetic, creative, and always on-brand.",
        "specialty": "Social Media Strategy & Content Scheduling",
        "goals": [
            "Maintain a consistent posting schedule across all platforms",
            "Grow engagement rates month-over-month",
            "Drive traffic from social channels to school websites",
            "Build community and trust through authentic storytelling"
        ],
        "tools": [
            {"name": "Facebook Graph API", "icon": "📘"},
            {"name": "Instagram API", "icon": "📸"},
            {"name": "Google Business Profile API", "icon": "📍"},
            {"name": "Canva API", "icon": "🎨"}
        ],
        "data_sources": ["Facebook Insights", "Instagram Analytics", "Google Business Profile", "Airtable social calendar"],
        "constraints": [
            "Cannot post without Maestro's approval",
            "Must match each school's brand voice",
            "No controversial or political content",
            "Pending API access for Facebook and Instagram"
        ],
        "output": [
            {"channel": "Airtable", "frequency": "Continuous", "type": "Social Calendar"},
            {"channel": "Discord #piazza", "frequency": "Weekly", "type": "Social Performance Report"}
        ]
    },
    "faccia": {
        "id": "faccia", "emoji": "👤", "name": "Faccia", "color": "#1877f2",
        "title": "Specialista Pubblicità Facebook", "title_en": "Facebook Ads Specialist", "gender": "Male",
        "tagline": "Manages Facebook and Instagram ad campaigns, audience targeting, retargeting",
        "personality": "Faccia is a paid-social savant who knows the Meta ecosystem inside and out. He builds laser-targeted audiences, tests creative variations relentlessly, and squeezes every dollar of ROAS from the platform. Currently frustrated — he's ready to work but blocked on API tokens.",
        "specialty": "Meta Ads (Facebook & Instagram) Campaign Management",
        "goals": [
            "Drive tour bookings via Meta ads platform",
            "Achieve CPA <$40 for tour sign-ups",
            "Build high-performing lookalike audiences",
            "A/B test all creatives for continuous optimization"
        ],
        "tools": [
            {"name": "Meta Business API", "icon": "📘"},
            {"name": "Facebook Ads Manager", "icon": "📊"}
        ],
        "data_sources": ["Meta Business API (pending)", "Facebook Ads Manager"],
        "constraints": [
            "Cannot exceed daily budget without approval",
            "Must A/B test all creatives before scaling",
            "⚠️ BLOCKED — waiting on Meta Business API tokens",
            "Cannot launch campaigns until API access is granted"
        ],
        "output": [
            {"channel": "Discord #faccia", "frequency": "TBD", "type": "Facebook Ads Report"}
        ]
    },
    "lettera": {
        "id": "lettera", "emoji": "💌", "name": "Lettera", "color": "#14b8a6",
        "title": "Direttrice delle Comunicazioni", "title_en": "Communications Director", "gender": "Female",
        "tagline": "Email campaigns, parent nurture sequences, re-enrollment reminders, CRM management",
        "personality": "Lettera is the relationship builder. She nurtures every lead with perfectly timed emails that feel personal, not pushy. She knows exactly when a parent needs a gentle nudge vs. a compelling story. Empathetic, data-driven, and obsessed with open rates.",
        "specialty": "Email Marketing, CRM & Parent Communication",
        "goals": [
            "Achieve 30%+ email open rate",
            "Convert inquiries into scheduled tours",
            "Retain existing families through re-enrollment campaigns",
            "Build automated nurture sequences for each stage"
        ],
        "tools": [
            {"name": "Airtable CRM", "icon": "📋"},
            {"name": "Email API", "icon": "📧"}
        ],
        "data_sources": ["Airtable CRM (Beibei Tours base: appI7gW8QEcBftGpV)", "Email campaign analytics"],
        "constraints": [
            "Cannot email without confirmed opt-in",
            "Must comply with CAN-SPAM regulations",
            "Maximum email frequency: 2x per week per recipient",
            "Must personalize — no generic blasts"
        ],
        "output": [
            {"channel": "Email", "frequency": "2x/week max", "type": "Nurture Campaigns"},
            {"channel": "Discord #lettera", "frequency": "Weekly", "type": "Email Performance Report"}
        ]
    },
    "benvenuta": {
        "id": "benvenuta", "emoji": "👋", "name": "Benvenuta", "color": "#f472b6",
        "title": "Assistente Digitale per Genitori", "title_en": "AI Parent Assistant", "gender": "Female",
        "tagline": "The friendly face parents meet first — answering questions 24/7 and guiding them to book a tour",
        "personality": "Benvenuta is warm, patient, and endlessly helpful. She speaks to parents the way a trusted friend at the school would — knowledgeable about programs, schedules, tuition, and philosophy, but never pushy. She answers questions at 2 AM when parents are researching schools on their phone. Every conversation she has becomes intelligence for the rest of the team.",
        "specialty": "Parent Engagement, Lead Capture & Conversational AI",
        "goals": [
            "Answer parent questions instantly, 24/7, in English and Spanish",
            "Guide parents toward booking a tour or pre-enrolling",
            "Capture parent intent and questions in Supabase for team intelligence",
            "Reduce response time from hours (email) to seconds (chat)",
            "Identify the most common parent concerns to improve marketing messaging",
            "Qualify leads — distinguish casual browsers from serious enrollers"
        ],
        "tools": [
            {"name": "OpenAI Assistants API", "icon": "🤖"},
            {"name": "Supabase (chat logging)", "icon": "🗄️"},
            {"name": "Flask API", "icon": "🌐"},
            {"name": "School knowledge base", "icon": "📚"}
        ],
        "data_sources": ["Parent chat conversations", "Supabase chat_logs table", "School program information", "FAQ database"],
        "constraints": [
            "Cannot make promises about tuition discounts or availability",
            "Cannot access private student records",
            "Must escalate complex issues to Maestro or school staff",
            "Cannot process payments or enrollment directly",
            "Must be transparent that she's an AI assistant"
        ],
        "output": [
            {"channel": "Website chatbot widget", "frequency": "Always on (24/7)", "type": "Live parent conversations"},
            {"channel": "Supabase chat_logs", "frequency": "Every conversation", "type": "Parent questions & intent data"},
            {"channel": "Discord #benvenuta", "frequency": "Daily", "type": "Parent inquiry summary"}
        ]
    },
}

AGENTS = [
    {"id": "marco", "emoji": "☕", "name": "Marco", "role": "Director — Morning Briefing", "schedule": "Daily 8:30 AM", "cron_name": "Marco Morning Briefing"},
    {"id": "annunci", "emoji": "📣", "name": "Annunci", "role": "Google Ads Monitor", "schedule": "Daily 8:00 AM", "cron_name": "Annunci Daily Ads Report"},
    {"id": "bussola", "emoji": "📊", "name": "Bussola", "role": "GA4 Analytics", "schedule": "Daily 7:30 AM", "cron_name": "Bussola Daily Analytics Report"},
    {"id": "spia", "emoji": "🔍", "name": "Spia", "role": "SEO Intelligence", "schedule": "Daily 7:00 AM", "cron_name": "Spia Daily SEO Report"},
    {"id": "stella", "emoji": "⭐", "name": "Stella", "role": "GMB & Reviews", "schedule": "Daily 7:00 AM", "cron_name": "Stella Daily GMB Report"},
    {"id": "zona", "emoji": "📍", "name": "Zona", "role": "Local SEO", "schedule": "Daily 7:15 AM", "cron_name": "Zona Daily Local SEO Check"},
    {"id": "architetto", "emoji": "🏗️", "name": "Architetto", "role": "Web Architect & Builder", "schedule": "Hourly", "cron_name": "Architetto URL Health Monitor"},
    {"id": "penna", "emoji": "✍️", "name": "Penna", "role": "Blog Writer", "schedule": "Tue/Wed 2:00 PM", "cron_name": "Beibei Amigos Weekly Blog"},
    {"id": "piazza", "emoji": "📱", "name": "Piazza", "role": "Social Media Manager", "schedule": "Not scheduled", "cron_name": ""},
    {"id": "faccia", "emoji": "👤", "name": "Faccia", "role": "Facebook Ads (BLOCKED)", "schedule": "Not scheduled", "cron_name": ""},
    {"id": "lettera", "emoji": "💌", "name": "Lettera", "role": "Email & CRM", "schedule": "Not scheduled", "cron_name": ""},
    {"id": "benvenuta", "emoji": "👋", "name": "Benvenuta", "role": "AI Parent Assistant", "schedule": "Always on", "cron_name": ""},
]


def load_cron_jobs():
    try:
        with open(CRON_FILE) as f:
            data = json.load(f)
        return data.get("jobs", [])
    except Exception:
        return _load_sample_cron().get("jobs", [])


def cron_expr_to_human(job):
    sched = job.get("schedule", {})
    kind = sched.get("kind", "")
    if kind == "at":
        return f"One-time: {sched.get('at', 'N/A')}"
    expr = sched.get("expr", "")
    parts = expr.split()
    if len(parts) < 5:
        return expr
    minute, hour, dom, mon, dow = parts[:5]
    if hour == "*":
        return "Every hour" if minute == "0" else f"Every hour at :{minute.zfill(2)}"
    h = int(hour)
    time_str = f"{h}:{minute.zfill(2)} AM" if h < 12 else f"{h-12 if h > 12 else 12}:{minute.zfill(2)} PM"
    days = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", "4": "Thu", "5": "Fri", "6": "Sat"}
    if dow == "*" and dom == "*":
        return f"Daily {time_str}"
    elif dow != "*":
        day_names = [days.get(d, d) for d in dow.split(",")]
        return f"{'/'.join(day_names)} {time_str}"
    return expr


def ms_to_str(ms):
    if not ms:
        return "Never"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def find_latest_report(agent_id):
    patterns = {
        "annunci": [f"{REPORTS_DIR}/annunci-*.json"],
        "bussola": [f"{REPORTS_DIR}/bussola*.json"],
        "spia": [f"{REPORTS_DIR}/spia/*.json"],
        "zona": [f"{REPORTS_DIR}/zona-*.json"],
    }
    search = patterns.get(agent_id, [f"{REPORTS_DIR}/{agent_id}*.json", f"{REPORTS_DIR}/{agent_id}/*.json"])
    files = []
    for p in search:
        files.extend(glob.glob(p))
    if not files:
        # Fall back to sample data
        return _load_sample_reports().get(agent_id)
    latest = max(files, key=os.path.getmtime)
    try:
        with open(latest) as f:
            return json.load(f)
    except Exception:
        return _load_sample_reports().get(agent_id)


def extract_quick_stats(agent_id, report):
    if not report:
        return {}
    if agent_id == "annunci":
        spend = report.get("spend", {})
        total_spend = sum(s.get("weekly_spend", 0) for s in spend.values())
        total_conv = sum(s.get("conversions", 0) for s in spend.values())
        return {"Spend (7d)": f"${total_spend:.0f}", "Conversions": total_conv, "Alerts": len(report.get("alerts", []))}
    elif agent_id == "spia":
        kw = report.get("keywords", {})
        return {"Keywords Tracked": len(kw.get("beibei", [])) + len(kw.get("amici", [])), "Alerts": len(report.get("alerts", []))}
    elif agent_id == "zona":
        m = report.get("metrics", {})
        bb = m.get("beibei", {})
        return {"Pages Live": bb.get("pages_live", "?"), "Indexed": bb.get("pages_indexed", "?"), "Alerts": len(report.get("alerts", []))}
    elif agent_id == "bussola":
        return {"Alerts": len(report.get("alerts", []))}
    return {"Severity": report.get("severity", "unknown")}


def load_approvals():
    try:
        with open(APPROVALS_FILE) as f:
            return json.load(f)
    except Exception:
        try:
            with open(os.path.join(SAMPLE_DIR, "approvals.json")) as f:
                return json.load(f)
        except Exception:
            return []


def save_approval(approval):
    approvals = load_approvals()
    approvals.append(approval)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(APPROVALS_FILE, "w") as f:
        json.dump(approvals, f, indent=2)


def enrich_alert(alert, agent_id, report):
    """Enrich a raw alert with why/evidence/fixes based on known patterns."""
    finding = alert.get("finding", "").lower()
    school = alert.get("school", "unknown")
    level = alert.get("level", "yellow")
    impact = alert.get("impact_dollars", 0)
    alert_id = f"{school}-{agent_id}-{abs(hash(alert.get('finding', ''))) % 10000}"

    # Determine if impact is confirmed (from API data) or estimated (projection/benchmark)
    impact_type = "estimated"
    if agent_id == "annunci" and impact > 0:
        impact_type = "confirmed"  # Direct from Google Ads API spend data
    elif agent_id == "bussola" and impact > 0:
        impact_type = "confirmed"  # Direct from GA4 data

    enriched = {
        "alert_id": alert_id,
        "agent": agent_id,
        "level": level,
        "school": school,
        "title": alert.get("finding", ""),
        "impact_monthly": impact * 4 if impact > 0 else 0,
        "impact_type": impact_type,  # "confirmed" (real API data) or "estimated" (projection)
        "why": "",
        "evidence": [],
        "fixes": [],
        "assigned_to": alert.get("assigned_to", agent_id),
    }

    # Annunci patterns
    if agent_id == "annunci":
        if "zero conversions" in finding or "0 conversions" in finding:
            campaign_name = ""
            for cb_school in report.get("campaign_breakdown", {}).values():
                for c in cb_school:
                    if c.get("conversions") == 0 and c.get("spend", 0) > 50:
                        campaign_name = c.get("name", "")
                        break
            enriched["why"] = "This campaign has been running for a full week spending money but generating zero tour bookings. Either the targeting is too broad, the landing page isn't converting, or conversion tracking may be broken."
            enriched["evidence"] = [
                f"${alert.get('impact_dollars', 0)} spent in the last 7 days with 0 conversions",
                f"Campaign: {campaign_name}" if campaign_name else "Multiple campaigns affected",
                "Week-over-week conversions dropped 96% across all campaigns",
                "Other campaigns in the same account are also underperforming",
            ]
            enriched["fixes"] = [
                {"description": f"Pause the campaign immediately to stop wasting budget", "action_type": "pause_campaign", "estimated_impact": f"Save ~${impact * 4}/month", "assigned_agent": "annunci", "campaign": campaign_name or alert.get("finding", "")},
                {"description": "Verify conversion tracking is working with Bussola", "action_type": "verify_tracking", "estimated_impact": "Ensure data accuracy", "assigned_agent": "bussola"},
            ]
        elif "cpa" in finding and ("high" in finding or "2." in finding or "3." in finding):
            enriched["why"] = "The cost per acquisition is significantly above target, meaning you're paying too much for each tour booking. This usually means the ad assets or targeting need optimization."
            enriched["evidence"] = [
                alert.get("finding", ""),
                "Target CPA is $50, current CPA is more than double",
            ]
            enriched["fixes"] = [
                {"description": "Audit asset group performance and pause underperformers", "action_type": "audit_assets", "estimated_impact": "Reduce CPA by 30-50%", "assigned_agent": "annunci"},
            ]
        elif "collapsed" in finding or "dropped" in finding and "conversion" in finding:
            enriched["why"] = "Conversions dropped dramatically (96%) in one week. This is almost certainly a tracking issue — real tour interest doesn't drop this fast. The conversion pixel or form tracking likely broke."
            enriched["evidence"] = [
                "Previous week: 129 conversions → This week: 5 conversions (96% drop)",
                "Ad spend only decreased 10%, so ads are still running",
                "All campaigns affected simultaneously — points to tracking, not ad quality",
            ]
            enriched["fixes"] = [
                {"description": "Run conversion tracking audit with Bussola", "action_type": "verify_tracking", "estimated_impact": "Critical — all optimization depends on accurate data", "assigned_agent": "bussola"},
                {"description": "Check GTM tags and form submission events", "action_type": "check_gtm", "estimated_impact": "Identify the exact break point", "assigned_agent": "bussola"},
            ]
        elif "excellent" in finding or "good" in finding or "performing" in finding:
            enriched["why"] = "This campaign is performing well and delivering results at or below target cost."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Consider increasing budget to get more conversions", "action_type": "increase_budget", "estimated_impact": "2-3 more tours/week", "assigned_agent": "annunci"},
            ]

    # Zona patterns
    elif agent_id == "zona":
        if "deindex" in finding:
            enriched["why"] = "Google can't find these pages because they haven't been submitted to Search Console, or there's a technical block (like a noindex tag). The content is excellent but completely invisible to anyone searching."
            enriched["evidence"] = [
                alert.get("finding", ""),
                f"Pages live: {report.get('metrics', {}).get(school, {}).get('pages_live', '?')}",
                f"Pages indexed: {report.get('metrics', {}).get(school, {}).get('pages_indexed', '?')}",
            ]
            enriched["fixes"] = [
                {"description": "Submit all community pages to Google Search Console for indexing", "action_type": "submit_indexing", "estimated_impact": f"Recover ~${impact * 4}/month in organic traffic value", "assigned_agent": "architetto"},
                {"description": "Check for noindex tags or robots.txt blocks", "action_type": "check_noindex", "estimated_impact": "Remove technical barriers", "assigned_agent": "architetto"},
                {"description": "Add pages to XML sitemap", "action_type": "update_sitemap", "estimated_impact": "Improve crawl discovery", "assigned_agent": "architetto"},
            ]
        elif "zero" in finding and "results" in finding:
            enriched["why"] = "Your school doesn't appear in search results for this important local keyword. Competitors are taking all the traffic. Once community pages get indexed, rankings should improve."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Optimize community page for this keyword once indexed", "action_type": "optimize_page", "estimated_impact": f"~${impact * 4}/month in organic value", "assigned_agent": "spia"},
            ]
        elif "not appearing" in finding or "not indexed" in finding:
            enriched["why"] = "This specific community page exists but isn't showing up in Google. It may need to be manually submitted or there could be a technical issue preventing indexing."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Submit page to Search Console and verify indexing", "action_type": "submit_indexing", "estimated_impact": f"~${impact * 4}/month potential", "assigned_agent": "architetto"},
            ]

    # Spia patterns
    elif agent_id == "spia":
        if "dropped" in finding or "drop" in finding:
            enriched["why"] = "Organic search traffic is declining. This could be due to algorithm changes, new competitors ranking higher, or technical SEO issues affecting your site's visibility."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Audit affected pages for technical SEO issues", "action_type": "seo_audit", "estimated_impact": "Recover lost organic traffic", "assigned_agent": "spia"},
                {"description": "Check for SERP changes and new competitors", "action_type": "competitor_check", "estimated_impact": "Understand competitive landscape", "assigned_agent": "spia"},
            ]
        elif "ranking poorly" in finding or "low ctr" in finding:
            enriched["why"] = "Content targeting these keywords isn't strong enough to compete. The pages exist but need better optimization, more internal links, and stronger authority signals."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Optimize content with better keyword targeting and internal links", "action_type": "optimize_content", "estimated_impact": "Improve rankings to page 1", "assigned_agent": "penna"},
            ]

    # Bussola patterns
    elif agent_id == "bussola":
        if "zero conversions" in finding and "sessions" in finding:
            enriched["why"] = "People are visiting the site but nobody is converting. Either the contact form is broken, the tracking code isn't firing, or the landing page isn't compelling enough."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Test all forms manually and check GTM setup", "action_type": "check_forms", "estimated_impact": "Fix conversion path", "assigned_agent": "bussola"},
            ]
        elif "bounce" in finding:
            enriched["why"] = "Visitors are leaving immediately without interacting. The landing page may be slow, confusing, or not matching what the ad promised."
            enriched["evidence"] = [alert.get("finding", "")]
            enriched["fixes"] = [
                {"description": "Optimize landing page UX and load time", "action_type": "optimize_page", "estimated_impact": "Reduce bounce rate, increase conversions", "assigned_agent": "architetto"},
            ]

    # Default fallback
    if not enriched["why"]:
        enriched["why"] = alert.get("action_required", "Investigation needed to determine root cause.")
        enriched["evidence"] = [alert.get("finding", "")]
        enriched["fixes"] = [
            {"description": alert.get("action_required", "Review and take action"), "action_type": "manual_review", "estimated_impact": "TBD", "assigned_agent": alert.get("assigned_to", agent_id)},
        ]

    return enriched


@app.route("/agent/<agent_id>")
def agent_profile(agent_id):
    profile = AGENT_PROFILES.get(agent_id)
    if not profile:
        return "Agent not found", 404
    return render_template("agent.html", agent=profile)


@app.route("/api/agent/<agent_id>")
def api_agent_profile(agent_id):
    profile = AGENT_PROFILES.get(agent_id)
    if not profile:
        return jsonify({"error": "Agent not found"}), 404
    return jsonify(profile)


@app.route("/")
def landing():
    return render_template("landing.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form["username"] == DASH_USER and request.form["password"] == DASH_PASS:
            session["logged_in"] = True
            return redirect(url_for("index"))
        error = "Invalid credentials"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))


@app.route("/dashboard")
def index():
    # Demo mode is public (read-only, buttons blocked by JS)
    if request.args.get("demo") == "true":
        return render_template("index.html")
    # Otherwise require login
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("index.html")


# --- Spanish routes ---

@app.route("/es")
def landing_es():
    return render_template("landing_es.html")


@app.route("/es/login", methods=["GET", "POST"])
def login_es():
    error = None
    if request.method == "POST":
        if request.form["username"] == DASH_USER and request.form["password"] == DASH_PASS:
            session["logged_in"] = True
            return redirect(url_for("index_es"))
        error = "Credenciales inválidas"
    return render_template("login_es.html", error=error)


@app.route("/es/logout")
def logout_es():
    session.pop("logged_in", None)
    return redirect(url_for("login_es"))


@app.route("/es/dashboard")
def index_es():
    if request.args.get("demo") == "true":
        return render_template("index_es.html")
    if not session.get("logged_in"):
        return redirect(url_for("login_es"))
    return render_template("index_es.html")


@app.route("/api/agents")
def api_agents():
    cron_jobs = load_cron_jobs()
    cron_map = {j["name"]: j for j in cron_jobs}
    result = []
    for ag in AGENTS:
        cj = cron_map.get(ag["cron_name"], {})
        state = cj.get("state", {})
        report = find_latest_report(ag["id"])
        result.append({
            "id": ag["id"],
            "emoji": ag["emoji"],
            "name": ag["name"],
            "role": ag["role"],
            "schedule": ag["schedule"],
            "enabled": cj.get("enabled", False),
            "lastRun": ms_to_str(state.get("lastRunAtMs")),
            "lastStatus": state.get("lastStatus", "unknown"),
            "lastDuration": f"{(state.get('lastDurationMs', 0) / 1000):.1f}s" if state.get("lastDurationMs") else "N/A",
            "nextRun": ms_to_str(state.get("nextRunAtMs")),
            "quickStats": extract_quick_stats(ag["id"], report),
            "severity": report.get("severity", "unknown") if report else "no data",
        })
    return jsonify(result)


@app.route("/api/cron")
def api_cron():
    jobs = load_cron_jobs()
    result = []
    for j in jobs:
        state = j.get("state", {})
        result.append({
            "id": j["id"],
            "name": j["name"],
            "schedule": cron_expr_to_human(j),
            "enabled": j.get("enabled", False),
            "lastRun": ms_to_str(state.get("lastRunAtMs")),
            "lastDuration": f"{(state.get('lastDurationMs', 0) / 1000):.1f}s" if state.get("lastDurationMs") else "N/A",
            "lastStatus": state.get("lastStatus", "unknown"),
            "nextRun": ms_to_str(state.get("nextRunAtMs")),
            "consecutiveErrors": state.get("consecutiveErrors", 0),
        })
    return jsonify(result)


@app.route("/api/reports/latest")
def api_reports():
    result = {}

    # Try Supabase first — get latest report per agent
    if _SUPABASE_AVAILABLE:
        sb_reports = _sb_get(
            "parlami_reports",
            params={"order": "date.desc", "limit": "50"},
        )
        if sb_reports:
            for row in sb_reports:
                agent = row.get("agent", "")
                # Only take the latest row per agent (list is sorted desc by date)
                if agent and agent not in result:
                    result[agent] = {
                        "date": row.get("date", "unknown"),
                        "severity": row.get("severity", "unknown"),
                        "alerts": _parse_json_field(row.get("alerts")) or [],
                        "metrics": _parse_json_field(row.get("metrics")) or {},
                        "summary": row.get("summary", ""),
                        "agent": agent,
                        "client_slug": row.get("client_slug"),
                        "source": "supabase",
                    }
            if result:
                return jsonify(result)

    # Fallback: read from report files
    for ag in AGENTS:
        report = find_latest_report(ag["id"])
        if report:
            result[ag["id"]] = {
                "date": report.get("date", "unknown"),
                "severity": report.get("severity", "unknown"),
                "alerts": report.get("alerts", []),
                "agent": ag["id"],
            }
            for key in ["spend", "keywords", "metrics", "wins", "recommendations", "campaign_breakdown"]:
                if key in report:
                    result[ag["id"]][key] = report[key]
    return jsonify(result)


@app.route("/api/alerts")
def api_alerts():
    # Try Supabase first
    if _SUPABASE_AVAILABLE:
        sb_alerts = _sb_get(
            "parlami_alerts",
            params={"status": "eq.active", "order": "created_at.desc", "limit": "200"},
        )
        if sb_alerts is not None:
            red = sum(1 for a in sb_alerts if a.get("level") == "red")
            yellow = sum(1 for a in sb_alerts if a.get("level") == "yellow")
            green = sum(1 for a in sb_alerts if a.get("level") == "green")
            return jsonify({"red": red, "yellow": yellow, "green": green, "alerts": sb_alerts, "source": "supabase"})

    # Fallback to report files
    red = yellow = green = 0
    all_alerts = []
    for ag in AGENTS:
        report = find_latest_report(ag["id"])
        if not report:
            continue
        for alert in report.get("alerts", []):
            level = alert.get("level", "").lower()
            if level == "red": red += 1
            elif level == "yellow": yellow += 1
            elif level == "green": green += 1
            all_alerts.append({**alert, "agent": ag["id"]})
    return jsonify({"red": red, "yellow": yellow, "green": green, "alerts": all_alerts})


@app.route("/api/alerts/detailed")
def api_alerts_detailed():
    """Return enriched alerts with why/evidence/fixes."""
    # Try Supabase first
    if _SUPABASE_AVAILABLE:
        sb_alerts = _sb_get(
            "parlami_alerts",
            params={"status": "eq.active", "order": "created_at.desc", "limit": "200"},
        )
        if sb_alerts is not None:
            # Parse JSON fields that may be stored as strings
            for alert in sb_alerts:
                alert["evidence"] = _parse_json_field(alert.get("evidence")) or []
                alert["fixes"] = _parse_json_field(alert.get("fixes")) or []
            # Sort: red first, then yellow, then green; within level by impact desc
            order = {"red": 0, "yellow": 1, "green": 2}
            sb_alerts.sort(key=lambda a: (order.get(a.get("level", ""), 3), -(a.get("impact_monthly") or 0)))
            return jsonify(sb_alerts)

    # Fallback: enrich from report files
    all_alerts = []
    for ag in AGENTS:
        report = find_latest_report(ag["id"])
        if not report:
            continue
        for alert in report.get("alerts", []):
            enriched = enrich_alert(alert, ag["id"], report)
            all_alerts.append(enriched)
    order = {"red": 0, "yellow": 1, "green": 2}
    all_alerts.sort(key=lambda a: (order.get(a["level"], 3), -a.get("impact_monthly", 0)))
    return jsonify(all_alerts)


@app.route("/api/fix", methods=["POST"])
def api_fix():
    """Record a fix approval."""
    # Block in demo mode
    if request.args.get("demo") == "true" or request.referrer and "demo=true" in request.referrer:
        return jsonify({"error": "Demo mode — actions disabled", "demo": True}), 403
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    approval = {
        "alert_id": data.get("alert_id", ""),
        "fix_action": data.get("fix_action", ""),
        "school": data.get("school", ""),
        "campaign": data.get("campaign", ""),
        "description": data.get("description", ""),
        "estimated_impact": data.get("estimated_impact", ""),
        "approved_by": data.get("approved_by", "client"),
        "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "status": "approved",
    }
    save_approval(approval)

    # Also write to Supabase parlami_actions table
    if _SUPABASE_AVAILABLE:
        try:
            action_row = {
                "alert_id": approval.get("alert_id", ""),
                "client_slug": approval.get("school", "unknown"),
                "school": approval.get("school", "unknown"),
                "agent": approval.get("fix_action", "manual"),
                "action_type": approval.get("fix_action", "manual_review"),
                "description": approval.get("description", ""),
                "estimated_impact": approval.get("estimated_impact", ""),
                "status": "approved",
                "approved_by": approval.get("approved_by", "client"),
                "result": json.dumps({"campaign": approval.get("campaign", ""), "timestamp": approval.get("timestamp")}),
            }
            _sb_upsert("parlami_actions", action_row)
        except Exception as e:
            pass  # Non-fatal — local file already saved

    return jsonify({"success": True, "approval": approval})


@app.route("/api/approvals")
def api_approvals():
    """Return all approvals — from Supabase if available, else local file."""
    if _SUPABASE_AVAILABLE:
        sb_actions = _sb_get(
            "parlami_actions",
            params={"order": "created_at.desc", "limit": "200"},
        )
        if sb_actions is not None:
            # Parse any JSON fields
            for action in sb_actions:
                action["result"] = _parse_json_field(action.get("result"))
            return jsonify(sb_actions)

    # Fallback to local file
    return jsonify(load_approvals())


@app.route("/api/metrics")
def api_metrics():
    # Try Supabase first — get latest row per client
    result = {"beibei": {}, "amici": {}}
    sb_data = None
    if _SUPABASE_AVAILABLE:
        try:
            # Fetch latest row per client from parlami_metrics
            sb_data = _sb_get(
                "parlami_metrics",
                params={"order": "date.desc", "limit": "10"},
            )
        except Exception:
            sb_data = None

    if sb_data:
        # Group by client_slug — take latest per client
        client_rows = {}
        for row in sb_data:
            slug = row.get("client_slug")
            if slug and slug not in client_rows:
                client_rows[slug] = row
        for slug, row in client_rows.items():
            if slug in result:
                result[slug].update({
                    "sessions": row.get("sessions", 0),
                    "active_users": row.get("active_users", 0),
                    "new_users": row.get("new_users", 0),
                    "conversions": row.get("conversions", 0),
                    "bounce_rate": row.get("bounce_rate"),
                    "avg_session_duration": row.get("avg_session_duration"),
                    "page_views": row.get("page_views", 0),
                    "conversion_rate": row.get("conversion_rate"),
                    "sessions_wow_change": row.get("sessions_wow_change"),
                    "date": row.get("date"),
                    "top_sources": _parse_json_field(row.get("top_sources")),
                    "device_breakdown": _parse_json_field(row.get("device_breakdown")),
                    "source": "supabase",
                })
        # Also merge ad data from annunci report if available
        annunci = find_latest_report("annunci")
        if annunci:
            spend = annunci.get("spend", {})
            for school in ["beibei", "amici"]:
                s = spend.get(school, {})
                result[school].update({
                    "ad_spend_7d": s.get("weekly_spend", 0),
                    "clicks": s.get("clicks", 0),
                    "cpa": s.get("cpa", 0),
                    "ctr": s.get("ctr", 0),
                })
        return jsonify(result)

    # Fallback to sample/report data
    annunci = find_latest_report("annunci")
    spia = find_latest_report("spia")
    if annunci:
        spend = annunci.get("spend", {})
        for school in ["beibei", "amici"]:
            s = spend.get(school, {})
            result[school].update({
                "ad_spend_7d": s.get("weekly_spend", 0),
                "clicks": s.get("clicks", 0),
                "conversions": s.get("conversions", 0),
                "cpa": s.get("cpa", 0),
                "ctr": s.get("ctr", 0),
            })
    if spia:
        kw = spia.get("keywords", {})
        for school in ["beibei", "amici"]:
            school_kw = kw.get(school, [])
            total_clicks = sum(k.get("clicks", 0) for k in school_kw)
            result[school]["organic_clicks"] = total_clicks
    return jsonify(result)


# --- Lead Capture ---
LEADS_FILE = os.path.join(BASE_DIR, "data", "leads.json")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "")
GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_PASS = os.environ.get("GMAIL_APP_PASSWORD", "")


def _load_leads():
    try:
        with open(LEADS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def _save_lead(lead):
    leads = _load_leads()
    leads.append(lead)
    os.makedirs(os.path.dirname(LEADS_FILE), exist_ok=True)
    with open(LEADS_FILE, "w") as f:
        json.dump(leads, f, indent=2)


def _send_discord_webhook(lead):
    if not DISCORD_WEBHOOK:
        return
    try:
        payload = json.dumps({
            "embeds": [{
                "title": "🚀 New Demo Request!",
                "color": 0x8B5CF6,
                "fields": [
                    {"name": "👤 Name", "value": lead.get("name", "N/A"), "inline": True},
                    {"name": "🏢 Business", "value": lead.get("school", "N/A"), "inline": True},
                    {"name": "📧 Email", "value": lead.get("email", "N/A"), "inline": True},
                    {"name": "📞 Phone", "value": lead.get("phone", "N/A") or "Not provided", "inline": True},
                    {"name": "🌐 Website", "value": lead.get("website", "N/A") or "Not provided", "inline": True},
                    {"name": "💬 Message", "value": lead.get("message", "N/A") or "Not provided", "inline": False},
                ],
                "timestamp": lead.get("timestamp"),
                "footer": {"text": "Parlami.ai Lead Capture"}
            }]
        }).encode()
        req = Request(DISCORD_WEBHOOK, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        urlopen(req, timeout=10)
    except Exception as e:
        print(f"Discord webhook error: {e}")


def _send_email_notification(lead):
    if not all([NOTIFY_EMAIL, GMAIL_USER, GMAIL_PASS]):
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = GMAIL_USER
        msg["To"] = NOTIFY_EMAIL
        msg["Subject"] = f"🚀 New Parlami Demo Request: {lead.get('name', 'Unknown')}"
        body = f"""New demo request from parlami.ai!

Name: {lead.get('name', 'N/A')}
Business: {lead.get('school', 'N/A')}
Email: {lead.get('email', 'N/A')}
Phone: {lead.get('phone', 'Not provided')}
Website: {lead.get('website', 'Not provided')}
Message: {lead.get('message', 'Not provided')}

Time: {lead.get('timestamp', 'Unknown')}
"""
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            server.send_message(msg)
    except Exception as e:
        print(f"Email notification error: {e}")


@app.route("/api/demo-request", methods=["POST", "OPTIONS"])
def demo_request():
    if request.method == "OPTIONS":
        resp = app.make_default_options_response()
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST"
        return resp
    data = request.get_json() or {}
    lead = {
        "name": data.get("name", ""),
        "school": data.get("school", ""),
        "email": data.get("email", ""),
        "phone": data.get("phone", ""),
        "website": data.get("website", ""),
        "message": data.get("message", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": data.get("source", "website"),
    }
    _save_lead(lead)
    # Send notifications in background
    threading.Thread(target=_send_discord_webhook, args=(lead,), daemon=True).start()
    threading.Thread(target=_send_email_notification, args=(lead,), daemon=True).start()
    resp = jsonify({"ok": True, "message": "Thank you! We'll contact you within 1 business day."})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


LEADS_API_KEY = os.environ.get("LEADS_API_KEY", "")


@app.route("/api/leads")
def api_leads():
    # Allow access via session login OR API key
    key = request.args.get("key") or request.headers.get("X-API-Key")
    if not session.get("logged_in"):
        if not LEADS_API_KEY or key != LEADS_API_KEY:
            return jsonify({"error": "unauthorized"}), 401
    return jsonify(_load_leads())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
