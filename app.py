#!/usr/bin/env python3
"""Parlami V2 Dashboard — Real data from GA4, Google Ads, GSC for both schools."""

import os
import json
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, send_from_directory
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
)
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__, static_folder="static", template_folder="templates")

# === CONFIG ===
SCHOOLS = {
    "amici": {
        "name": "Amici Montessori",
        "ga4_property": "375416137",
        "gsc_site": "sc-domain:amicimontessori.com",
        "ads_campaign_prefix": "AMICI",
        "url": "https://amicimontessori.com",
        "plan": "Pro Plan · $1,497/mo",
        "color": "#009688"
    },
    "beibei": {
        "name": "Beibei Amigos",
        "ga4_property": "390456366",
        "gsc_site": "sc-domain:beibeiamigos.com",
        "ads_campaign_prefix": "BEIBEI",
        "url": "https://www.beibeiamigos.com",
        "plan": "Pro Plan · $1,497/mo",
        "color": "#e91e63"
    }
}

SA_KEY = os.path.expanduser("~/.openclaw/secrets/google-service-account.json")
ADS_ENV = os.path.expanduser("~/Projects/parlami_ai/.env")

# === HELPERS ===
def load_ads_env():
    """Load Google Ads credentials from .env file."""
    creds = {}
    with open(ADS_ENV) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                creds[k.strip()] = v.strip()
    return creds

def get_ga4_client():
    credentials = service_account.Credentials.from_service_account_file(
        SA_KEY, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)

def get_gsc_service():
    credentials = service_account.Credentials.from_service_account_file(
        SA_KEY, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    return build('searchconsole', 'v1', credentials=credentials)

def get_ads_client():
    env = load_ads_env()
    config = {
        "developer_token": env.get("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": env.get("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": env.get("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": env.get("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True
    }
    return GoogleAdsClient.load_from_dict(config)

# === GA4 ===
def fetch_ga4_overview(school_id, days=7):
    """Fetch website visitors, sessions, conversions from GA4."""
    client = get_ga4_client()
    school = SCHOOLS[school_id]
    
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    prev_start = (datetime.now() - timedelta(days=days*2)).strftime("%Y-%m-%d")
    prev_end = (datetime.now() - timedelta(days=days+1)).strftime("%Y-%m-%d")
    
    # Current period
    request = RunReportRequest(
        property=f"properties/{school['ga4_property']}",
        date_ranges=[DateRange(start_date=start, end_date=today)],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="sessions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="conversions"),
            Metric(name="screenPageViews"),
        ]
    )
    response = client.run_report(request)
    
    current = {}
    if response.rows:
        row = response.rows[0]
        current = {
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
            "bounceRate": round(float(row.metric_values[2].value) * 100, 1),
            "avgDuration": round(float(row.metric_values[3].value), 0),
            "conversions": int(float(row.metric_values[4].value)),
            "pageViews": int(row.metric_values[5].value),
        }
    
    # Previous period
    request_prev = RunReportRequest(
        property=f"properties/{school['ga4_property']}",
        date_ranges=[DateRange(start_date=prev_start, end_date=prev_end)],
        metrics=[Metric(name="totalUsers"), Metric(name="sessions"), Metric(name="conversions")]
    )
    response_prev = client.run_report(request_prev)
    
    previous = {}
    if response_prev.rows:
        row = response_prev.rows[0]
        previous = {
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
            "conversions": int(float(row.metric_values[2].value)),
        }
    
    # Calculate changes
    def pct_change(curr, prev):
        if prev == 0: return 0
        return round(((curr - prev) / prev) * 100, 1)
    
    current["usersChange"] = pct_change(current.get("users", 0), previous.get("users", 0))
    current["sessionsChange"] = pct_change(current.get("sessions", 0), previous.get("sessions", 0))
    current["conversionsChange"] = current.get("conversions", 0) - previous.get("conversions", 0)
    
    return current

def fetch_ga4_traffic_by_source(school_id, days=7):
    """Fetch traffic breakdown by source/medium."""
    client = get_ga4_client()
    school = SCHOOLS[school_id]
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    request = RunReportRequest(
        property=f"properties/{school['ga4_property']}",
        date_ranges=[DateRange(start_date=start, end_date=today)],
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
        metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
        limit=10
    )
    response = client.run_report(request)
    
    sources = []
    for row in response.rows:
        sources.append({
            "channel": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })
    return sorted(sources, key=lambda x: x["sessions"], reverse=True)

def fetch_ga4_daily_traffic(school_id, days=30):
    """Fetch daily session counts for chart."""
    client = get_ga4_client()
    school = SCHOOLS[school_id]
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    request = RunReportRequest(
        property=f"properties/{school['ga4_property']}",
        date_ranges=[DateRange(start_date=start, end_date=today)],
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
    )
    response = client.run_report(request)
    
    daily = []
    for row in response.rows:
        daily.append({
            "date": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })
    return sorted(daily, key=lambda x: x["date"])

# === GOOGLE ADS ===
def fetch_ads_overview(school_id, days=7):
    """Fetch ad spend, clicks, conversions, CPA from Google Ads."""
    client = get_ads_client()
    env = load_ads_env()
    customer_id = env.get("GOOGLE_ADS_CUSTOMER_ID", "8882805182")
    school = SCHOOLS[school_id]
    
    ga_service = client.get_service("GoogleAdsService")
    
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    query = f"""
        SELECT
            campaign.name,
            campaign.status,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.ctr
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{today}'
            AND campaign.name LIKE '%{school['ads_campaign_prefix']}%'
    """
    
    results = ga_service.search(customer_id=customer_id, query=query)
    
    campaigns = []
    totals = {"impressions": 0, "clicks": 0, "cost": 0, "conversions": 0, "ctr": 0}
    count = 0
    
    for row in results:
        cost = row.metrics.cost_micros / 1_000_000
        campaigns.append({
            "name": row.campaign.name,
            "status": row.campaign.status.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "cost": round(cost, 2),
            "conversions": round(row.metrics.conversions, 1),
            "ctr": round(row.metrics.ctr * 100, 2) if row.metrics.ctr else 0,
        })
        totals["impressions"] += row.metrics.impressions
        totals["clicks"] += row.metrics.clicks
        totals["cost"] += cost
        totals["conversions"] += row.metrics.conversions
        count += 1
    
    totals["cost"] = round(totals["cost"], 2)
    totals["conversions"] = round(totals["conversions"], 1)
    if totals["clicks"] > 0:
        totals["ctr"] = round((totals["clicks"] / totals["impressions"]) * 100, 2) if totals["impressions"] > 0 else 0
    if totals["conversions"] > 0:
        totals["cpa"] = round(totals["cost"] / totals["conversions"], 2)
    else:
        totals["cpa"] = 0
    
    return {"campaigns": campaigns, "totals": totals}

# === GOOGLE SEARCH CONSOLE ===
def fetch_gsc_overview(school_id, days=7):
    """Fetch top keywords and positions from GSC."""
    service = get_gsc_service()
    school = SCHOOLS[school_id]
    
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    response = service.searchanalytics().query(
        siteUrl=school["gsc_site"],
        body={
            "startDate": start,
            "endDate": today,
            "dimensions": ["query"],
            "rowLimit": 25,
            "orderBy": [{"fieldName": "clicks", "sortOrder": "DESCENDING"}]
        }
    ).execute()
    
    keywords = []
    for row in response.get("rows", []):
        keywords.append({
            "keyword": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": round(row["ctr"] * 100, 2),
            "position": round(row["position"], 1),
        })
    
    # Also get totals
    totals_response = service.searchanalytics().query(
        siteUrl=school["gsc_site"],
        body={
            "startDate": start,
            "endDate": today,
        }
    ).execute()
    
    totals = {
        "clicks": totals_response.get("rows", [{}])[0].get("clicks", 0) if totals_response.get("rows") else 0,
        "impressions": totals_response.get("rows", [{}])[0].get("impressions", 0) if totals_response.get("rows") else 0,
    }
    
    # Count page 1 keywords
    page1_count = sum(1 for k in keywords if k["position"] <= 10)
    
    return {"keywords": keywords, "totals": totals, "page1Count": page1_count}

def fetch_gsc_pages(school_id, days=7):
    """Fetch top performing pages."""
    service = get_gsc_service()
    school = SCHOOLS[school_id]
    today = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    response = service.searchanalytics().query(
        siteUrl=school["gsc_site"],
        body={
            "startDate": start,
            "endDate": today,
            "dimensions": ["page"],
            "rowLimit": 10,
            "orderBy": [{"fieldName": "clicks", "sortOrder": "DESCENDING"}]
        }
    ).execute()
    
    pages = []
    for row in response.get("rows", []):
        pages.append({
            "page": row["keys"][0].replace(school["url"], ""),
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "position": round(row["position"], 1),
        })
    return pages

# === API ROUTES ===
@app.route("/")
def index():
    return send_from_directory("templates", "index_v2.html")

@app.route("/api/overview/<school_id>")
def api_overview(school_id):
    """Full dashboard overview — all data sources."""
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 404
    
    data = {"school": SCHOOLS[school_id]}
    errors = {}
    
    try:
        data["ga4"] = fetch_ga4_overview(school_id)
    except Exception as e:
        errors["ga4"] = str(e)
        data["ga4"] = None
    
    try:
        data["ga4_sources"] = fetch_ga4_traffic_by_source(school_id)
    except Exception as e:
        errors["ga4_sources"] = str(e)
        data["ga4_sources"] = None
    
    try:
        data["ads"] = fetch_ads_overview(school_id)
    except Exception as e:
        errors["ads"] = str(e)
        data["ads"] = None
    
    try:
        data["gsc"] = fetch_gsc_overview(school_id)
    except Exception as e:
        errors["gsc"] = str(e)
        data["gsc"] = None
    
    if errors:
        data["errors"] = errors
    
    return jsonify(data)

@app.route("/api/ga4/<school_id>/daily")
def api_ga4_daily(school_id):
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 404
    try:
        return jsonify(fetch_ga4_daily_traffic(school_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ads/<school_id>")
def api_ads(school_id):
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 404
    try:
        return jsonify(fetch_ads_overview(school_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/gsc/<school_id>/keywords")
def api_gsc_keywords(school_id):
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 404
    try:
        return jsonify(fetch_gsc_overview(school_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/gsc/<school_id>/pages")
def api_gsc_pages(school_id):
    if school_id not in SCHOOLS:
        return jsonify({"error": "Unknown school"}), 404
    try:
        return jsonify(fetch_gsc_pages(school_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/schools")
def api_schools():
    return jsonify(SCHOOLS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5055, debug=True)
