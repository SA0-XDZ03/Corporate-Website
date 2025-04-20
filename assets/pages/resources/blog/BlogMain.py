import sqlite3
import feedparser
import re
import time
import schedule
from threading import Thread, Lock
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from termcolor import colored
from textblob import TextBlob
import spacy
import json

# Load Spacy model for NLP tasks
nlp = spacy.load("en_core_web_sm")

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# Flask-Limiter for rate limiting
limiter = Limiter(get_remote_address, app=app, default_limits=["200 per day", "50 per hour"])

# RSS Feed file
RSS_FEED_FILE = "RSSFeeds.txt"

# Thread lock for serialized database access
db_lock = Lock()

# Initialize the database (Re-create on each run)
def init_db():
    with db_lock:
        conn = sqlite3.connect('blog.db', timeout=10)
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS blog_posts')  # Drop table if exists
        cursor.execute('''
            CREATE TABLE blog_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                author TEXT,
                categories TEXT,
                sentiment TEXT,
                sector TEXT,
                keywords TEXT,
                published_at DATETIME NOT NULL UNIQUE,
                link TEXT NOT NULL
            )
        ''')
        conn.commit()
        conn.close()
        print(colored("Database re-created successfully.", "green"))

# Read RSS feed URLs from a file
def read_rss_feed_urls(file_path):
    try:
        with open(file_path, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(colored(f"Error: RSS feed file '{file_path}' not found.", "red"))
        return []

# Perform sentiment analysis
def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity < 0:
        return "Negative"
    else:
        return "Neutral"

# Identify sector from text
def identify_sector(text):
    sectors = {
        "Defense": ["military", "army", "navy", "defense"],
        "Government": ["policy", "election", "government", "minister"],
        "Sports": ["football", "cricket", "Olympics", "sports"],
        "Crime": ["crime", "murder", "theft", "fraud"],
        "Entertainment": ["movie", "film", "actor", "celebrity"],
        "Financial": ["stocks", "market", "finance", "investment"],
        "Energy": ["oil", "gas", "energy", "renewable"],
        "Technology": ["cybersecurity", "microsoft", "network", "data science", "machine learning","information technology", "technology", "artificial intelligence"]
    }
    text_lower = text.lower()
    for sector, keywords in sectors.items():
        if any(keyword in text_lower for keyword in keywords):
            return sector
    return "General"

# Extract keywords, places, and names using NLP
def extract_keywords(text):
    doc = nlp(text)
    entities = {
        "countries": [],
        "places": [],
        "names": [],
        "other_keywords": []
    }
    for ent in doc.ents:
        if ent.label_ in ["GPE", "LOC"]:
            entities["places"].append(ent.text)
        elif ent.label_ == "PERSON":
            entities["names"].append(ent.text)
        elif ent.label_ in ["ORG", "NORP"]:
            entities["countries"].append(ent.text)
        else:
            entities["other_keywords"].append(ent.text)
    return entities

# Extract RSS feed entry details
# Extract RSS feed entry details
def extract_feed_entry(entry):
    title = re.sub(r'[^\x00-\x7F]+', '', entry.get('title', 'No Title'))  # Remove non-ASCII characters
    description = re.sub(r'[^\x00-\x7F]+', '', entry.get('description', 'No Description'))
    author = getattr(entry, 'author', 'Unknown Author')
    categories = ', '.join(tag.term for tag in getattr(entry, 'tags', [])) if 'tags' in entry else "Uncategorized"
    published_at = entry.get('published', 'Unknown Date')
    link = entry.get('link', 'No Link')

    # Analyze sentiment
    sentiment = analyze_sentiment(title + " " + description)

    # Identify sector
    sector = identify_sector(title + " " + description)

    # Extract keywords
    keywords = extract_keywords(title + " " + description)

    # Ensure keywords are JSON serializable
    keywords_str = json.dumps({
        "countries": keywords["countries"],
        "places": keywords["places"],
        "names": keywords["names"],
        "other_keywords": keywords["other_keywords"],
    })

    return title, description, author, categories, sentiment, sector, keywords_str, published_at, link


# Fetch and update blog posts from RSS feeds
def fetch_blog_posts():
    rss_feed_urls = read_rss_feed_urls(RSS_FEED_FILE)
    if not rss_feed_urls:
        print(colored("No RSS feed URLs found. Please update RSSFeed.txt.", "red"))
        return

    try:
        print(colored("\nStarting RSS feed fetch...", "yellow"))
        total_new_posts = 0

        for feed_url in rss_feed_urls:
            print(colored(f"Fetching feed: {feed_url}", "cyan"))
            feed = feedparser.parse(feed_url)

            if feed.bozo:
                print(colored(f"Error parsing RSS feed: {feed.bozo_exception} for URL: {feed_url}", "red"))
                continue

            new_posts = 0
            for entry in feed.entries:
                title, description, author, categories, sentiment, sector, keywords, published_at, link = extract_feed_entry(entry)

                with db_lock:
                    conn = sqlite3.connect('blog.db', timeout=10)
                    cursor = conn.cursor()
                    try:
                        # Insert or update the record
                        cursor.execute('''
                            INSERT INTO blog_posts (title, description, author, categories, sentiment, sector, keywords, published_at, link)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(published_at) DO UPDATE SET
                                title = excluded.title,
                                description = excluded.description,
                                author = excluded.author,
                                categories = excluded.categories,
                                sentiment = excluded.sentiment,
                                sector = excluded.sector,
                                keywords = excluded.keywords,
                                link = excluded.link
                        ''', (title, description, author, categories, sentiment, sector, keywords, published_at, link))

                        if cursor.rowcount > 0:
                            new_posts += 1
                            print(colored(f"Updated or added post: {title}", "green"))
                        conn.commit()
                    except sqlite3.OperationalError as e:
                        print(colored(f"Database operation error: {e}", "red"))
                    finally:
                        conn.close()

            print(colored(f"Completed fetching feed: {feed_url}. New or updated posts: {new_posts}", "blue"))
            total_new_posts += new_posts

        print(colored(f"RSS feed fetch complete. Total new or updated posts: {total_new_posts}", "blue"))

    except Exception as e:
        print(colored(f"Error fetching RSS feeds: {e}", "red"))

# Schedule updates: First fetch immediately, then every 5 minutes
def schedule_updates():
    print(colored("Starting initial RSS feed fetch...", "cyan"))
    fetch_blog_posts()  # Initial fetch on execution
    schedule.every(5).minutes.do(fetch_blog_posts)  # Schedule every 5 minutes
    print(colored("Scheduled RSS feed updates every 5 minutes.", "cyan"))
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

# Search endpoint
@app.route('/search', methods=['GET'])
@limiter.limit("10 per minute")
def search():
    query = request.args.get('query', '').strip()
    if not query or len(query) > 100:  # Validate query length
        return jsonify({"error": "Invalid search query"}), 400

    query = re.sub(r'[^\w\s]', '', query)  # Sanitize input
    with db_lock:
        conn = sqlite3.connect('blog.db', timeout=10)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, description, author, categories, sentiment, sector, keywords, published_at, link
            FROM blog_posts
            WHERE title LIKE ? OR description LIKE ? OR categories LIKE ?
        ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
        results = cursor.fetchall()
        conn.close()

    print(colored(f"Search performed for query: {query}", "cyan"))
    return jsonify(results)

if __name__ == '__main__':
    print(colored("Initializing application...", "blue"))
    init_db()
    Thread(target=schedule_updates, daemon=True).start()
    print(colored("Starting Flask server...", "blue"))
    app.run(debug=True, host='0.0.0.0', port=5000)