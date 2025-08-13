# main.py - Simple cron service for sports events
from flask import Flask, request, jsonify
from openai import OpenAI
import psycopg2
import json
import os
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration - hardcoded for simplicity
REGIONS = [
    "Juan-Les-Pins, Antibes, South of France",
    "San Francisco Bay Area, CA",
    "New York City, NY"
]

def get_db_connection():
    """Connect to PostgreSQL"""
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )

def create_search_prompt(region):
    """Create search prompt for LLM"""
    today = datetime.now()
    next_month = today + timedelta(days=30)
    
    return f"""Find current sports events happening in {region} from {today.strftime('%Y-%m-%d')} to {next_month.strftime('%Y-%m-%d')}.

Please search for and provide information about upcoming sports events including:
- Professional sports games 
- Local tournaments
- Major sporting events
- Tennis tournaments
- Soccer matches
- Basketball games

For each event you find, provide the information in this EXACT JSON format:

{{
  "events": [
    {{
      "event_name": "Event name here",
      "sport_type": "Tennis/Soccer/Basketball/etc",
      "description": "Brief description of the event",
      "event_location": "Venue name",
      "event_address": "Full address if available, or venue + city",
      "event_startdatetime": "YYYY-MM-DD HH:MM:SS",
      "event_enddatetime": "YYYY-MM-DD HH:MM:SS or null if unknown",
      "link": "Official website or ticket link if available"
    }}
  ]
}}

Important: Only include real, confirmed events. If no events found, return empty events array."""

def scrape_region_events(region):
    """Get events for a specific region using OpenAI"""
    try:
        client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
        prompt = create_search_prompt(region)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a sports event researcher. Provide accurate, real sports event information in the requested JSON format."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000,
            temperature=0.1
        )
        
        response_text = response.choices[0].message.content.strip()
        logger.info(f"OpenAI response for {region}: {response_text[:200]}...")
        
        events_data = json.loads(response_text)
        return events_data.get('events', [])
        
    except Exception as e:
        logger.error(f"Error scraping {region}: {e}")
        return []

def save_events_to_db(events, region):
    """Save events to database"""
    if not events:
        logger.info(f"No events to save for {region}")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    saved_count = 0
    
    try:
        for event in events:
            # Simple duplicate check
            cursor.execute("""
                SELECT id FROM sports_events 
                WHERE event_name = %s AND event_startdatetime = %s
            """, (event.get('event_name'), event.get('event_startdatetime')))
            
            if cursor.fetchone():
                logger.info(f"Duplicate skipped: {event.get('event_name')}")
                continue
            
            # Insert new event
            cursor.execute("""
                INSERT INTO sports_events 
                (event_name, sport_type, description, event_location, 
                 event_address, event_startdatetime, event_enddatetime, link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                event.get('event_name'),
                event.get('sport_type'),
                event.get('description'),
                event.get('event_location'),
                event.get('event_address'),
                event.get('event_startdatetime'),
                event.get('event_enddatetime'),
                event.get('link')
            ))
            
            saved_count += 1
            logger.info(f"Saved: {event.get('event_name')}")
        
        conn.commit()
        logger.info(f"Saved {saved_count} events for {region}")
        return saved_count
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

@app.route('/scrape', methods=['POST'])
def run_scrape():
    """Main endpoint to trigger event scraping"""
    try:
        total_events = 0
        results = {}
        
        for region in REGIONS:
            logger.info(f"Scraping events for: {region}")
            
            # Get events from OpenAI
            events = scrape_region_events(region)
            
            # Save to database
            saved_count = save_events_to_db(events, region)
            
            results[region] = {
                'found': len(events),
                'saved': saved_count
            }
            total_events += saved_count
        
        logger.info(f"Scraping completed! Total events: {total_events}")
        
        return jsonify({
            'status': 'success',
            'total_events': total_events,
            'results': results
        })
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))