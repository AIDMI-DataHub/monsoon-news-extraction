from datetime import datetime, timedelta
import pytz
import os
import pandas as pd
import time
import pygooglenews
import argparse
import re
import requests
from bs4 import BeautifulSoup
from language_map import get_language_for_region, get_all_languages_for_region, get_climate_impact_terms

# Import our smart handler
from smart_google_news_handler import smart_handler

def run_monsoon_script(target_date=None, days_back=0, single_state=None):
    """
    Run the monsoon script with STRICT date filtering for specified date range,
    with improved multilingual support and enhanced content validation.
    Enhanced with smart Google News handling to avoid rate limits.
    
    Args:
        target_date (str): Date in YYYY-MM-DD format, if None uses current date
        days_back (int): Number of days to look back from target_date (0 = target date only)
        single_state (str): If provided, only process this single state/UT
    """
    print("🧠 Initializing Smart Google News Handler...")
    
    # Set date range based on parameters
    ist = pytz.timezone('Asia/Kolkata')
    
    if target_date:
        try:
            end_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            print(f"🗓️ Using specified target date: {end_date}")
        except ValueError:
            print(f"❌ Invalid date format: {target_date}. Using current date.")
            end_date = datetime.now(ist).date()
    else:
        end_date = datetime.now(ist).date()
        print(f"🗓️ Using current date: {end_date}")
    
    start_date = end_date - timedelta(days=days_back)
    
    print(f"🔍 STRICTLY filtering for articles between {start_date} and {end_date}")
    
    # Use broader search window but filter precisely afterward
    when_parameter = f'{max(days_back + 7, 7)}d'
    
    # States and union territories
    states = [
        "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chhattisgarh",
        "goa", "gujarat", "haryana", "himachal-pradesh", "jharkhand", "karnataka",
        "kerala", "madhya-pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
        "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil-nadu", 
        "telangana", "tripura", "uttar-pradesh", "uttarakhand", "west-bengal"
    ]
    union_territories = [
        "andaman-and-nicobar-islands", "chandigarh", 
        "dadra-and-nagar-haveli-and-daman-and-diu",
        "lakshadweep", "delhi", "puducherry", 
        "jammu-and-kashmir", "ladakh"
    ]

    # Filter to single state if specified
    if single_state:
        if single_state in states:
            regions_to_process = [single_state]
            print(f"🎯 Processing only state: {single_state.replace('-', ' ').title()}")
        elif single_state in union_territories:
            regions_to_process = [single_state]
            print(f"🎯 Processing only union territory: {single_state.replace('-', ' ').title()}")
        else:
            print(f"❌ Invalid state/UT: {single_state}")
            print("Available states:", ", ".join(states))
            print("Available UTs:", ", ".join(union_territories))
            return
    else:
        regions_to_process = states + union_territories

    # Clean up existing files for the target date range to avoid duplication
    cleanup_existing_files_for_date_range(start_date, end_date, single_state)
    
    # Load newspaper database
    newspaper_db = load_newspaper_database()
    
    # Process national-level sources only if not filtering to single state
    if not single_state:
        national_sources = get_national_newspapers(newspaper_db)
        national_entries = process_newspaper_sources(national_sources, "national", start_date, end_date)
        if national_entries:
            save_national_results(national_entries, end_date)

    # Print smart handler initialization stats
    print("🧠 Smart Handler Status:")
    print("   🔄 Adaptive delays enabled")
    print("   🛡️ Circuit breaker protection active")
    print("   🎯 Query optimization enabled")
    print("   📊 Pattern learning active")

    # Process each region with smart handling
    for region in regions_to_process:
        region_name = region.replace("-", " ")
        print(f"\n===== Processing region: {region_name.title()} =====")
        
        # Get all languages for this region
        region_languages = get_all_languages_for_region(region)
        print(f"Languages for this region: {', '.join(region_languages)}")
        
        all_region_entries = []
        
        # Smart inter-language delay
        inter_language_delay = smart_handler.adaptive_delay()
        
        for lang_index, lang_code in enumerate(region_languages):
            print(f"\n--- Processing language: {lang_code} ({lang_index + 1}/{len(region_languages)}) ---")
            
            # Get monsoon-specific terms for this language
            monsoon_terms = get_climate_impact_terms(lang_code)
            print(f"🌧️ Using {len(monsoon_terms)} monsoon terms for {lang_code}")
            print(f"📝 Sample terms: {monsoon_terms[:3]}...")
            
            # Initialize Google News with this language
            gn = pygooglenews.GoogleNews(lang=lang_code, country='IN')
            
            # Create comprehensive query strategies for better coverage
            queries = create_smart_monsoon_queries(monsoon_terms, region_name, lang_code)
            
            # Track query performance for this language
            successful_queries = 0
            total_queries = len(queries)
            
            for query_index, query in enumerate(queries):
                print(f"🔍 Query {query_index + 1}/{total_queries}: {query[:60]}{'...' if len(query) > 60 else ''} | Language: {lang_code}")
                
                # Use smart search with advanced error handling
                results = smart_handler.smart_search(
                    gn_instance=gn,
                    query=query,
                    when_parameter=when_parameter,
                    lang_code=lang_code,
                    region=region,
                    max_retries=4
                )
                
                if not results or 'entries' not in results or not results['entries']:
                    print(f"⚠ No results found for this query in [{lang_code}].")
                    continue
                
                total_articles = len(results['entries'])
                print(f"✅ Found {total_articles} total articles for {region_name} in [{lang_code}]")
                
                # Extract with STRICT date filtering and enhanced content validation
                entries = extract_results_with_strict_date_filter(
                    results, "Monsoon", lang_code, start_date, end_date, monsoon_terms
                )
                
                filtered_count = len(entries)
                print(f"📅 STRICTLY filtered to {filtered_count} relevant monsoon articles within date range")
                
                if filtered_count > 0:
                    successful_queries += 1
                    dates = [datetime.strptime(entry[2].split()[0], "%Y-%m-%d").date() 
                            for entry in entries]
                    print(f"📊 Dates in filtered articles: {sorted(set(dates))}")
                
                all_region_entries.extend(entries)
                
                # Smart inter-query delay
                query_delay = smart_handler.adaptive_delay()
                if query_index < total_queries - 1:  # Don't delay after last query
                    print(f"⏳ Smart delay: {query_delay:.1f}s before next query...")
                    time.sleep(query_delay)
            
            # Language processing summary
            success_rate = (successful_queries / total_queries * 100) if total_queries > 0 else 0
            print(f"📈 Language {lang_code} summary: {successful_queries}/{total_queries} queries successful ({success_rate:.1f}%)")
            
            # Smart inter-language delay (longer between languages)
            if lang_index < len(region_languages) - 1:  # Don't delay after last language
                print(f"⏳ Inter-language delay: {inter_language_delay:.1f}s...")
                time.sleep(inter_language_delay)
            
            # Check region-specific newspapers for this state
            region_newspapers = get_regional_newspapers(newspaper_db, region)
            if region_newspapers:
                additional_entries = process_newspaper_sources(
                    region_newspapers, region, start_date, end_date
                )
                if additional_entries:
                    all_region_entries.extend(additional_entries)
        
        # Save combined results for this region
        if all_region_entries:
            region_type = 'states' if region in states else 'union-territories'
            save_results(
                all_region_entries,
                region_type,
                region,
                end_date
            )
        else:
            print(f"No monsoon articles found for {region_name}")
        
        # Print smart handler statistics for this region
        stats = smart_handler.get_statistics()
        print(f"🧠 Smart Handler Stats for {region_name}:")
        print(f"   📊 Overall success rate: {stats['success_rate']:.1f}%")
        print(f"   🔄 Circuit breaker: {stats['circuit_breaker_state']}")
        print(f"   🚫 Banned patterns: {stats['banned_patterns']}")
    
    # Final pipeline statistics
    print(f"\n🎉 Pipeline completed! Final Smart Handler Statistics:")
    final_stats = smart_handler.get_statistics()
    print(f"📊 Total requests: {final_stats['total_requests']}")
    print(f"✅ Successful requests: {final_stats['successful_requests']}")
    print(f"📈 Overall success rate: {final_stats['success_rate']:.1f}%")
    print(f"🔄 Circuit breaker state: {final_stats['circuit_breaker_state']}")
    print(f"🚫 Banned query patterns: {final_stats['banned_patterns']}")
    
    # Cleanup sessions
    smart_handler.cleanup_sessions()

def create_smart_monsoon_queries(monsoon_terms, region_name, lang_code):
    """
    Create optimized queries using smart handler insights and patterns.
    Reduces query volume and complexity to avoid rate limiting.
    """
    if not monsoon_terms:
        print(f"⚠️ No monsoon terms found for language {lang_code}")
        return [f"monsoon {region_name}"]
    
    print(f"🔤 Creating SMART queries from {len(monsoon_terms)} terms for {lang_code}")
    
    queries = []
    
    # Strategy 1: Start with individual high-impact terms (reduced from 5 to 3)
    priority_terms = monsoon_terms[:3]  # Only top 3 terms
    for i, term in enumerate(priority_terms):
        if term.strip():
            query = f'"{term}" {region_name}'
            queries.append(query)
            print(f"   Priority query {i+1}: {query}")
    
    # Strategy 2: Smart weather phenomena combination (simplified)
    if len(monsoon_terms) >= 3:
        weather_query = f'({monsoon_terms[0]} OR {monsoon_terms[1]}) {region_name}'
        queries.append(weather_query)
        print(f"   Weather query: {weather_query}")
    
    # Strategy 3: Impact terms (reduced complexity)
    if len(monsoon_terms) >= 6:
        impact_terms = monsoon_terms[3:6]  # Only 3 impact terms instead of 5
        impact_query = f'({" OR ".join(impact_terms[:2])}) {region_name}'  # Only 2 terms
        queries.append(impact_query)
        print(f"   Impact query: {impact_query}")
    
    # Strategy 4: Adaptive query count based on environment and success patterns
    is_local = os.environ.get('GITHUB_ACTIONS') != 'true'
    
    if is_local:
        # Local testing - very conservative
        print("🏠 Local mode: Using minimal query set to avoid rate limits")
        queries = queries[:3]  # Only first 3 queries
    else:
        # GitHub Actions - can be more aggressive but still smart
        # Strategy 5: Health/infrastructure (only if we have enough terms)
        if len(monsoon_terms) >= 10:
            health_terms = monsoon_terms[8:10]  # Only 2 health terms
            health_query = f'({" OR ".join(health_terms)}) {region_name}'
            queries.append(health_query)
            print(f"   Health query: {health_query}")
        
        # Strategy 6: Broad search (simplified)
        if len(monsoon_terms) >= 8:
            broad_terms = [
                monsoon_terms[0],    # main monsoon term
                monsoon_terms[3] if len(monsoon_terms) > 3 else monsoon_terms[1],    # flood term
                monsoon_terms[7] if len(monsoon_terms) > 7 else monsoon_terms[-1]    # last available term
            ]
            broad_query = f'({" OR ".join([term for term in broad_terms if term])}) {region_name}'
            queries.append(broad_query)
            print(f"   Broad query: {broad_query}")
    
    # Smart query validation and optimization
    optimized_queries = []
    for query in queries:
        # Skip overly complex queries that often get rate limited
        if query.count('OR') <= 4 and len(query) <= 200:  # Reasonable complexity limits
            optimized_queries.append(query)
        else:
            print(f"🔧 Skipping overly complex query: {query[:50]}...")
    
    print(f"📊 Created {len(optimized_queries)} optimized queries for {lang_code}")
    return optimized_queries

def load_newspaper_database():
    """Load the newspaper database from CSV file"""
    try:
        df = pd.read_csv('list_of_newspaper_statewise  Sheet1.csv')
        print(f"📰 Loaded {len(df)} newspapers from database")
        return df
    except FileNotFoundError:
        print("⚠ Newspaper database file not found. Using fallback sources.")
        return pd.DataFrame()

def get_national_newspapers(newspaper_db):
    """Get national-level newspapers from database"""
    if newspaper_db.empty:
        return []
    
    national_papers = newspaper_db[
        newspaper_db['State/UT'].str.contains('National', na=False, case=False)
    ]
    
    newspapers = []
    for _, row in national_papers.iterrows():
        newspapers.append({
            'name': row['Newspaper Name'],
            'website': row['Website'],
            'language': row['Language(s)'],
            'state': 'National'
        })
    
    print(f"📰 Found {len(newspapers)} national newspapers")
    return newspapers

def get_regional_newspapers(newspaper_db, region):
    """Get newspapers for a specific region from database"""
    if newspaper_db.empty:
        return []
    
    # Convert region format for matching
    region_display = region.replace('-', ' ').title()
    
    # Try different matching strategies
    matching_strategies = [
        region_display,
        region_display.replace('And', '&'),
        region_display.split()[0] if ' ' in region_display else region_display,
    ]
    
    # Special mappings for different naming conventions
    region_mappings = {
        'andaman-and-nicobar-islands': ['Andaman', 'Nicobar'],
        'dadra-and-nagar-haveli-and-daman-and-diu': ['Dadra', 'Nagar Haveli', 'Daman', 'Diu'],
        'jammu-and-kashmir': ['Jammu', 'Kashmir', 'J&K'],
        'uttar-pradesh': ['Uttar Pradesh', 'UP'],
        'madhya-pradesh': ['Madhya Pradesh', 'MP'],
        'himachal-pradesh': ['Himachal Pradesh', 'HP'],
        'arunachal-pradesh': ['Arunachal Pradesh'],
        'west-bengal': ['West Bengal', 'Bengal'],
    }
    
    if region in region_mappings:
        matching_strategies.extend(region_mappings[region])
    
    regional_papers = pd.DataFrame()
    for strategy in matching_strategies:
        matches = newspaper_db[
            newspaper_db['State/UT'].str.contains(strategy, na=False, case=False)
        ]
        if not matches.empty:
            regional_papers = matches
            break
    
    newspapers = []
    for _, row in regional_papers.iterrows():
        newspapers.append({
            'name': row['Newspaper Name'],
            'website': row['Website'],
            'language': row['Language(s)'],
            'state': row['State/UT']
        })
    
    if newspapers:
        print(f"📰 Found {len(newspapers)} newspapers for {region_display}")
    
    return newspapers

def extract_results_with_strict_date_filter(results, term, lang_code, start_date, end_date, monsoon_terms):
    """Extract results with extremely strict date filtering and enhanced monsoon content validation"""
    extracted_entries = []
    rejected_count = 0
    content_rejected = 0
    parse_error_count = 0
    
    for entry in results.get('entries', []):
        title = entry.title
        link = entry.link
        
        # Enhanced content validation: Check title for monsoon relevance
        if not is_monsoon_content_relevant(title, monsoon_terms):
            content_rejected += 1
            continue
        
        # Extract date from URL first
        url_date = extract_date_from_url(link)
        
        # Get publication date in IST
        ist_date_str = convert_gmt_to_ist(entry.published)
        
        # Parse the IST datetime string with strict validation
        try:
            ist_dt = datetime.strptime(ist_date_str, "%Y-%m-%d %H:%M:%S")
            article_date = ist_dt.date()
            
            # STRICT date filtering - only include articles within date range
            if article_date < start_date or article_date > end_date:
                # If URL date is available and within range, use that instead
                if url_date and start_date <= url_date <= end_date:
                    article_date = url_date
                    ist_date_str = article_date.strftime("%Y-%m-%d") + " 12:00:00"
                else:
                    rejected_count += 1
                    continue
                    
        except ValueError:
            parse_error_count += 1
            # Use URL date if available and within range
            if url_date and start_date <= url_date <= end_date:
                article_date = url_date
                ist_date_str = article_date.strftime("%Y-%m-%d") + " 12:00:00"
            else:
                # Skip if we can't determine a valid date
                continue

        # Get summary and validate content relevance
        summary = entry.summary if hasattr(entry, 'summary') else ""
        combined_text = f"{title} {summary}"
        
        # Final content validation with combined title and summary
        if not is_monsoon_content_relevant(combined_text, monsoon_terms):
            content_rejected += 1
            continue

        source = ""
        if hasattr(entry, 'source') and hasattr(entry.source, 'title'):
            source = entry.source.title

        extracted_entries.append([title, link, ist_date_str, source, summary, term, lang_code])
    
    if rejected_count > 0 or content_rejected > 0 or parse_error_count > 0:
        print(f"ℹ️ Rejected {rejected_count} articles outside date range, {content_rejected} not monsoon-relevant, {parse_error_count} with parsing errors")
        
    return extracted_entries

def is_monsoon_content_relevant(text, monsoon_terms):
    """Enhanced validation to check if content is genuinely monsoon-related using actual language terms"""
    if not text or not monsoon_terms:
        return False
    
    text_lower = text.lower()
    
    # Primary check: Must contain at least one monsoon term from our language map
    monsoon_matches = sum(1 for term in monsoon_terms if term.lower() in text_lower)
    if monsoon_matches == 0:
        return False
    
    # Exclude clearly irrelevant content (keep this language-neutral)
    irrelevant_patterns = [
        'fashion', 'beauty', 'recipe', 'cooking', 'sports score', 'cricket', 'football',
        'entertainment', 'celebrity', 'movie release', 'film', 'music album', 
        'festival celebration', 'wedding', 'marriage ceremony', 'astrology', 'horoscope',
        'stock market', 'share price', 'investment', 'real estate deal', 'property sale'
    ]
    
    irrelevant_count = sum(1 for pattern in irrelevant_patterns if pattern in text_lower)
    if irrelevant_count > 2:  # Multiple irrelevant indicators
        return False
    
    # For non-English languages, be more lenient since we have specific terms
    # If we found monsoon terms in local language, it's likely relevant
    if monsoon_matches >= 1:
        # Additional check: if we have multiple monsoon terms, it's very likely relevant
        if monsoon_matches >= 2:
            return True
        
        # Single monsoon term: check for additional context clues
        # Look for generic weather/impact words that work across languages
        context_indicators = [
            'weather', 'government', 'alert', 'warning', 'rescue', 'relief', 'help',
            'damage', 'affected', 'impact', 'water', 'river', 'road', 'house',
            'people', 'area', 'district', 'village', 'city', 'state'
        ]
        
        context_count = sum(1 for indicator in context_indicators if indicator in text_lower)
        
        # If we have monsoon terms + some context, it's likely relevant
        return context_count >= 1
    
    return False

def process_newspaper_sources(newspapers, region, start_date, end_date):
    """Process newspaper websites intelligently for monsoon content with smart delays"""
    entries = []
    
    for i, newspaper in enumerate(newspapers):
        try:
            print(f"Checking newspaper: {newspaper['name']} ({newspaper['language']})")
            website = newspaper['website']
            
            # Skip invalid URLs
            if not website or not website.startswith(('http://', 'https://')):
                continue
            
            # Smart delay between newspaper requests
            if i > 0:
                delay = smart_handler.adaptive_delay() * 0.5  # Shorter delay for newspapers
                time.sleep(delay)
            
            response = requests.get(website, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            }, timeout=15)
            
            if response.status_code != 200:
                print(f"Failed to access {website}: Status code {response.status_code}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get language-specific monsoon terms
            newspaper_lang = map_newspaper_language_to_code(newspaper['language'])
            monsoon_terms = get_climate_impact_terms(newspaper_lang)
            
            # Find monsoon-related content using multiple strategies
            monsoon_links = find_smart_monsoon_content(soup, website, monsoon_terms)
            
            print(f"Found {len(monsoon_links)} potential monsoon articles on {newspaper['name']}")
            
            # Process each link with validation (limited to prevent overload)
            for link in monsoon_links[:4]:  # Reduced from 6 to 4 articles per newspaper
                try:
                    article_data = extract_and_validate_newspaper_article(
                        link, start_date, end_date, monsoon_terms, newspaper['name']
                    )
                    if article_data:
                        entries.append(article_data)
                        print(f"Added monsoon article: {article_data[0][:60]}...")
                
                except Exception as e:
                    print(f"Error processing article {link}: {e}")
                    
                # Brief pause between articles
                time.sleep(1)
            
        except Exception as e:
            print(f"Error checking newspaper {newspaper['name']}: {e}")
    
    return entries

# Include all the remaining functions from your original monsoon.py
def map_newspaper_language_to_code(language_str):
    """Map newspaper language string to language code"""
    language_mapping = {
        'english': 'en', 'hindi': 'hi', 'tamil': 'ta', 'telugu': 'te',
        'malayalam': 'ml', 'kannada': 'kn', 'bengali': 'bn', 'gujarati': 'gu',
        'marathi': 'mr', 'odia': 'or', 'punjabi': 'pa', 'assamese': 'as',
        'urdu': 'ur', 'nepali': 'ne', 'khasi': 'en', 'meitei': 'en', 'mizo': 'en'
    }
    
    if not language_str:
        return 'en'
    
    lang_lower = language_str.lower()
    for lang_name, code in language_mapping.items():
        if lang_name in lang_lower:
            return code
    
    return 'en'  # Default to English

def find_smart_monsoon_content(soup, base_url, monsoon_terms):
    """Intelligently find monsoon-related content without assuming specific sections"""
    links = set()
    
    # Strategy 1: Look for links with monsoon-related text
    for a in soup.find_all('a', href=True):
        href = a['href']
        link_text = a.get_text().strip().lower()
        
        # Make relative URLs absolute
        if href.startswith('/'):
            base_domain = '/'.join(base_url.split('/')[:3])
            href = base_domain + href
        elif not href.startswith(('http://', 'https://')):
            continue
        
        # Skip non-article content
        if any(skip in href.lower() for skip in ['.jpg', '.png', '.pdf', '.mp4', 'facebook.com', 'twitter.com', 'instagram.com']):
            continue
        
        # Check if link text contains monsoon terms
        if any(term.lower() in link_text for term in monsoon_terms[:8]):  # Check top 8 terms
            links.add(href)
            continue
        
        # Check href for weather/monsoon keywords
        if any(keyword in href.lower() for keyword in ['weather', 'rain', 'flood', 'monsoon', 'storm']):
            links.add(href)
            continue
        
        # Check surrounding context (parent element text)
        parent = a.parent
        if parent:
            context = parent.get_text().lower()
            if any(term.lower() in context for term in monsoon_terms[:5]):
                links.add(href)
    
    # Strategy 2: Look for articles in news/weather sections
    news_sections = soup.find_all(['div', 'section'], class_=re.compile(r'news|weather|local|state|national', re.I))
    for section in news_sections:
        section_links = section.find_all('a', href=True)
        for a in section_links:
            href = a['href']
            if href.startswith('/'):
                base_domain = '/'.join(base_url.split('/')[:3])
                href = base_domain + href
            elif not href.startswith(('http://', 'https://')):
                continue
                
            # Add links from news sections that might contain relevant content
            if 'article' in href.lower() or 'news' in href.lower():
                links.add(href)
    
    # Strategy 3: Look for recent articles (today's date in URL)
    today_patterns = [
        datetime.now().strftime('%Y/%m/%d'),
        datetime.now().strftime('%Y-%m-%d'),
        datetime.now().strftime('%Y%m%d')
    ]
    
    for a in soup.find_all('a', href=True):
        href = a['href']
        if any(pattern in href for pattern in today_patterns):
            if href.startswith('/'):
                base_domain = '/'.join(base_url.split('/')[:3])
                href = base_domain + href
            elif href.startswith(('http://', 'https://')):
                links.add(href)
    
    return list(links)

def extract_and_validate_newspaper_article(url, start_date, end_date, monsoon_terms, newspaper_name):
    """Extract and validate newspaper article for monsoon relevance"""
    try:
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=15)
        
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title with multiple fallback strategies
        title = extract_article_title(soup)
        if not title or not is_monsoon_content_relevant(title, monsoon_terms):
            return None
        
        # Extract and validate date
        article_date = extract_article_date_enhanced(soup, url)
        if not article_date or article_date < start_date or article_date > end_date:
            return None
        
        # Extract summary/content for final validation
        summary = extract_article_summary_enhanced(soup)
        
        # Final comprehensive relevance check
        combined_text = f"{title} {summary}"
        if not is_monsoon_content_relevant(combined_text, monsoon_terms):
            return None
        
        # Detect language
        lang_code = detect_language_from_text(combined_text)
        
        date_str = article_date.strftime("%Y-%m-%d %H:%M:%S")
        
        return [title, url, date_str, newspaper_name, summary[:200], "Monsoon", lang_code]
        
    except Exception as e:
        print(f"Error extracting article from {url}: {e}")
        return None

def extract_article_title(soup):
    """Extract article title with multiple strategies"""
    # Strategy 1: Look for h1 tags
    h1_tags = soup.find_all('h1')
    for h1 in h1_tags:
        text = h1.get_text().strip()
        if len(text) > 10 and len(text) < 200:  # Reasonable title length
            return text
    
    # Strategy 2: Look for title tag
    title_tag = soup.find('title')
    if title_tag:
        text = title_tag.get_text().strip()
        # Clean common title suffixes
        for suffix in [' - Times of India', ' | The Hindu', ' - News18', ' | NDTV']:
            if text.endswith(suffix):
                text = text[:-len(suffix)]
        if len(text) > 10:
            return text
    
    # Strategy 3: Look for meta property title
    meta_title = soup.find('meta', property='og:title')
    if meta_title and meta_title.get('content'):
        return meta_title['content'].strip()
    
    return None

def extract_article_date_enhanced(soup, url):
    """Enhanced date extraction with multiple fallback strategies"""
    # Strategy 1: Try URL date first (most reliable)
    url_date = extract_date_from_url(url)
    if url_date:
        return url_date
    
    # Strategy 2: Meta tags
    meta_selectors = [
        'meta[property="article:published_time"]',
        'meta[name="publishdate"]',
        'meta[name="date"]',
        'meta[property="og:updated_time"]'
    ]
    
    for selector in meta_selectors:
        meta_tag = soup.select_one(selector)
        if meta_tag and meta_tag.get('content'):
            try:
                # Handle ISO format dates
                date_str = meta_tag['content']
                if 'T' in date_str:
                    date_str = date_str.split('T')[0]
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                continue
    
    # Strategy 3: Look for date in common CSS classes
    date_selectors = [
        '.publish-date', '.article-date', '.date', '.timestamp',
        '[class*="date"]', '[class*="time"]', '.byline-date'
    ]
    
    for selector in date_selectors:
        date_elem = soup.select_one(selector)
        if date_elem:
            date_text = date_elem.get_text().strip()
            parsed_date = parse_date_string_enhanced(date_text)
            if parsed_date:
                return parsed_date
    
    # Strategy 4: Look for date patterns in article text
    article_text = soup.get_text()
    date_patterns = [
        r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # 15 March 2024
        r'([A-Za-z]+\s+\d{1,2},\s+\d{4})',  # March 15, 2024
        r'(\d{4}-\d{2}-\d{2})',             # 2024-03-15
        r'(\d{1,2}/\d{1,2}/\d{4})'          # 15/03/2024
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, article_text)
        if match:
            parsed_date = parse_date_string_enhanced(match.group(1))
            if parsed_date:
                return parsed_date
    
    return None

def extract_article_summary_enhanced(soup):
    """Extract article summary with enhanced strategies"""
    # Strategy 1: Meta description
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc and 'content' in meta_desc.attrs:
        content = meta_desc['content'].strip()
        if len(content) > 50:
            return content[:300]
    
    # Strategy 2: First substantial paragraph
    paragraphs = soup.find_all('p')
    for p in paragraphs:
        text = p.get_text().strip()
        if len(text) > 50 and not any(skip in text.lower() for skip in ['cookie', 'subscribe', 'follow us']):
            return text[:300]
    
    # Strategy 3: Article lead or summary class
    summary_selectors = [
        '.article-summary', '.lead', '.excerpt', '.description',
        '[class*="summary"]', '[class*="lead"]'
    ]
    
    for selector in summary_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text().strip()
            if len(text) > 50:
                return text[:300]
    
    return ""

def parse_date_string_enhanced(date_str):
    """Parse various date string formats with enhanced support"""
    if not date_str:
        return None
        
    # Clean the date string
    date_str = re.sub(r'[^\w\s\-:/,]', '', date_str).strip()
    
    date_formats = [
        '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y',
        '%B %d, %Y', '%d %B %Y', '%b %d, %Y', '%d %b %Y',
        '%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M:%S',
        '%d %B, %Y', '%B %d %Y', '%d %b, %Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # Try parsing relative dates like "2 days ago"
    if 'ago' in date_str.lower():
        today = datetime.now().date()
        if 'today' in date_str.lower() or '0 day' in date_str.lower():
            return today
        elif 'yesterday' in date_str.lower() or '1 day' in date_str.lower():
            return today - timedelta(days=1)
        elif '2 day' in date_str.lower():
            return today - timedelta(days=2)
    
    return None

def detect_language_from_text(text):
    """Simple language detection based on character frequency"""
    if not text or len(text) < 50:
        return "en"
        
    # Count characters in different scripts
    devanagari = sum(1 for c in text if '\u0900' <= c <= '\u097F')
    bengali = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    tamil = sum(1 for c in text if '\u0B80' <= c <= '\u0BFF')
    telugu = sum(1 for c in text if '\u0C00' <= c <= '\u0C7F')
    kannada = sum(1 for c in text if '\u0C80' <= c <= '\u0CFF')
    malayalam = sum(1 for c in text if '\u0D00' <= c <= '\u0D7F')
    gujarati = sum(1 for c in text if '\u0A80' <= c <= '\u0AFF')
    punjabi = sum(1 for c in text if '\u0A00' <= c <= '\u0A7F')
    
    total_len = len(text)
    scripts = {
        "hi": devanagari, "bn": bengali, "ta": tamil, "te": telugu,
        "kn": kannada, "ml": malayalam, "gu": gujarati, "pa": punjabi
    }
    
    # If the text has significant non-Latin characters, identify the script
    for lang, count in scripts.items():
        if count > total_len * 0.15:  # If script represents over 15% of text
            return lang
    
    return "en"

def extract_date_from_url(url):
    """Try to extract date from URL patterns commonly found in news sites"""
    patterns = [
        r'/(\d{4})/(\d{1,2})/(\d{1,2})/',  # /2024/3/3/
        r'/(\d{4})-(\d{1,2})-(\d{1,2})/',  # /2024-3-3/
        r'(\d{4})(\d{2})(\d{2})',          # 20240303
        r'article(\d{8})',                 # article20240303
        r'/(\d{2})-(\d{2})-(\d{4})/',      # /15-03-2024/
        r'/(\d{2})(\d{2})(\d{4})/',        # /15032024/
        r'/news/(\d{4})/(\d{1,2})/(\d{1,2})/', # /news/2024/3/15/
        r'(\d{1,2})_(\d{1,2})_(\d{4})',    # 15_03_2024
        r'-(\d{4})(\d{2})(\d{2})-',        # -20240315-
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            try:
                groups = match.groups()
                
                if len(groups) == 1:  # Single group like 20240315
                    date_str = groups[0]
                    if len(date_str) == 8:
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                    else:
                        continue
                elif len(groups[0]) == 4:  # Year first
                    year = int(groups[0])
                    month = int(groups[1])
                    day = int(groups[2])
                elif len(groups[2]) == 4:  # Year last
                    day = int(groups[0])
                    month = int(groups[1])
                    year = int(groups[2])
                else:
                    continue
                
                # Validate date components
                if year < 2000 or year > 2030 or month < 1 or month > 12 or day < 1 or day > 31:
                    continue
                    
                return datetime(year, month, day).date()
            except (ValueError, IndexError):
                continue
    
    return None

def cleanup_existing_files_for_date_range(start_date, end_date, single_state=None):
    """Remove existing files for the date range to avoid duplication"""
    base_path = "data"
    current_date = start_date
    deleted_count = 0
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        
        if single_state:
            # Only clean up for the specific state
            states = [
                "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chhattisgarh",
                "goa", "gujarat", "haryana", "himachal-pradesh", "jharkhand", "karnataka",
                "kerala", "madhya-pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
                "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil-nadu", 
                "telangana", "tripura", "uttar-pradesh", "uttarakhand", "west-bengal"
            ]
            union_territories = [
                "andaman-and-nicobar-islands", "chandigarh", 
                "dadra-and-nagar-haveli-and-daman-and-diu",
                "lakshadweep", "delhi", "puducherry", 
                "jammu-and-kashmir", "ladakh"
            ]
            
            if single_state in states:
                region_type = "states"
            elif single_state in union_territories:
                region_type = "union-territories"
            else:
                continue
                
            path = f"{base_path}/{region_type}/{single_state}/Monsoon/{year}/{month:02d}/{day:02d}"
            if os.path.exists(path):
                for file in os.listdir(path):
                    if file.endswith('.csv'):
                        file_path = os.path.join(path, file)
                        try:
                            os.remove(file_path)
                            deleted_count += 1
                        except Exception as e:
                            print(f"Error deleting {file_path}: {e}")
        else:
            # Clean up all regions (original behavior)
            regions_info = [
                ("states", [
                    "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chhattisgarh",
                    "goa", "gujarat", "haryana", "himachal-pradesh", "jharkhand", "karnataka",
                    "kerala", "madhya-pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
                    "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil-nadu", 
                    "telangana", "tripura", "uttar-pradesh", "uttarakhand", "west-bengal"
                ]),
                ("union-territories", [
                    "andaman-and-nicobar-islands", "chandigarh", 
                    "dadra-and-nagar-haveli-and-daman-and-diu",
                    "lakshadweep", "delhi", "puducherry", 
                    "jammu-and-kashmir", "ladakh"
                ]),
                ("national", ["all"])
            ]
            
            for region_type, regions in regions_info:
                for region_name in regions:
                    path = f"{base_path}/{region_type}/{region_name}/Monsoon/{year}/{month:02d}/{day:02d}"
                    if os.path.exists(path):
                        for file in os.listdir(path):
                            if file.endswith('.csv'):
                                file_path = os.path.join(path, file)
                                try:
                                    os.remove(file_path)
                                    deleted_count += 1
                                except Exception as e:
                                    print(f"Error deleting {file_path}: {e}")
        
        current_date += timedelta(days=1)
    
    if deleted_count > 0:
        print(f"🧹 Cleaned up {deleted_count} existing files for date range")

def save_results(all_entries, region_type, region_name, current_date):
    """Save results to CSV file"""
    if not all_entries:
        return

    path = f"data/{region_type}/{region_name}/Monsoon/{current_date.year}/{current_date.strftime('%m')}/{current_date.strftime('%d')}"
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, 'results.csv')

    columns = ["Title", "Link", "Date", "Source", "Summary", "Term", "LanguageQueried"]
    df = pd.DataFrame(all_entries, columns=columns)

    # Remove duplicates based on URL
    if len(df) > 0:
        before_count = len(df)
        df = df.drop_duplicates(subset=['Link'])
        if before_count > len(df):
            print(f"ℹ️ Removed {before_count - len(df)} duplicate articles")
    
    # Perform final validation of dates before saving
    if 'Date' in df.columns and len(df) > 0:
        df['_DateCheck'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        df = df.dropna(subset=['_DateCheck'])
        df = df.drop(columns=['_DateCheck'])
        
    df.to_csv(file_path, mode='w', header=True, index=False)
    print(f"✅ CSV created for Monsoon in {region_name} with {len(df)} articles")

def save_national_results(all_entries, current_date):
    """Save national-level results"""
    if not all_entries:
        return

    path = f"data/national/all/Monsoon/{current_date.year}/{current_date.strftime('%m')}/{current_date.strftime('%d')}"
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, 'results.csv')

    columns = ["Title", "Link", "Date", "Source", "Summary", "Term", "LanguageQueried"]
    df = pd.DataFrame(all_entries, columns=columns)

    # Remove duplicates based on URL
    if len(df) > 0:
        before_count = len(df)
        df = df.drop_duplicates(subset=['Link'])
        if before_count > len(df):
            print(f"ℹ️ Removed {before_count - len(df)} duplicate articles")
    
    # Perform final validation of dates before saving
    if 'Date' in df.columns and len(df) > 0:
        df['_DateCheck'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        df = df.dropna(subset=['_DateCheck'])
        df = df.drop(columns=['_DateCheck'])
        
    df.to_csv(file_path, mode='w', header=True, index=False)
    print(f"✅ CSV created for national Monsoon articles with {len(df)} articles")

def convert_gmt_to_ist(gmt_datetime):
    """Convert GMT datetime to IST"""
    try:
        gmt_format = "%a, %d %b %Y %H:%M:%S %Z"
        gmt = pytz.timezone('GMT')
        ist = pytz.timezone('Asia/Kolkata')
        gmt_dt = datetime.strptime(gmt_datetime, gmt_format)
        gmt_dt = gmt.localize(gmt_dt)
        return gmt_dt.astimezone(ist).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return gmt_datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run smart monsoon news article collection script with advanced rate limiting')
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format (default: current date)')
    parser.add_argument('--days-back', type=int, default=0, help='Number of days to look back from target date (default: 0)')
    parser.add_argument('--state', type=str, help='Process only this single state/UT (e.g., kerala, maharashtra, delhi)')
    parser.add_argument('--reset-smart-handler', action='store_true', help='Reset smart handler state for fresh start')
    
    args = parser.parse_args()
    
    if args.reset_smart_handler:
        smart_handler.reset_state()
        print("🔄 Smart handler state reset")
    
    print("🌧️ Starting Smart Monsoon News Collection")
    print(f"📅 Target date: {args.date if args.date else 'Current date'}")
    print(f"📅 Days back: {args.days_back}")
    if args.state:
        print(f"🎯 Single state mode: {args.state}")
    
    try:
        run_monsoon_script(target_date=args.date, days_back=args.days_back, single_state=args.state)
        
        # Print final smart handler statistics
        print("\n🎯 Final Smart Handler Report:")
        final_stats = smart_handler.get_statistics()
        print(f"📊 Success rate: {final_stats['success_rate']:.1f}%")
        print(f"🔄 Circuit breaker: {final_stats['circuit_breaker_state']}")
        if final_stats['per_region_stats']:
            print("📍 Top performing regions:")
            region_stats = sorted(final_stats['per_region_stats'].items(), 
                                key=lambda x: x[1]['success_rate'], reverse=True)[:5]
            for region, stats in region_stats:
                print(f"   {region}: {stats['success_rate']:.1f}% ({stats['requests']} requests)")
        
        print("✅ Smart Monsoon News Collection completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️ Collection interrupted by user")
        print("🧠 Smart handler statistics at interruption:")
        stats = smart_handler.get_statistics()
        print(f"📊 Processed {stats['total_requests']} requests with {stats['success_rate']:.1f}% success rate")
        smart_handler.cleanup_sessions()
    except Exception as e:
        print(f"❌ Error during collection: {e}")
        print("🧠 Smart handler final statistics:")
        stats = smart_handler.get_statistics()
        print(f"📊 Processed {stats['total_requests']} requests with {stats['success_rate']:.1f}% success rate")
        smart_handler.cleanup_sessions()
        raise