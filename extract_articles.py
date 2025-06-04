import os
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
from article_scraper import ArticleScraper
import re
import hashlib
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def normalize_url(url):
    """
    Normalize URL for better duplicate detection by removing tracking parameters
    and standardizing format.
    """
    try:
        # Handle Google News RSS URLs specially
        if 'news.google.com/rss/articles/' in url:
            # For Google News URLs, use the unique article ID part
            article_id_match = re.search(r'/articles/([^?]+)', url)
            if article_id_match:
                return f"google_news_{article_id_match.group(1)}"
        
        # Parse the URL
        parsed = urlparse(url)
        
        # Remove common tracking parameters
        if parsed.query:
            query_params = parse_qs(parsed.query)
            # Remove tracking parameters
            tracking_params = [
                'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
                'fbclid', 'gclid', '_ga', '_gac', 'ref', 'source', 'medium',
                'campaign', 'oc'  # Google News 'oc' parameter
            ]
            for param in tracking_params:
                query_params.pop(param, None)
            
            # Rebuild query string
            new_query = urlencode(query_params, doseq=True)
            parsed = parsed._replace(query=new_query)
        
        # Remove fragment and normalize
        parsed = parsed._replace(fragment='')
        normalized = urlunparse(parsed).lower().rstrip('/')
        
        return normalized
    except Exception as e:
        logging.warning(f"Error normalizing URL {url}: {e}")
        return url.lower().rstrip('/')

def match_final_url_to_original(final_url, original_urls):
    """
    Improved URL matching logic to handle redirects and variations.
    Returns the best matching original URL or None.
    """
    if not final_url or not original_urls:
        return None
    
    # Strategy 1: Exact match
    for original_url in original_urls:
        if final_url == original_url:
            return original_url
    
    # Strategy 2: Handle Google News redirects
    if 'news.google.com' in final_url:
        # For Google News, try to find any original URL that was also from Google News
        for original_url in original_urls:
            if 'news.google.com' in original_url:
                return original_url
    
    # Strategy 3: Domain matching with path similarity
    final_domain = extract_domain(final_url)
    final_path = urlparse(final_url).path.lower()
    
    domain_matches = []
    for original_url in original_urls:
        original_domain = extract_domain(original_url)
        if final_domain == original_domain:
            domain_matches.append(original_url)
    
    if domain_matches:
        # If multiple domain matches, try to find the one with most similar path
        if len(domain_matches) == 1:
            return domain_matches[0]
        
        # Find best path match
        best_match = domain_matches[0]
        best_similarity = 0
        
        for match_url in domain_matches:
            match_path = urlparse(match_url).path.lower()
            # Simple similarity based on common path components
            final_parts = set(final_path.split('/'))
            match_parts = set(match_path.split('/'))
            common_parts = final_parts.intersection(match_parts)
            similarity = len(common_parts) / max(len(final_parts), len(match_parts), 1)
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = match_url
        
        return best_match
    
    # Strategy 4: Fuzzy domain matching (handle subdomains)
    final_main_domain = extract_main_domain(final_domain)
    
    for original_url in original_urls:
        original_domain = extract_domain(original_url)
        original_main_domain = extract_main_domain(original_domain)
        
        if final_main_domain == original_main_domain:
            return original_url
    
    # Strategy 5: Check if final URL starts with any original URL (or vice versa)
    for original_url in original_urls:
        if final_url.startswith(original_url) or original_url.startswith(final_url):
            return original_url
    
    return None

def extract_main_domain(domain):
    """Extract main domain (e.g., 'example.com' from 'www.news.example.com')"""
    if not domain:
        return ""
    
    parts = domain.split('.')
    if len(parts) >= 2:
        return '.'.join(parts[-2:])
    return domain

def extract_domain(url):
    """Extract domain from URL with better error handling"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix for consistency
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception as e:
        logging.warning(f"Error extracting domain from {url}: {e}")
        # Fallback regex
        match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        return match.group(1).lower() if match else ""

def extract_articles_from_csv(csv_path, scraper, region_name, disaster_type):
    """
    Extract articles from a CSV file with improved error handling and better URL processing.
    """
    if not os.path.exists(csv_path):
        logging.warning(f"CSV path {csv_path} does not exist.")
        return []
        
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logging.error(f"Failed to read CSV {csv_path}: {e}")
        return []
        
    if "Link" not in df.columns:
        logging.warning(f"⚠ Skipping {csv_path}, no 'Link' column found.")
        return []
        
    urls = df["Link"].fillna("").tolist()
    valid_urls = []
    
    # Better URL validation and cleaning
    for url in urls:
        if url and isinstance(url, str):
            url = url.strip()
            if url.startswith("http://") or url.startswith("https://"):
                valid_urls.append(url)
    
    if len(valid_urls) < len(urls):
        logging.warning(f"⚠ Skipped {len(urls) - len(valid_urls)} invalid URLs in {csv_path}")
    
    if not valid_urls:
        logging.warning(f"⚠ No valid URLs found in {csv_path}")
        return []
    
    logging.info(f"📊 Processing {len(valid_urls)} URLs from {csv_path}")
    
    # Process in smaller batches for better resource management
    batch_size = 5
    all_results = []
    
    for i in range(0, len(valid_urls), batch_size):
        batch_urls = valid_urls[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(valid_urls) + batch_size - 1)//batch_size
        
        logging.info(f"Processing batch {batch_num}/{total_batches} from {csv_path}")
        
        # Use the improved ArticleScraper to extract articles
        batch_results = scraper.getArticles(batch_urls)
        
        # Only keep successful extractions
        successful_results = []
        for j, result in enumerate(batch_results):
            if j >= len(batch_urls):
                continue
                
            original_url = batch_urls[j]
            
            # Check if extraction was successful
            if result and len(result) >= 3 and result[2]:
                if len(result) == 4:
                    final_url, article_title, article_text, detected_language = result
                else:
                    final_url, article_title, article_text = result
                    detected_language = "en"  # Default
                    
                if article_text and len(article_text) > 200:  # Only keep meaningful extractions
                    successful_results.append((original_url, final_url, article_title, article_text, detected_language))
                    logging.info(f"✅ Successfully extracted article from {original_url}")
                else:
                    logging.warning(f"⚠ Article text too short for {original_url} ({len(article_text) if article_text else 0} chars)")
            else:
                logging.warning(f"⚠ No article text extracted for {original_url}")
        
        all_results.extend(successful_results)
        
        # Pause between batches
        if i + batch_size < len(valid_urls):
            time.sleep(2)
    
    # Create a mapping from URL to row in dataframe for looking up metadata
    url_to_row = {}
    for idx, url in enumerate(df["Link"]):
        if url and isinstance(url, str):
            url_to_row[url.strip()] = idx
    
    extracted_data = []
    
    for original_url, final_url, article_title, article_text, detected_language in all_results:
        if not article_text:
            continue
        
        # Improved URL matching to handle redirects better
        matched_original = match_final_url_to_original(final_url, [original_url] + list(url_to_row.keys()))
        
        if not matched_original:
            # If we can't find a good match, use the original URL but log it
            matched_original = original_url
            logging.info(f"🔄 Using original URL as fallback for final URL {final_url}")
        elif matched_original != original_url:
            logging.info(f"🔄 Matched final URL {final_url} to original {matched_original}")
        
        # Get the corresponding row from the dataframe
        df_idx = url_to_row.get(matched_original)
        if df_idx is not None and df_idx < len(df):
            row = df.iloc[df_idx].to_dict()
        else:
            # Create a minimal row if we can't find it
            row = {"Title": "", "Source": "", "Summary": "", "Date": "", "Term": "", "LanguageQueried": ""}
        
        # Create a unique ID for the article based on normalized URL and title
        normalized_url = normalize_url(final_url)
        article_id = hashlib.md5((normalized_url + (article_title or "")).encode()).hexdigest()
        
        item = {
            "id": article_id,
            "title": row.get("Title", "") or (article_title or ""),
            "final_url": final_url,
            "original_url": matched_original,
            "normalized_url": normalized_url,
            "article_text": article_text,
            "article_language": detected_language,
            "other_info": {
                "csv_title": row.get("Title", ""),
                "csv_source": row.get("Source", ""),
                "csv_summary": row.get("Summary", ""),
                "csv_date": row.get("Date", ""),
                "csv_term": row.get("Term", ""),
                "csv_language_queried": row.get("LanguageQueried", "")
            },
            "state": region_name,
            "disaster_type": disaster_type,
            "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "extraction_quality": assess_extraction_quality(article_text)
        }
        
        extracted_data.append(item)
    
    logging.info(f"✅ Extracted {len(extracted_data)}/{len(valid_urls)} articles from {csv_path}")
    return extracted_data

def assess_extraction_quality(text):
    """
    Assess the quality of the extracted article text.
    Returns: "high", "medium", or "low"
    """
    if not text:
        return "low"
        
    # Calculate text length and number of paragraphs
    text_length = len(text)
    paragraphs = text.split('\n')
    num_paragraphs = len([p for p in paragraphs if len(p.strip()) > 20])
    
    # Check for HTML artifacts that might indicate poor extraction
    html_artifacts = re.findall(r'<[^>]+>', text)
    has_html = len(html_artifacts) > 0
    
    # Look for common patterns that indicate poor extraction
    poor_extraction_patterns = [
        r'cookie', r'privacy policy', r'terms of service', 
        r'subscribe', r'sign up', r'log in', r'login',
        r'advertisement', r'click here', r'read more'
    ]
    
    num_poor_patterns = sum(1 for pattern in poor_extraction_patterns 
                           if re.search(pattern, text.lower()))
    
    # Check for repetitive content (could indicate extraction errors)
    words = text.split()
    unique_words = set(words)
    word_repetition_ratio = len(unique_words) / len(words) if words else 0
    
    # Assess quality with more nuanced criteria
    if (text_length > 1500 and num_paragraphs >= 3 and not has_html and 
        num_poor_patterns <= 1 and word_repetition_ratio > 0.3):
        return "high"
    elif (text_length > 500 and num_paragraphs >= 2 and num_poor_patterns <= 2 and 
          word_repetition_ratio > 0.2):
        return "medium"
    else:
        return "low"

def get_all_csvs_for_today(base_dir="data", days_range=0):
    """
    Get all CSV files for today and optionally for a specific range of previous days.
    
    Args:
        base_dir (str): Base directory to search
        days_range (int): Number of previous days to include (0 = today only)
    
    Returns:
        List of CSV paths with their region info
    """
    # Get date components for the target range
    today = datetime.now()
    target_dates = []
    
    for i in range(days_range + 1):
        target_date = today - timedelta(days=i)
        target_dates.append({
            'year': str(target_date.year),
            'month': target_date.strftime("%m"),
            'day': target_date.strftime("%d")
        })
    
    csv_files = []
    
    for root, dirs, files in os.walk(base_dir):
        # Check if "results.csv" is in this directory
        if "results.csv" not in files:
            continue
            
        csv_path = os.path.join(root, "results.csv")
        
        # Path pattern: data/states/<STATE>/Monsoon/<YEAR>/<MONTH>/<DAY>/results.csv
        path_parts = csv_path.split(os.sep)
        
        # Ensure we have enough parts
        if len(path_parts) < 8:
            logging.warning(f"⚠ Path format not recognized: {csv_path}")
            continue
            
        region_type = path_parts[1]  # "states" or "union-territories"
        region_name = path_parts[2]  # e.g. "andhra-pradesh"
        disaster_type = path_parts[3]  # e.g. "Monsoon" or "Heatwave"
        folder_year = path_parts[4]
        folder_month = path_parts[5]
        folder_day = path_parts[6]
        
        # Check if this CSV is for one of our target dates
        for date_info in target_dates:
            if (folder_year == date_info['year'] and
                folder_month == date_info['month'] and
                folder_day == date_info['day']):
                
                csv_files.append({
                    'path': csv_path,
                    'region_type': region_type,
                    'region_name': region_name,
                    'disaster_type': disaster_type
                })
                break
    
    return csv_files

def smart_remove_duplicates(all_data):
    """
    Remove duplicate articles with intelligent duplicate detection.
    Uses multiple strategies: URL normalization, content similarity, and domain clustering.
    """
    if not all_data:
        return []
    
    logging.info(f"🔄 Starting deduplication of {len(all_data)} articles")
    
    # Strategy 1: Remove exact URL duplicates (after normalization)
    url_map = {}
    url_deduplicated = []
    
    for item in all_data:
        normalized_url = item.get('normalized_url') or normalize_url(item.get('final_url', ''))
        
        if not normalized_url:
            url_deduplicated.append(item)
            continue
            
        url_hash = hashlib.md5(normalized_url.encode()).hexdigest()
        
        if url_hash not in url_map:
            url_map[url_hash] = item
            url_deduplicated.append(item)
        else:
            # Keep the one with better quality
            existing_quality = url_map[url_hash].get('extraction_quality', 'low')
            current_quality = item.get('extraction_quality', 'low')
            
            quality_order = {'high': 3, 'medium': 2, 'low': 1}
            if quality_order.get(current_quality, 1) > quality_order.get(existing_quality, 1):
                # Replace with better quality version
                url_map[url_hash] = item
                url_deduplicated = [x for x in url_deduplicated if x != url_map[url_hash]]
                url_deduplicated.append(item)
    
    logging.info(f"🔄 Removed {len(all_data) - len(url_deduplicated)} URL duplicates")
    
    # Strategy 2: Remove content duplicates using text similarity
    content_map = {}
    content_deduplicated = []
    
    for item in url_deduplicated:
        text = item.get('article_text', '')
        if not text or len(text) < 100:
            content_deduplicated.append(item)
            continue
        
        # Create content fingerprint using first and last parts + length
        text_clean = re.sub(r'\s+', ' ', text.lower()).strip()
        
        # Use first 500 chars + last 200 chars + length for fingerprint
        start_text = text_clean[:500]
        end_text = text_clean[-200:] if len(text_clean) > 200 else ""
        length_bucket = str(len(text_clean) // 100)  # Group by length buckets
        
        fingerprint = f"{start_text}||{end_text}||{length_bucket}"
        content_hash = hashlib.md5(fingerprint.encode()).hexdigest()
        
        if content_hash not in content_map:
            content_map[content_hash] = item
            content_deduplicated.append(item)
        else:
            # Keep the one with better quality or more recent timestamp
            existing = content_map[content_hash]
            existing_quality = existing.get('extraction_quality', 'low')
            current_quality = item.get('extraction_quality', 'low')
            
            quality_order = {'high': 3, 'medium': 2, 'low': 1}
            if quality_order.get(current_quality, 1) > quality_order.get(existing_quality, 1):
                content_map[content_hash] = item
                content_deduplicated = [x for x in content_deduplicated if x != existing]
                content_deduplicated.append(item)
    
    logging.info(f"🔄 Removed {len(url_deduplicated) - len(content_deduplicated)} content duplicates")
    
    # Strategy 3: Remove very similar titles from same domain
    domain_title_map = {}
    final_deduplicated = []
    
    for item in content_deduplicated:
        domain = extract_domain(item.get('final_url', ''))
        title = item.get('title', '').lower().strip()
        
        if not domain or not title:
            final_deduplicated.append(item)
            continue
        
        # Create a simplified title (remove common words and normalize)
        title_words = re.findall(r'\w+', title)
        # Remove common words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were'}
        significant_words = [w for w in title_words if len(w) > 3 and w not in stop_words]
        title_key = ' '.join(sorted(significant_words[:5]))  # Use first 5 significant words
        
        domain_title_key = f"{domain}::{title_key}"
        
        if domain_title_key not in domain_title_map:
            domain_title_map[domain_title_key] = item
            final_deduplicated.append(item)
        else:
            # Keep the one with longer text content
            existing = domain_title_map[domain_title_key]
            existing_length = len(existing.get('article_text', ''))
            current_length = len(item.get('article_text', ''))
            
            if current_length > existing_length:
                domain_title_map[domain_title_key] = item
                final_deduplicated = [x for x in final_deduplicated if x != existing]
                final_deduplicated.append(item)
    
    logging.info(f"🔄 Removed {len(content_deduplicated) - len(final_deduplicated)} title duplicates")
    logging.info(f"🔄 Final result: {len(final_deduplicated)}/{len(all_data)} unique articles ({(len(final_deduplicated)/len(all_data)*100):.1f}%)")
    
    return final_deduplicated

def create_language_statistics(all_data):
    """
    Create statistics about languages in extracted articles.
    """
    if not all_data:
        return {
            'total_articles': 0,
            'language_distribution': {}
        }
        
    language_counts = {}
    total_articles = len(all_data)
    
    for item in all_data:
        lang = item.get('article_language', 'unknown')
        if lang in language_counts:
            language_counts[lang] += 1
        else:
            language_counts[lang] = 1
    
    # Convert to percentages
    language_stats = {
        'total_articles': total_articles,
        'language_distribution': {
            lang: {
                'count': count,
                'percentage': round(count / total_articles * 100, 2)
            }
            for lang, count in language_counts.items()
        }
    }
    
    return language_stats

def save_results(final_data, language_stats):
    """
    Save extracted articles and statistics to organized JSON folders by date.
    """
    # Get today's date for folder organization
    today = datetime.now()
    date_str = today.strftime('%Y-%m-%d')
    
    # Create main output folders
    main_output_dir = f"JSON Output/{date_str}"
    spare_output_dir = f"JSON Output Spare/{date_str}"
    
    os.makedirs(main_output_dir, exist_ok=True)
    os.makedirs(spare_output_dir, exist_ok=True)
    
    # Separate articles by quality
    high_quality = [item for item in final_data if item.get('extraction_quality') == 'high']
    medium_quality = [item for item in final_data if item.get('extraction_quality') == 'medium']
    low_quality = [item for item in final_data if item.get('extraction_quality') == 'low']
    
    # MAIN OUTPUT FOLDER: Combined high and medium quality articles
    combined_quality_data = high_quality + medium_quality
    
    if combined_quality_data:
        main_output_file = os.path.join(main_output_dir, "articles_combined.json")
        with open(main_output_file, "w", encoding="utf-8") as f:
            json.dump(combined_quality_data, f, ensure_ascii=False, indent=2)
        logging.info(f"💾 Combined high+medium quality articles saved to {main_output_file}")
    
    # SPARE OUTPUT FOLDER: Everything else
    
    # 1. All articles (complete dataset)
    all_output_file = os.path.join(spare_output_dir, "articles_all.json")
    with open(all_output_file, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    # 2. High quality only
    if high_quality:
        high_output_file = os.path.join(spare_output_dir, "articles_high_quality.json")
        with open(high_output_file, "w", encoding="utf-8") as f:
            json.dump(high_quality, f, ensure_ascii=False, indent=2)
    
    # 3. Medium quality only
    if medium_quality:
        medium_output_file = os.path.join(spare_output_dir, "articles_medium_quality.json")
        with open(medium_output_file, "w", encoding="utf-8") as f:
            json.dump(medium_quality, f, ensure_ascii=False, indent=2)
    
    # 4. Low quality only
    if low_quality:
        low_output_file = os.path.join(spare_output_dir, "articles_low_quality.json")
        with open(low_output_file, "w", encoding="utf-8") as f:
            json.dump(low_quality, f, ensure_ascii=False, indent=2)
    
    # 5. Statistics with more detailed breakdown
    stats = {
        'extraction_date': date_str,
        'total_articles': len(final_data),
        'language_stats': language_stats,
        'quality_breakdown': {
            'high': len(high_quality),
            'medium': len(medium_quality),
            'low': len(low_quality)
        },
        'region_breakdown': {},
        'disaster_breakdown': {},
        'extraction_timestamp': today.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Add region and disaster breakdowns
    for item in final_data:
        region = item.get('state', 'unknown')
        disaster = item.get('disaster_type', 'unknown')
        
        if region not in stats['region_breakdown']:
            stats['region_breakdown'][region] = 0
        stats['region_breakdown'][region] += 1
        
        if disaster not in stats['disaster_breakdown']:
            stats['disaster_breakdown'][disaster] = 0
        stats['disaster_breakdown'][disaster] += 1
    
    stats_output_file = os.path.join(spare_output_dir, "extraction_stats.json")
    with open(stats_output_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    logging.info(f"📁 JSON Output organized by date:")
    logging.info(f"   📂 Main: {main_output_dir}/articles_combined.json ({len(combined_quality_data)} articles)")
    logging.info(f"   📂 Spare: {spare_output_dir}/ (all files including stats)")
    logging.info(f"📊 Quality breakdown: {len(high_quality)} high, {len(medium_quality)} medium, {len(low_quality)} low")
    
    return {
        'main_folder': main_output_dir,
        'spare_folder': spare_output_dir,
        'combined_file': os.path.join(main_output_dir, "articles_combined.json") if combined_quality_data else None,
        'stats_file': stats_output_file
    }

def main():
    """
    Main function to extract articles from CSV files with improved approach.
    """
    logging.info("🔄 Starting article extraction process with improved duplicate handling")
    
    # Initialize the article scraper with improved settings
    scraper = ArticleScraper(parallelism=1, process_timeout=60)
    
    try:
        # Get all CSV files for today (default) or with optional days range
        days_range = 0  # Set to 0 for strict today-only filtering
        csv_files = get_all_csvs_for_today(days_range=days_range)
        
        logging.info(f"📂 Found {len(csv_files)} CSV files to process")
        
        all_extracted = []
        stats = {
            'total_csvs_processed': 0,
            'total_articles_extracted': 0,
            'extraction_failures': 0,
            'by_region': {},
            'by_disaster': {}
        }
        
        # Process each CSV file with improved extraction
        for csv_info in csv_files:
            csv_path = csv_info['path']
            region_type = csv_info['region_type']
            region_name = csv_info['region_name']
            disaster_type = csv_info['disaster_type']
            
            # Create a readable region name for logging
            readable_region = region_name.replace('-', ' ').title()
            
            logging.info(f"\n=== Processing {readable_region} ({disaster_type}) ===")
            
            # Extract articles with improved error handling
            data_rows = extract_articles_from_csv(csv_path, scraper, region_name, disaster_type)
            
            # Update stats
            stats['total_csvs_processed'] += 1
            stats['total_articles_extracted'] += len(data_rows)
            
            # Update region stats
            if region_name not in stats['by_region']:
                stats['by_region'][region_name] = 0
            stats['by_region'][region_name] += len(data_rows)
            
            # Update disaster stats
            if disaster_type not in stats['by_disaster']:
                stats['by_disaster'][disaster_type] = 0
            stats['by_disaster'][disaster_type] += len(data_rows)
            
            all_extracted.extend(data_rows)
            
            # Add a brief pause between regions
            time.sleep(2)
        
        # Clean up the scraper
        scraper.quit()
        
        # Use smart deduplication
        logging.info(f"\n🔄 Applying intelligent deduplication to {len(all_extracted)} articles")
        final_data = smart_remove_duplicates(all_extracted)
        
        # Only proceed with stats if we have data
        if final_data:
            # Calculate language statistics
            language_stats = create_language_statistics(final_data)
            
            # Save files in organized folders
            output_info = save_results(final_data, language_stats)
            
            # Log completion message with statistics
            logging.info(f"\n✅ Article extraction complete")
            logging.info(f"📊 Processed {stats['total_csvs_processed']} CSV files")
            logging.info(f"📊 Extracted {len(final_data)} unique articles")
            
            high_count = len([item for item in final_data if item.get('extraction_quality') == 'high'])
            medium_count = len([item for item in final_data if item.get('extraction_quality') == 'medium'])
            low_count = len([item for item in final_data if item.get('extraction_quality') == 'low'])
            
            logging.info(f"📊 Quality breakdown: {high_count} high, {medium_count} medium, {low_count} low")
            
            if language_stats and 'language_distribution' in language_stats:
                total_languages = len(language_stats['language_distribution'])
                logging.info(f"📊 Articles in {total_languages} different languages")
                
                top_languages = sorted(
                    language_stats['language_distribution'].items(), 
                    key=lambda x: x[1]['count'], 
                    reverse=True
                )[:3]
                
                if top_languages:
                    lang_info = ", ".join([f"{lang}: {info['count']} ({info['percentage']}%)" 
                                        for lang, info in top_languages])
                    logging.info(f"📊 Top languages: {lang_info}")
                    
            # Log region breakdown
            if stats['by_region']:
                logging.info(f"📊 Articles by region: {dict(sorted(stats['by_region'].items(), key=lambda x: x[1], reverse=True))}")
                
        else:
            logging.warning("⚠ No articles were successfully extracted")
            
    except Exception as e:
        logging.error(f"Error in article extraction process: {e}", exc_info=True)
        
if __name__ == "__main__":
    main()