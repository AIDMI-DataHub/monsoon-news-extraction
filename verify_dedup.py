#!/usr/bin/env python3
"""
Deduplication Verification Script
Analyzes the deduplication results to ensure quality and accuracy
"""

import json
import os
from collections import defaultdict
from difflib import SequenceMatcher
import re

def similarity(a, b):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a, b).ratio()

def analyze_deduplication_results():
    """Analyze the deduplication results from your pipeline"""
    
    print("🔍 Analyzing Deduplication Results...")
    print("=" * 60)
    
    # Find the most recent results
    base_dirs = ["JSON Output", "JSON Output Spare"]
    
    for base_dir in base_dirs:
        if not os.path.exists(base_dir):
            print(f"❌ Directory {base_dir} not found")
            continue
            
        print(f"\n📂 Analyzing {base_dir}/")
        
        # Find date folders
        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and item.startswith("2025"):
                print(f"   📅 Found date folder: {item}")
                
                # Look for JSON files
                for file in os.listdir(item_path):
                    if file.endswith('.json'):
                        file_path = os.path.join(item_path, file)
                        analyze_json_file(file_path)

def analyze_json_file(file_path):
    """Analyze a specific JSON file for deduplication quality"""
    
    print(f"\n📋 Analyzing: {os.path.basename(file_path)}")
    print("-" * 40)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print("❌ JSON file is not a list of articles")
            return
            
        articles = data
        print(f"📊 Total articles in file: {len(articles)}")
        
        # 1. Check for URL duplicates
        urls = [article.get('final_url', '') for article in articles]
        url_duplicates = find_duplicates(urls)
        
        if url_duplicates:
            print(f"⚠️  Found {len(url_duplicates)} URL duplicates:")
            for url, count in url_duplicates.items():
                print(f"   🔗 {url} (appears {count} times)")
        else:
            print("✅ No URL duplicates found")
        
        # 2. Check for title duplicates
        titles = [article.get('title', '') for article in articles if article.get('title')]
        title_duplicates = find_duplicates(titles)
        
        if title_duplicates:
            print(f"⚠️  Found {len(title_duplicates)} exact title duplicates:")
            for title, count in title_duplicates.items():
                print(f"   📰 '{title[:60]}...' (appears {count} times)")
        else:
            print("✅ No exact title duplicates found")
        
        # 3. Check for similar titles (potential false negatives)
        check_similar_titles(articles)
        
        # 4. Check content similarity
        check_content_similarity(articles)
        
        # 5. Language distribution
        check_language_distribution(articles)
        
        # 6. Quality distribution
        check_quality_distribution(articles)
        
    except Exception as e:
        print(f"❌ Error analyzing {file_path}: {e}")

def find_duplicates(items):
    """Find exact duplicates in a list"""
    counts = defaultdict(int)
    for item in items:
        if item:  # Skip empty items
            counts[item] += 1
    
    return {item: count for item, count in counts.items() if count > 1}

def check_similar_titles(articles):
    """Check for titles that are very similar but not exact duplicates"""
    
    print("\n🔍 Checking for similar titles...")
    
    titles_with_index = [(i, article.get('title', '')) for i, article in enumerate(articles) if article.get('title')]
    
    similar_pairs = []
    
    for i in range(len(titles_with_index)):
        for j in range(i + 1, len(titles_with_index)):
            idx1, title1 = titles_with_index[i]
            idx2, title2 = titles_with_index[j]
            
            # Calculate similarity
            sim = similarity(title1.lower(), title2.lower())
            
            # If very similar but not identical, it might be a missed duplicate
            if 0.8 <= sim < 1.0:
                similar_pairs.append((sim, idx1, idx2, title1, title2))
    
    if similar_pairs:
        print(f"⚠️  Found {len(similar_pairs)} pairs of very similar titles:")
        for sim, idx1, idx2, title1, title2 in sorted(similar_pairs, reverse=True):
            print(f"   📊 Similarity: {sim:.2f}")
            print(f"   📰 Article {idx1}: '{title1[:50]}...'")
            print(f"   📰 Article {idx2}: '{title2[:50]}...'")
            print(f"   🔗 URLs: {articles[idx1].get('final_url', 'N/A')}")
            print(f"   🔗 URLs: {articles[idx2].get('final_url', 'N/A')}")
            print()
    else:
        print("✅ No highly similar titles found")

def check_content_similarity(articles):
    """Check for content similarity (sample check)"""
    
    print("\n🔍 Checking content similarity (sample)...")
    
    # Only check first few articles to avoid performance issues
    sample_size = min(10, len(articles))
    sample_articles = articles[:sample_size]
    
    content_similarities = []
    
    for i in range(len(sample_articles)):
        for j in range(i + 1, len(sample_articles)):
            content1 = sample_articles[i].get('content', '')
            content2 = sample_articles[j].get('content', '')
            
            if content1 and content2:
                # Take first 500 chars for comparison
                content1_sample = content1[:500]
                content2_sample = content2[:500]
                
                sim = similarity(content1_sample.lower(), content2_sample.lower())
                
                if sim > 0.7:  # High similarity threshold
                    content_similarities.append((sim, i, j))
    
    if content_similarities:
        print(f"⚠️  Found {len(content_similarities)} pairs with high content similarity:")
        for sim, i, j in content_similarities:
            print(f"   📊 Similarity: {sim:.2f} between articles {i} and {j}")
            print(f"   📰 Title 1: '{sample_articles[i].get('title', 'N/A')[:50]}...'")
            print(f"   📰 Title 2: '{sample_articles[j].get('title', 'N/A')[:50]}...'")
    else:
        print(f"✅ No high content similarity found (checked {sample_size} articles)")

def check_language_distribution(articles):
    """Check language distribution"""
    
    print("\n🌐 Language Distribution:")
    
    languages = defaultdict(int)
    for article in articles:
        lang = article.get('language', 'unknown')
        languages[lang] += 1
    
    total = len(articles)
    for lang, count in sorted(languages.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total) * 100
        print(f"   🗣️  {lang}: {count} articles ({percentage:.1f}%)")

def check_quality_distribution(articles):
    """Check quality distribution if available"""
    
    print("\n⭐ Quality Distribution:")
    
    qualities = defaultdict(int)
    for article in articles:
        quality = article.get('quality', 'unknown')
        qualities[quality] += 1
    
    total = len(articles)
    for quality, count in sorted(qualities.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total) * 100
        print(f"   ⭐ {quality}: {count} articles ({percentage:.1f}%)")

def generate_dedup_report():
    """Generate a comprehensive deduplication report"""
    
    print("\n📋 DEDUPLICATION QUALITY REPORT")
    print("=" * 60)
    
    # Look for the stats file in JSON Output Spare
    stats_files = []
    for root, dirs, files in os.walk("JSON Output Spare"):
        for file in files:
            if "stats" in file.lower() and file.endswith('.json'):
                stats_files.append(os.path.join(root, file))
    
    if stats_files:
        latest_stats = max(stats_files, key=os.path.getctime)
        print(f"📊 Found stats file: {latest_stats}")
        
        try:
            with open(latest_stats, 'r', encoding='utf-8') as f:
                stats = json.load(f)
            
            print("\n📈 Deduplication Statistics:")
            if 'deduplication' in stats:
                dedup = stats['deduplication']
                print(f"   📊 Original articles: {dedup.get('original_count', 'N/A')}")
                print(f"   📊 After deduplication: {dedup.get('final_count', 'N/A')}")
                print(f"   📊 Removal rate: {dedup.get('removal_percentage', 'N/A')}%")
                print(f"   🔗 URL duplicates removed: {dedup.get('url_duplicates_removed', 'N/A')}")
                print(f"   📰 Title duplicates removed: {dedup.get('title_duplicates_removed', 'N/A')}")
                print(f"   📝 Content duplicates removed: {dedup.get('content_duplicates_removed', 'N/A')}")
        
        except Exception as e:
            print(f"❌ Error reading stats: {e}")
    else:
        print("❌ No stats file found")

if __name__ == "__main__":
    print("🔍 DEDUPLICATION VERIFICATION TOOL")
    print("=" * 60)
    print("This tool helps verify that deduplication worked correctly")
    print("and identifies any potential issues.\n")
    
    # Run the analysis
    analyze_deduplication_results()
    
    # Generate report
    generate_dedup_report()
    
    print("\n" + "=" * 60)
    print("✅ Analysis complete!")
    print("\n💡 How to interpret results:")
    print("   ✅ Green checkmarks = Good, no issues found")
    print("   ⚠️  Yellow warnings = Potential issues to review")
    print("   ❌ Red X's = Problems that need attention")
    print("\n📋 If you see warnings, manually review those articles")
    print("   to determine if they're truly duplicates or unique content.")