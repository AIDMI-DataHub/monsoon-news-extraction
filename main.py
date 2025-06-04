# main.py - Monsoon News Extraction Pipeline
import subprocess
import argparse
import sys
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Monsoon news extraction pipeline for climate impact monitoring')
    parser.add_argument('--date', type=str, 
                      help='Target date in YYYY-MM-DD format (default: current date)')
    parser.add_argument('--days-back', type=int, default=0, 
                      help='Number of days to look back from target date (default: 0)')
    parser.add_argument('--state', type=str, 
                      help='Process only this single state/UT (e.g., kerala, maharashtra, delhi)')
    parser.add_argument('--skip-folders', action='store_true', 
                      help='Skip folder creation step')
    parser.add_argument('--skip-extraction', action='store_true', 
                      help='Skip article content extraction step')
    
    args = parser.parse_args()
    
    print("🌧️ Starting Monsoon News Extraction Pipeline")
    print(f"📅 Target date: {args.date if args.date else 'Current date'}")
    print(f"📅 Days back: {args.days_back}")
    if args.state:
        print(f"🎯 Single state mode: {args.state}")
    
    # Validate date format if provided
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD")
            sys.exit(1)
    
    try:
        # Step 1: Create required folders (unless skipped)
        if not args.skip_folders:
            print("\n📁 Step 1: Creating folder structure...")
            subprocess.run(["python", "utils.py"], check=True)
            print("✅ Folder structure created")
        else:
            print("\n📁 Step 1: Skipping folder creation")
        
        # Step 2: Run monsoon news collection
        print(f"\n🌧️ Step 2: Running monsoon news collection...")
        cmd = ["python", "monsoon.py"]
        
        # Add date parameter if provided
        if args.date:
            cmd.extend(["--date", args.date])
        
        # Add days-back parameter if provided
        if args.days_back > 0:
            cmd.extend(["--days-back", str(args.days_back)])
        
        # Add state parameter if provided
        if args.state:
            cmd.extend(["--state", args.state])
        
        subprocess.run(cmd, check=True)
        print("✅ Monsoon news collection completed")
        
        # Step 3: Extract full articles (unless skipped)
        if not args.skip_extraction:
            print(f"\n📰 Step 3: Extracting full article content...")
            subprocess.run(["python", "extract_articles.py"], check=True)
            print("✅ Article content extraction completed")
        else:
            print("\n📰 Step 3: Skipping article content extraction")
        
        print(f"\n🎉 Monsoon pipeline completed successfully!")
        
        # Show output locations
        if not args.skip_extraction:
            today = datetime.now().strftime('%Y-%m-%d')
            print(f"\n📁 Output locations:")
            print(f"   📂 Daily articles: JSON Output/{today}/articles_combined.json")
            print(f"   📂 Detailed data: JSON Output Spare/{today}/")
            print(f"   📂 CSV data: data/[states|union-territories]/[region]/Monsoon/")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running subprocess: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()