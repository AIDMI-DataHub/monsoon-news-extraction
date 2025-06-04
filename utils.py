# utils.py - Monsoon Folder Creation
import os
from datetime import datetime
import calendar

def create_folders():
    """Create folder structure for monsoon news data storage"""
    base_path = 'data'
    
    # Indian states
    states = [
        "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", 
        "chhattisgarh", "goa", "gujarat", "haryana", "himachal-pradesh", 
        "jharkhand", "karnataka", "kerala", "madhya-pradesh", "maharashtra", 
        "manipur", "meghalaya", "mizoram", "nagaland", "odisha", "punjab", 
        "rajasthan", "sikkim", "tamil-nadu", "telangana", "tripura", 
        "uttar-pradesh", "uttarakhand", "west-bengal"
    ]
    
    # Union territories
    union_territories = [
        "andaman-and-nicobar-islands", "chandigarh", 
        "dadra-and-nagar-haveli-and-daman-and-diu", "lakshadweep", "delhi", 
        "puducherry", "jammu-and-kashmir", "ladakh"
    ]
    
    # Only create Monsoon folders
    climate_events = ["Monsoon"]
    year = datetime.now().year
    
    print(f"📁 Creating folder structure for {year}...")
    
    # Create subfolders for each state/UT, Monsoon event, year, month, day
    folder_count = 0
    
    for state in states:
        for event in climate_events:
            for month in range(1, 13):
                days_in_month = calendar.monthrange(year, month)[1]
                for day in range(1, days_in_month + 1):
                    folder_path = f"{base_path}/states/{state}/{event}/{year}/{month:02d}/{day:02d}"
                    os.makedirs(folder_path, exist_ok=True)
                    folder_count += 1
    
    for ut in union_territories:
        for event in climate_events:
            for month in range(1, 13):
                days_in_month = calendar.monthrange(year, month)[1]
                for day in range(1, days_in_month + 1):
                    folder_path = f"{base_path}/union-territories/{ut}/{event}/{year}/{month:02d}/{day:02d}"
                    os.makedirs(folder_path, exist_ok=True)
                    folder_count += 1
    
    # Create national folder structure
    for event in climate_events:
        for month in range(1, 13):
            days_in_month = calendar.monthrange(year, month)[1]
            for day in range(1, days_in_month + 1):
                folder_path = f"{base_path}/national/all/{event}/{year}/{month:02d}/{day:02d}"
                os.makedirs(folder_path, exist_ok=True)
                folder_count += 1
    
    # Create JSON output folders
    json_output_dirs = ["JSON Output", "JSON Output Spare"]
    for json_dir in json_output_dirs:
        os.makedirs(json_dir, exist_ok=True)
    
    print(f"✅ Created {folder_count} monsoon data folders")
    print(f"✅ Created JSON output directories")
    print(f"📊 Structure: {len(states)} states + {len(union_territories)} UTs + national")

if __name__ == "__main__":
    create_folders()