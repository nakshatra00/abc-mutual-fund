#!/usr/bin/env python3
"""
Example usage of the ABSLF scraper
"""
from ingestion.scrape_abslf import scrape_abslf, discover_files


def demo_scraper():
    """Demonstrate different ways to use the scraper."""
    
    print("🧪 ABSLF Scraper Demo\n")
    
    # 1. Just discover what's available
    print("1️⃣ Discovering available monthly files...")
    monthly_files = discover_files("monthly")
    print(f"Found {len(monthly_files)} monthly files\n")
    
    # 2. Discover fortnightly files
    print("2️⃣ Discovering available fortnightly files...")
    fortnightly_files = discover_files("fortnightly")
    print(f"Found {len(fortnightly_files)} fortnightly files\n")
    
    # 3. Download limited number of files
    print("3️⃣ Downloading 1 monthly file...")
    result = scrape_abslf(
        disclosure_type="monthly",
        max_files=1,
        output_dir="demo_output"
    )
    
    if result['success']:
        print(f"✅ Downloaded {result['files_downloaded']} files")
        print(f"📁 Files saved to: {result['output_directory']}")
        print(f"📋 Files: {result['downloaded_files']}")
    else:
        print(f"❌ Download failed: {result.get('message')}")
    
    return result


if __name__ == "__main__":
    demo_scraper()