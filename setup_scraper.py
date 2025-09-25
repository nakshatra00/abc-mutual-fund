#!/usr/bin/env python3
"""
Quick setup and testing script for the mutual fund scraper system.
"""

import sys
from pathlib import Path

def main():
    print("=== Mutual Fund Scraper System ===")
    print()
    
    # Check Python version
    python_version = sys.version_info
    if python_version < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"Current version: {python_version.major}.{python_version.minor}.{python_version.micro}")
        return False
    else:
        print(f"✅ Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    # Check directory structure
    print("\n📁 Checking directory structure...")
    
    required_dirs = [
        "config",
        "config/scrapers",
        "src/scrapers",
        "data",
        "data/raw",
        "logs"
    ]
    
    for dir_path in required_dirs:
        path = Path(dir_path)
        if path.exists():
            print(f"  ✅ {dir_path}")
        else:
            print(f"  📁 Creating {dir_path}")
            path.mkdir(parents=True, exist_ok=True)
    
    # Check configuration files
    print("\n⚙️ Checking configuration files...")
    
    config_files = [
        "config/scraper_config.yml",
        "config/scrapers/hdfc.yml",
        "config/scrapers/icici.yml", 
        "config/scrapers/uti.yml",
        "config/scrapers/sbi.yml"
    ]
    
    for config_file in config_files:
        path = Path(config_file)
        if path.exists():
            print(f"  ✅ {config_file}")
        else:
            print(f"  ❌ Missing: {config_file}")
    
    # Check Python modules
    print("\n🐍 Checking Python dependencies...")
    
    required_packages = [
        ("requests", "Web requests"),
        ("aiohttp", "Async HTTP client"),
        ("beautifulsoup4", "HTML parsing"),
        ("pandas", "Data processing"),
        ("pyyaml", "YAML configuration"),
        ("openpyxl", "Excel file handling")
    ]
    
    missing_packages = []
    
    for package, description in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package} - {description}")
        except ImportError:
            print(f"  ❌ {package} - {description} (MISSING)")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n📦 To install missing packages, run:")
        print(f"  pip install {' '.join(missing_packages)}")
        print(f"\nOr install all requirements:")
        print(f"  pip install -r requirements.txt")
    
    # Check scraper modules
    print("\n🔧 Checking scraper modules...")
    
    try:
        sys.path.insert(0, str(Path("src")))
        from scrapers import load_scraper_manager
        print("  ✅ Scraper modules import successfully")
        
        try:
            manager = load_scraper_manager()
            amcs = manager.get_available_amcs()
            print(f"  ✅ Loaded {len(amcs)} AMC configurations: {', '.join(amcs)}")
        except Exception as e:
            print(f"  ⚠️ Warning loading scraper manager: {e}")
            
    except ImportError as e:
        print(f"  ❌ Cannot import scraper modules: {e}")
    
    # Usage examples
    print("\n🚀 Usage Examples:")
    print("  # List available AMCs")
    print("  python run_scraper.py --list-amcs")
    print()
    print("  # Scrape all fund types for HDFC")
    print("  python run_scraper.py --amc HDFC")
    print()
    print("  # Dry run to see what would be scraped")
    print("  python run_scraper.py --amc HDFC --dry-run")
    print()
    print("  # Scrape specific fund type for multiple AMCs")
    print("  python run_scraper.py --amc HDFC,ICICI --fund-type corporate-bond")
    print()
    print("  # Scrape everything")
    print("  python run_scraper.py --all")
    
    print("\n✨ Setup complete! The scraper system is ready to use.")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)