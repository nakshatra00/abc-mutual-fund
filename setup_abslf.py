#!/usr/bin/env python3
"""
Quick setup and test for ABSLF scraper.
"""
import subprocess
import sys

def install_dependencies():
    """Install required dependencies."""
    dependencies = ['requests', 'beautifulsoup4', 'pyyaml']
    
    print("📦 Installing dependencies...")
    for dep in dependencies:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', dep])
            print(f"✅ Installed {dep}")
        except subprocess.CalledProcessError:
            print(f"❌ Failed to install {dep}")
            return False
    
    return True

def test_imports():
    """Test if required modules can be imported."""
    modules = {
        'requests': 'HTTP requests',
        'bs4': 'HTML parsing',
        'yaml': 'YAML configuration'
    }
    
    print("\n🧪 Testing imports...")
    for module, description in modules.items():
        try:
            __import__(module)
            print(f"✅ {module} - {description}")
        except ImportError:
            print(f"❌ {module} - {description} (FAILED)")
            return False
    
    return True

def main():
    print("=== ABSLF Scraper Quick Setup ===\n")
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Failed to install dependencies")
        return False
    
    # Test imports
    if not test_imports():
        print("❌ Import test failed")
        return False
    
    print("\n🎉 Setup complete!")
    print("\n🚀 Usage examples:")
    print("# Discover files only:")
    print("python simple_abslf_scraper.py --discover-only")
    print()
    print("# Download all files:")
    print("python simple_abslf_scraper.py")
    print()
    print("# Download only corporate bond files:")
    print("python simple_abslf_scraper.py --filter 'corporate'")
    print()
    print("# Download max 3 files:")
    print("python simple_abslf_scraper.py --max-files 3")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)