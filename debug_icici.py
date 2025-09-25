#!/usr/bin/env python3
"""
Debug ICICI website structure
"""
import requests
from bs4 import BeautifulSoup
import re

def debug_icici_page():
    """Debug the ICICI downloads page to understand its structure."""
    
    url = "https://www.icicipruamc.com/media-center/downloads?currentTabFilter=OtherSchemeDisclosures&&subCatTabFilter=Fortnightly%20Portfolio%20Disclosures"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    print(f"🔍 Fetching ICICI page: {url}")
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        print(f"✅ Page fetched successfully, status: {response.status_code}")
        print(f"📄 Content length: {len(response.content)} bytes")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for text containing "September" 
        print("\n🔍 Looking for September mentions:")
        september_texts = soup.find_all(string=re.compile(r'september', re.IGNORECASE))
        
        for i, text in enumerate(september_texts[:10]):  # Show first 10
            print(f"  {i+1}. {text.strip()}")
        
        # Look for download links
        print(f"\n🔗 Found {len(september_texts)} September mentions")
        
        # Look for specific fortnightly patterns
        print("\n🔍 Looking for fortnightly portfolio mentions:")
        fortnightly_texts = soup.find_all(string=re.compile(r'fortnightly.*portfolio', re.IGNORECASE))
        
        for i, text in enumerate(fortnightly_texts[:10]):
            print(f"  {i+1}. {text.strip()}")
            
            # Find parent elements and look for download links
            parent = text.parent
            level = 0
            while parent and level < 5:
                download_links = parent.find_all('a', href=True)
                if download_links:
                    print(f"    Found {len(download_links)} links at level {level}")
                    for link in download_links[:3]:  # Show first 3
                        href = link.get('href', '')
                        link_text = link.get_text(strip=True)
                        print(f"      - {link_text}: {href}")
                parent = parent.parent
                level += 1
        
        # Look for any download buttons or links
        print(f"\n🔗 Found {len(fortnightly_texts)} fortnightly mentions")
        
        # Look for download buttons
        print("\n🔍 Looking for download buttons:")
        download_buttons = soup.find_all(['a', 'button'], string=re.compile(r'download', re.IGNORECASE))
        print(f"Found {len(download_buttons)} download buttons")
        
        for i, button in enumerate(download_buttons[:5]):
            print(f"  {i+1}. {button.get_text(strip=True)} - {button.get('href', 'No href')}")
        
        # Look for links containing 'download' in href
        download_links = soup.find_all('a', href=re.compile(r'download', re.IGNORECASE))
        print(f"\n🔗 Found {len(download_links)} download links")
        
        for i, link in enumerate(download_links[:5]):
            print(f"  {i+1}. {link.get_text(strip=True)} - {link['href']}")
        
        # Check for any Excel or zip files
        file_links = soup.find_all('a', href=re.compile(r'\.(xlsx?|zip)$', re.IGNORECASE))
        print(f"\n📁 Found {len(file_links)} file links (.xlsx/.zip)")
        
        for i, link in enumerate(file_links[:5]):
            print(f"  {i+1}. {link.get_text(strip=True)} - {link['href']}")
        
        # Save a sample of the HTML for inspection
        with open('debug_icici_page.html', 'w', encoding='utf-8') as f:
            f.write(str(soup))
        print(f"\n💾 Saved page HTML to debug_icici_page.html")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fetching page: {e}")
        return False

if __name__ == "__main__":
    debug_icici_page()