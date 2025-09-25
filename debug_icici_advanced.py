#!/usr/bin/env python3
"""
Advanced ICICI API detection and scraping
"""
import requests
import json
import re
from urllib.parse import urljoin

def find_icici_api_endpoints():
    """Try to find API endpoints that ICICI uses to load portfolio data."""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.icicipruamc.com/media-center/downloads'
    })
    
    base_url = "https://www.icicipruamc.com"
    
    # Common API endpoints to try
    api_endpoints = [
        "/api/downloads",
        "/api/media-center/downloads", 
        "/api/portfolio-disclosures",
        "/api/scheme-disclosures",
        "/api/documents",
        "/backend/downloads",
        "/services/downloads",
        "/content/downloads"
    ]
    
    print("🔍 Testing potential API endpoints...")
    
    for endpoint in api_endpoints:
        try:
            url = urljoin(base_url, endpoint)
            response = session.get(url, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ Found working endpoint: {url}")
                
                # Try to parse as JSON
                try:
                    data = response.json()
                    print(f"📄 JSON Response preview: {str(data)[:200]}...")
                    
                    # Look for portfolio-related data
                    json_str = json.dumps(data, default=str).lower()
                    if 'portfolio' in json_str or 'fortnightly' in json_str:
                        print("🎯 Contains portfolio data!")
                        return url, data
                        
                except:
                    print(f"📄 Non-JSON response: {response.text[:200]}...")
                    
            elif response.status_code == 404:
                print(f"❌ {endpoint} - Not Found")
            else:
                print(f"⚠️ {endpoint} - Status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ {endpoint} - Error: {e}")
    
    # Try to find API calls by examining the main page JavaScript
    print("\n🔍 Examining JavaScript for API calls...")
    
    try:
        main_page = session.get(f"{base_url}/media-center/downloads")
        html_content = main_page.text
        
        # Look for API patterns in the JavaScript
        api_patterns = [
            r'api["\']?\s*:\s*["\']([^"\']+)["\']',
            r'fetch\s*\(\s*["\']([^"\']+)["\']',
            r'axios\s*\.\s*get\s*\(\s*["\']([^"\']+)["\']',
            r'["\'](/api/[^"\']+)["\']',
            r'["\']([^"\']*downloads[^"\']*\.json)["\']'
        ]
        
        found_apis = set()
        
        for pattern in api_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            for match in matches:
                if match.startswith('/') or 'api' in match.lower():
                    found_apis.add(match)
        
        print(f"🔍 Found {len(found_apis)} potential API endpoints in JavaScript:")
        for api in found_apis:
            print(f"  - {api}")
            
            # Test each found API
            try:
                test_url = urljoin(base_url, api)
                response = session.get(test_url, timeout=5)
                
                if response.status_code == 200:
                    print(f"    ✅ Working: {test_url}")
                    try:
                        data = response.json()
                        json_str = json.dumps(data, default=str).lower()
                        if 'portfolio' in json_str or 'fortnightly' in json_str:
                            print("    🎯 Contains portfolio data!")
                            return test_url, data
                    except:
                        pass
                        
            except:
                pass
                
    except Exception as e:
        print(f"❌ Error examining JavaScript: {e}")
    
    return None, None

def try_direct_download_patterns():
    """Try to find direct download patterns like ABSLF."""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://www.icicipruamc.com/'
    })
    
    print("\n🔍 Testing direct download URL patterns...")
    
    # Common patterns for September 2025
    test_patterns = [
        "https://www.icicipruamc.com/content/dam/iciciprumf/downloads/portfolio/fortnightly/2025/september/fortnightly_debt_scheme_portfolio_15_september_2025.xlsx",
        "https://www.icicipruamc.com/content/dam/iciciprumf/downloads/fortnightly/2025/fortnightly_portfolio_15_sep_2025.xlsx",
        "https://www.icicipruamc.com/media/downloads/fortnightly/2025/portfolio_15_september_2025.xlsx",
        "https://www.icicipruamc.com/downloads/portfolio/fortnightly_debt_scheme_portfolio_15_september_2025.xlsx",
        "https://www.icicipruamc.com/blob/downloads/fortnightly_portfolio_15_sep_2025.xlsx"
    ]
    
    for pattern in test_patterns:
        try:
            response = session.head(pattern, timeout=10)
            if response.status_code == 200:
                print(f"✅ Found working pattern: {pattern}")
                return pattern
            else:
                print(f"❌ {pattern} - Status: {response.status_code}")
        except Exception as e:
            print(f"❌ {pattern} - Error: {e}")
    
    return None

if __name__ == "__main__":
    print("🚀 ICICI Advanced API Detection")
    print("=" * 50)
    
    # Try API endpoints
    api_url, api_data = find_icici_api_endpoints()
    
    if api_url:
        print(f"\n🎉 Found working API: {api_url}")
    else:
        print("\n❌ No working API endpoints found")
        
        # Try direct patterns
        direct_url = try_direct_download_patterns()
        
        if direct_url:
            print(f"\n🎉 Found direct download pattern: {direct_url}")
        else:
            print("\n❌ No direct download patterns work")
            print("\n💡 This suggests ICICI requires JavaScript/browser rendering")
            print("   We may need to use Selenium or similar browser automation")