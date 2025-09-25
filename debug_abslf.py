#!/usr/bin/env python3
"""
Quick debug script to see what's actually on the ABSLF page.
"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def debug_abslf_page():
    url = "https://mutualfund.adityabirlacapital.com/forms-and-downloads/portfolio"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    print(f"🔍 Checking: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"Content length: {len(response.content)} bytes")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print(f"\n=== Page Title ===")
        title = soup.title.get_text() if soup.title else "No title"
        print(title)
        
        # Check for any downloadable files
        print(f"\n=== All Links with Download Extensions ===")
        links = soup.find_all('a', href=True)
        download_extensions = ['.pdf', '.xls', '.xlsx', '.zip', '.doc', '.docx']
        
        download_links = []
        for link in links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            if any(ext in href.lower() for ext in download_extensions):
                download_links.append({
                    'text': text,
                    'href': href,
                    'full_url': urljoin(url, href)
                })
        
        print(f"Found {len(download_links)} download links:")
        for i, link in enumerate(download_links, 1):
            print(f"{i}. {link['text']}")
            print(f"   {link['full_url']}")
            print()
        
        # Check for portfolio-related text
        print(f"\n=== Links with 'Portfolio' in text ===")
        portfolio_links = []
        for link in links:
            text = link.get_text(strip=True).lower()
            if 'portfolio' in text:
                portfolio_links.append({
                    'text': link.get_text(strip=True),
                    'href': link.get('href', '')
                })
        
        print(f"Found {len(portfolio_links)} portfolio-related links:")
        for link in portfolio_links[:10]:  # Show first 10
            print(f"- {link['text']} -> {link['href']}")
        
        # Check for disclosure-related text
        print(f"\n=== Links with 'Disclosure' in text ===")
        disclosure_links = []
        for link in links:
            text = link.get_text(strip=True).lower()
            if 'disclosure' in text:
                disclosure_links.append({
                    'text': link.get_text(strip=True),
                    'href': link.get('href', '')
                })
        
        print(f"Found {len(disclosure_links)} disclosure-related links:")
        for link in disclosure_links[:10]:  # Show first 10
            print(f"- {link['text']} -> {link['href']}")
        
        # Check page content for clues
        print(f"\n=== Page Content Clues ===")
        page_text = soup.get_text().lower()
        
        keywords = ['download', 'portfolio', 'disclosure', 'excel', 'zip', 'monthly', 'quarterly']
        for keyword in keywords:
            count = page_text.count(keyword)
            if count > 0:
                print(f"'{keyword}' appears {count} times")
        
        # Look for forms or buttons
        forms = soup.find_all('form')
        buttons = soup.find_all('button')
        inputs = soup.find_all('input', type='submit')
        
        print(f"\nFound {len(forms)} forms, {len(buttons)} buttons, {len(inputs)} submit inputs")
        
        if forms:
            print("Form actions:")
            for form in forms:
                action = form.get('action', '')
                method = form.get('method', 'get')
                print(f"  {method.upper()} {action}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_abslf_page()