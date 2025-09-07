#!/usr/bin/env python3
"""
Rating Standardizer Utility
Standardizes rating formats across different agencies (CRISIL, ICRA, CARE, etc.)
"""

import pandas as pd
import yaml
import re
from pathlib import Path


def load_rating_config():
    """Load rating mapping configuration from YAML file"""
    config_path = Path("config/rating_map.yml")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def extract_rating_from_text(rating_text, config):
    """
    Extract and standardize rating from text containing agency prefix
    
    Args:
        rating_text (str): Original rating text (e.g., "CRISIL AAA", "ICRA AA+")
        config (dict): Rating configuration from YAML
        
    Returns:
        str: Standardized rating or original if not found
    """
    if pd.isna(rating_text) or not isinstance(rating_text, str):
        return None
    
    rating_text = str(rating_text).strip()
    
    # Check for sovereign indicators first
    rating_lower = rating_text.lower()
    for token in config['sovereign_tokens']:
        if token in rating_lower:
            return "SOVEREIGN"
    
    # Check for invalid/non-rating patterns first
    invalid_patterns = [
        r'^\d+\.\d+',  # Numeric patterns like "0.06271996470050001"
    ]
    
    # Add invalid patterns from config if available
    if 'invalid_patterns' in config:
        for pattern in config['invalid_patterns']:
            invalid_patterns.append(pattern.lower().replace(' ', r'\s+'))
    
    for pattern in invalid_patterns:
        if re.search(pattern, rating_lower):
            return None
    
    # Remove common agency prefixes (including FITCH, BWR, IND)
    agencies = ['crisil', 'icra', 'care', 'ind-ra', 'ind', 'brickwork', 'bwr', 'acuite', 'fitch']
    cleaned_rating = rating_text.lower()
    
    # Remove agency prefixes and common separators
    for agency in agencies:
        patterns = [
            f'^{agency}\\s*-\\s*',   # "CRISIL - AAA"
            f'^{agency}\\s+',        # "CRISIL AAA"
            f'^\\[{agency}\\]',      # "[ICRA]AAA"
        ]
        for pattern in patterns:
            cleaned_rating = re.sub(pattern, '', cleaned_rating)
    
    # Remove common suffixes like (CE), (SO), /Stable, etc.
    cleaned_rating = re.sub(r'\s*\([^)]*\)\s*$', '', cleaned_rating)
    cleaned_rating = re.sub(r'\s*/.*$', '', cleaned_rating)
    cleaned_rating = cleaned_rating.strip()
    
    # Try to match against rating map
    for key, standard_rating in config['map'].items():
        if cleaned_rating == key.lower():
            return standard_rating
    
    # If no exact match, try partial matching for common patterns
    # Handle cases like "AAA/Stable", "AA+ (Stable)", etc.
    for key, standard_rating in config['map'].items():
        pattern = f"^{re.escape(key.lower())}[\\s\\(\\/)]*"
        if re.match(pattern, cleaned_rating):
            return standard_rating
    
    # Return None if no match found
    return None


def standardize_rating_column(df, rating_column='Rating', create_new_column=True):
    """
    Standardize ratings in a DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame with rating column
        rating_column (str): Name of the rating column to standardize
        create_new_column (bool): If True, creates 'Standardized Rating' column, 
                                 if False, updates the original column
        
    Returns:
        pd.DataFrame: DataFrame with standardized ratings
    """
    if rating_column not in df.columns:
        print(f"Warning: Column '{rating_column}' not found in DataFrame")
        return df
    
    config = load_rating_config()
    df_copy = df.copy()
    
    # Apply rating standardization
    standardized_ratings = df_copy[rating_column].apply(
        lambda x: extract_rating_from_text(x, config)
    )
    
    if create_new_column:
        df_copy['Standardized Rating'] = standardized_ratings
    else:
        df_copy[rating_column] = standardized_ratings
    
    return df_copy


def get_rating_statistics(df, rating_column='Standardized Rating'):
    """
    Generate rating distribution statistics
    
    Args:
        df (pd.DataFrame): DataFrame with standardized rating column
        rating_column (str): Name of the standardized rating column
        
    Returns:
        pd.DataFrame: Rating statistics with counts and percentages
    """
    if rating_column not in df.columns:
        print(f"Warning: Column '{rating_column}' not found in DataFrame")
        return pd.DataFrame()
    
    config = load_rating_config()
    rating_order = config['order']
    
    # Count ratings
    rating_counts = df[rating_column].value_counts()
    
    # Create ordered statistics
    stats_data = []
    for rating in rating_order:
        if rating in rating_counts:
            count = rating_counts[rating]
            percentage = (count / len(df)) * 100
            stats_data.append({
                'Rating': rating,
                'Count': count,
                'Percentage': round(percentage, 2)
            })
    
    # Add any ratings not in the predefined order
    for rating in rating_counts.index:
        if rating not in rating_order and rating is not None:
            count = rating_counts[rating]
            percentage = (count / len(df)) * 100
            stats_data.append({
                'Rating': rating,
                'Count': count,
                'Percentage': round(percentage, 2)
            })
    
    # Add null ratings if any
    null_count = df[rating_column].isna().sum()
    if null_count > 0:
        stats_data.append({
            'Rating': 'UNRATED/NULL',
            'Count': null_count,
            'Percentage': round((null_count / len(df)) * 100, 2)
        })
    
    return pd.DataFrame(stats_data)


def print_rating_summary(df, rating_column='Standardized Rating'):
    """Print a summary of rating distribution"""
    stats = get_rating_statistics(df, rating_column)
    
    print(f"\n📊 RATING DISTRIBUTION SUMMARY:")
    print("-" * 40)
    for _, row in stats.iterrows():
        print(f"{row['Rating']:>12}: {row['Count']:>4} holdings ({row['Percentage']:>5.1f}%)")
    
    total_rated = df[rating_column].notna().sum()
    coverage = (total_rated / len(df)) * 100
    print(f"\nRating Coverage: {total_rated:,}/{len(df):,} holdings ({coverage:.1f}%)")


# Utility function for external use
def standardize_ratings(df, rating_column='Rating', create_new_column=True, print_summary=False):
    """
    Main utility function to standardize ratings in any DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame with rating column
        rating_column (str): Name of the rating column to standardize
        create_new_column (bool): If True, creates 'Standardized Rating' column
        print_summary (bool): If True, prints rating distribution summary
        
    Returns:
        pd.DataFrame: DataFrame with standardized ratings
    """
    standardized_df = standardize_rating_column(df, rating_column, create_new_column)
    
    if print_summary:
        summary_column = 'Standardized Rating' if create_new_column else rating_column
        print_rating_summary(standardized_df, summary_column)
    
    return standardized_df


if __name__ == "__main__":
    # This function should not create CSV when called alone
    print("Rating Standardizer utility loaded.")
    print("Use standardize_ratings() function to standardize rating columns.")
    print("Example: standardized_df = standardize_ratings(df, 'Rating', print_summary=True)")
