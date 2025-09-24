"""
Case Conversion Utilities
Handles column name normalization to ensure consistent variable naming
"""

import polars as pl
import re
from typing import Dict, List

def to_snake_case(name: str) -> str:
    """
    Convert any naming convention to snake_case
    Handles camelCase, PascalCase, kebab-case, spaces, etc.
    """
    # Replace spaces and hyphens with underscores
    name = re.sub(r'[-\s]+', '_', name)

    # Insert underscore before capital letters (for camelCase/PascalCase)
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)

    # Convert to lowercase
    name = name.lower()

    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)

    # Remove leading/trailing underscores
    name = name.strip('_')

    return name

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert all column names to snake_case for consistent processing
    Returns dataframe with normalized column names
    """
    # Create mapping of original to snake_case names
    column_mapping = {col: to_snake_case(col) for col in df.columns}

    # Handle duplicates that might arise from normalization
    used_names = set()
    final_mapping = {}

    for original, snake in column_mapping.items():
        if snake in used_names:
            # Add suffix to make unique
            counter = 2
            while f"{snake}_{counter}" in used_names:
                counter += 1
            snake = f"{snake}_{counter}"

        used_names.add(snake)
        final_mapping[original] = snake

    # Rename columns
    return df.rename(final_mapping)

def denormalize_column_names(df: pl.DataFrame, original_columns: List[str]) -> pl.DataFrame:
    """
    Convert column names back to their original case if they exist in original_columns
    New columns (from enrichment) keep snake_case
    """
    # Create reverse mapping for original columns
    original_snake_mapping = {to_snake_case(col): col for col in original_columns}

    # Find which columns to rename back
    rename_mapping = {}
    for col in df.columns:
        if col in original_snake_mapping:
            rename_mapping[col] = original_snake_mapping[col]

    if rename_mapping:
        return df.rename(rename_mapping)
    else:
        return df

def get_column_case_style(columns: List[str]) -> str:
    """
    Detect the predominant case style in column names
    Returns: 'snake_case', 'camelCase', 'PascalCase', 'kebab-case', 'mixed', or 'unknown'
    """
    if not columns:
        return 'unknown'

    snake_count = sum(1 for col in columns if '_' in col and col.islower())
    camel_count = sum(1 for col in columns if re.search(r'[a-z][A-Z]', col) and '_' not in col)
    pascal_count = sum(1 for col in columns if col[0].isupper() and re.search(r'[a-z][A-Z]', col) and '_' not in col)
    kebab_count = sum(1 for col in columns if '-' in col and col.islower())

    total = len(columns)

    # Determine predominant style (>50% threshold)
    if snake_count / total > 0.5:
        return 'snake_case'
    elif camel_count / total > 0.5:
        return 'camelCase'
    elif pascal_count / total > 0.5:
        return 'PascalCase'
    elif kebab_count / total > 0.5:
        return 'kebab-case'
    elif (snake_count + camel_count + pascal_count + kebab_count) / total > 0.5:
        return 'mixed'
    else:
        return 'unknown'

def create_column_mapping_report(original_columns: List[str]) -> Dict[str, str]:
    """
    Create a mapping report showing original -> snake_case conversions
    Useful for debugging and documentation
    """
    return {col: to_snake_case(col) for col in original_columns}
