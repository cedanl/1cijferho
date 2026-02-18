# Ensure project root is in sys.path for backend imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
import polars as pl
import json
import re

def normalize_name(name, naming_func=None):
	"""
	Normalize variable names using the provided naming convention function (e.g., snake_case).
	If no function is provided, defaults to snake_case.
	"""
	if naming_func:
		return naming_func(name)
	# Default: snake_case, remove special chars
	name = name.lower()
	name = re.sub(r'[^a-z0-9]+', '_', name)
	name = re.sub(r'_+', '_', name).strip('_')
	return name

def load_dec_tables_from_metadata(metadata_json_path, dec_output_dir, naming_func=None):
	"""
	Load Dec_* tables as Polars DataFrames based on metadata JSON.
	Returns a dict: {table_title: DataFrame}
	Skips missing files with warning.
	"""
	with open(metadata_json_path, encoding='utf-8') as f:
		meta = json.load(f)
	dec_tables = {}
	for table in meta['tables']:
		dec_file = table['table_title'].replace('.asc', '.csv')
		dec_path = os.path.join(dec_output_dir, dec_file)
		try:
			df = pl.read_csv(dec_path, separator=';', encoding='latin1')
			if table['table_title'].lower().startswith('dec_landcode'):
				print(f"[decoder][Dec_landcode] Loaded {dec_file} with shape {df.shape}")
				print(f"[decoder][Dec_landcode] Columns: {df.columns}")
				print(f"[decoder][Dec_landcode] First 3 rows:\n{df.head(3)}")
			dec_tables[table['table_title']] = df
		except Exception as e:
			print(f"[decoder] Warning: Could not load {dec_file}: {e}")
	return dec_tables

def decode_fields(df, metadata_json_path, dec_tables, naming_func=None):
	"""
	For each field in df that is listed in decoding_variables in the metadata,
	left join the decoded values from the corresponding Dec_* table.
	Appends decoded columns to df. Returns a new DataFrame.
	"""
	with open(metadata_json_path, encoding='utf-8') as f:
		meta = json.load(f)
	# --- Normalize main DataFrame columns and keep mapping to original names ---
	orig_columns = list(df.columns)
	norm_map = {normalize_name(col, naming_func): col for col in orig_columns}
	norm_columns = list(norm_map.keys())
	norm_df = df.rename({v: k for k, v in norm_map.items()})
	print(f"[decoder] Main DataFrame columns (normalized): {norm_columns}")
	decode_summary = []
	result_df = norm_df.clone()
	for table in meta['tables']:
		dec_vars = table.get('decoding_variables', [])
		if not dec_vars:
			continue
		dec_table = dec_tables.get(table['table_title'])
		if dec_table is None:
			if table['table_title'].lower().startswith('dec_landcode'):
				print(f"[decoder][Dec_landcode] Table not loaded for {table['table_title']}")
			continue
		content = table['content']
		if len(content) < 2:
			continue
		code_col = content[1].split()[0]
		code_col_norm = normalize_name(code_col, naming_func)
		join_df = dec_table.rename({c: normalize_name(c, naming_func) for c in dec_table.columns})
		# Special handling for Dec_landcode: fallback to 'code_land' if 'code' is missing
		if table['table_title'].lower().startswith('dec_landcode') and code_col_norm not in join_df.columns:
			print(f"[decoder][Dec_landcode] Expected code column '{code_col_norm}' not found. Available columns: {join_df.columns}")
			if 'code_land' in join_df.columns:
				code_col_norm = 'code_land'
				print(f"[decoder][Dec_landcode] Fallback: using 'code_land' as code column.")
			else:
				print(f"[decoder][Dec_landcode] ERROR: No suitable code column found for join. Skipping this table.")
				continue
		if code_col_norm in join_df.columns:
			join_df = join_df.with_columns(
				pl.col(code_col_norm).cast(pl.Utf8).str.strip_chars().alias(code_col_norm)
			)
		if table['table_title'].lower().startswith('dec_landcode'):
			print(f"[decoder][Dec_landcode] code_col: {code_col} (normalized: {code_col_norm})")
			print(f"[decoder][Dec_landcode] join_df columns: {join_df.columns}")
			print(f"[decoder][Dec_landcode] join_df sample codes: {join_df[code_col_norm].unique().to_list()[:10] if code_col_norm in join_df.columns else 'N/A'}")
		for var in dec_vars:
			var_norm = normalize_name(var, naming_func)
			if var_norm not in result_df.columns:
				# Try to find the closest match
				import difflib
				closest = difflib.get_close_matches(var_norm, result_df.columns, n=1)
				print(f"[decoder] Skipping {var} (normalized: {var_norm}) - not in main DataFrame.")
				print(f"[decoder] Available columns: {list(result_df.columns)}")
				if closest:
					print(f"[decoder] Closest match: {closest[0]}")
				continue
			# Normalize main df code column to string and strip whitespace
			result_df = result_df.with_columns(
				pl.col(var_norm).cast(pl.Utf8).str.strip_chars().alias(var_norm)
			)
			if table['table_title'].lower().startswith('dec_landcode'):
				print(f"[decoder][Dec_landcode] Decoding variable: {var} (normalized: {var_norm})")
				print(f"[decoder][Dec_landcode] Main column unique codes (first 10): {result_df[var_norm].unique().to_list()[:10]}")
			try:
				dec_cols = [c for c in join_df.columns if c != code_col_norm]
				before_rows = result_df.height
				# Only add decoded columns, do not overwrite main DataFrame
				joined = result_df.join(
					join_df,
					left_on=var_norm,
					right_on=code_col_norm,
					how='left',
				)
				# Add only decoded columns
				for col in dec_cols:
					new_col = f"{var_norm}__{col}"
					result_df = result_df.with_columns(
						joined[col].alias(new_col)
					)
				after_rows = result_df.height
				decode_summary.append(f"Decoded {var} ({before_rows} rows, {len(dec_cols)} columns added)")
				# Debug: print codes that failed to match (if any)
				unmatched = joined.filter(pl.col(var_norm).is_not_null() & pl.col(dec_cols[0]).is_null())
				if unmatched.height > 0:
					sample_codes = unmatched[var_norm].unique().to_list()[:5]
					print(f"[decoder][Dec_landcode] Unmatched codes for {var} with {table['table_title']}: {sample_codes}")
			except Exception as e:
				print(f"[decoder][Dec_landcode] Error decoding {var} with {table['table_title']}: {e}")
	print(f"[decoder] Decoding summary: {decode_summary}")
	# --- Restore original column names for output ---
	result_df = result_df.rename({k: v for k, v in norm_map.items() if k in result_df.columns})
	return result_df


def clean_for_latin1(df):
	"""
	Replace problematic unicode characters in all string columns to ensure latin-1 compatibility.
	Currently replaces '⁄' (U+2044) with '/'.
	Extend as needed for other characters.
	"""
	import polars as pl
	# Regex for any character not in latin-1 (U+0000 to U+00FF)
	non_latin1_regex = r"[^\x00-\xFF]"
	for col in df.columns:
		if df[col].dtype == pl.Utf8:
			df = df.with_columns(
				pl.col(col)
				.str.replace_all('⁄', '/')
				.str.replace_all(non_latin1_regex, '?')
				.alias(col)
			)
	return df

