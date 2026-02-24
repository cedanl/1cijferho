import polars as pl
import json
import os
from backend.utils.converter_headers import normalize_name, clean_header_name


def load_variable_mappings(variable_metadata_path=None, naming_func=None):
	"""
	Load variable-level value mappings from a variable metadata JSON file.
	Returns dict: {normalized_variable_name: {raw_code: label, ...}}
	Tries sensible default locations when `variable_metadata_path` is None.
	"""
	candidates = []
	if variable_metadata_path:
		candidates.append(variable_metadata_path)
	candidates.append(os.path.join(os.getcwd(), 'data', '00-metadata', 'json', 'variable_metadata.json'))
	for p in candidates:
		if os.path.exists(p):
			path = p
			break
	else:
		return {}
	# --- Sanitize variable_metadata.json before loading ---
	try:
		from backend.utils.sanitize_variable_metadata import sanitize_variable_metadata_json
		sanitize_variable_metadata_json(path)
	except Exception as e:
		print(f"[decoder] Warning: could not sanitize variable metadata {path}: {e}")
	try:
		with open(path, encoding='utf-8') as f:
			items = json.load(f)
	except Exception as e:
		print(f"[decoder] Warning: could not load variable metadata {path}: {e}")
		return {}
	maps = {}
	for item in items:
		name = item.get('name')
		values = item.get('values') or {}
		if not name or not isinstance(values, dict):
			continue
		norm = normalize_name(name, naming_func)
		entry = {}
		for k, v in values.items():
			# keep raw key as-is (including markers like '[leeg]') but strip surrounding whitespace
			if isinstance(k, str):
				key = k.strip()
			else:
				key = str(k)
			# Add original string
			entry[key] = v
			# Add uppercase variant if different
			if key.upper() != key:
				entry[key.upper()] = v
			# Add zero-padded variant for numeric codes (up to 2 digits)
			if key.isdigit():
				zfill2 = key.zfill(2)
				entry[zfill2] = v
				try:
					int_key = int(key)
					entry[int_key] = v
				except Exception:
					pass
			# Add int version if possible and not already present
			else:
				try:
					int_key = int(key)
					if int_key not in entry:
						entry[int_key] = v
				except Exception:
					pass
		if entry:
			# store original name and mapping for richer diagnostics
			maps[norm] = {"orig_name": name, "mapping": entry}
	# Logging: show how many variable mappings were loaded
	if maps:
		try:
			print(f"[decoder] Loaded {len(maps)} variable mappings from {path}")
			# print a small sample of variable names and key counts
			sample = list(maps.items())[:5]
			for k, v in sample:
				try:
					print(f"[decoder]  - {v.get('orig_name')} (normalized: {k}) -> {len(v.get('mapping', {}))} keys")
				except Exception:
					pass
		except Exception:
			pass
	return maps

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
		# Try to determine code columns from metadata content
		schema_overrides = {}
		content = table.get('content', [])
		if len(content) >= 2:
			# First code column is usually in the second row, first word
			code_col = content[1].split()[0]
			schema_overrides[code_col] = pl.String
			# If a composite key, second code column is in the third row
			if len(content) > 2:
				code_col2 = content[2].split()[0]
				schema_overrides[code_col2] = pl.String
		try:
			if schema_overrides:
				df = pl.read_csv(dec_path, separator=';', encoding='latin1', schema_overrides=schema_overrides)
			else:
				df = pl.read_csv(dec_path, separator=';', encoding='latin1')
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
	from backend.utils.converter_headers import strip_accents
	norm_map = {normalize_name(col, naming_func): col for col in orig_columns}
	norm_columns = list(norm_map.keys())
	norm_df = df.rename({v: k for k, v in norm_map.items()})
	# Force all columns to string to preserve leading zeros for codes
	for col in norm_df.columns:
		norm_df = norm_df.with_columns(
			pl.col(col).cast(pl.Utf8).str.strip_chars().alias(col)
		)
	decode_summary = []
	dec_tables_used = set()
	dec_tables_not_used = set(dec_tables.keys())
	result_df = norm_df.clone()
	import difflib
	for table in meta['tables']:
		dec_vars = table.get('decoding_variables', [])
		for table in meta['tables']:
			dec_vars = table.get('decoding_variables', [])
			dec_table = dec_tables.get(table['table_title'])
			content = table.get('content', [])
			# PATCH: If no decoding_variables, use first column as decoding variable for any table
			if not dec_vars and len(content) > 1:
				code_col = content[1].split()[0]
				dec_vars = [code_col]
			if dec_vars:
				if dec_table is None:
					continue
				if len(content) < 2:
					continue
				code_col = content[1].split()[0]
				code_col_norm = normalize_name(strip_accents(code_col), naming_func)
				join_df = dec_table.rename({c: normalize_name(strip_accents(c), naming_func) for c in dec_table.columns})
				# Special handling for Dec_landcode and Dec_nationaliteitscode: fallback to correct code column if 'code' is missing
				if table['table_title'].lower().startswith('dec_landcode') and code_col_norm not in join_df.columns:
					if 'code_land' in join_df.columns:
						code_col_norm = 'code_land'
					else:
						continue
				if table['table_title'].lower().startswith('dec_nationaliteitscode') and code_col_norm not in join_df.columns:
					if 'code_nationaliteit' in join_df.columns:
						code_col_norm = 'code_nationaliteit'
					else:
						continue
				if code_col_norm in join_df.columns:
					join_df = join_df.with_columns(
						pl.col(code_col_norm).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(code_col_norm)
					)
				for var in dec_vars:
					var_norm = normalize_name(strip_accents(var), naming_func)
					if var_norm not in result_df.columns:
						closest = difflib.get_close_matches(var_norm, result_df.columns, n=1)
						if closest:
							print(f"[decode_fields][DEBUG] Skipping '{var}' (normalized: '{var_norm}') - not in main DataFrame. Closest match: {closest[0]}")
						else:
							print(f"[decode_fields][DEBUG] Skipping '{var}' (normalized: '{var_norm}') - not in main DataFrame. No close match found.")
						continue
					# Normalize main df code column to string and strip whitespace
					result_df = result_df.with_columns(
						pl.col(var_norm).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(var_norm)
					)
					try:
						dec_cols = [c for c in join_df.columns if c != code_col_norm]
						before_rows = result_df.height
						joined = result_df.join(
							join_df,
							left_on=var_norm,
							right_on=code_col_norm,
							how='left',
						)
						for col in dec_cols:
							new_col = f"{var_norm}__{normalize_name(strip_accents(col), naming_func)}"
							result_df = result_df.with_columns(
								joined[col].alias(new_col)
							)
						after_rows = result_df.height
						decode_summary.append(f"Decoded {var} ({before_rows} rows, {len(dec_cols)} columns added)")
						unmatched = joined.filter(pl.col(var_norm).is_not_null() & pl.col(dec_cols[0]).is_null())
						if unmatched.height > 0:
							sample_codes = unmatched[var_norm].unique().to_list()[:5]
							print(f"[decode_fields][DEBUG] Unmatched codes for {var} with {table['table_title']}: {sample_codes}")
					except Exception as e:
						print(f"[decode_fields][DEBUG] Error decoding {var} with {table['table_title']}: {e}")
	# Vakkenbestanden patch: checking Opmerking for decode instructions
	for table in meta['tables']:
		if table.get('decoding_variables', []):
			continue  # Already handled
		# Look for columns with 'te decoderen met Dec_' in their Opmerking
		content = table.get('content', [])
		if not content or len(content) < 2:
			continue
		# Find header row (should contain 'Opmerking' or similar)
		header_row = None
		for i, row in enumerate(content):
			if 'opmerking' in row.lower():
				header_row = i
				break
		if header_row is None:
			continue
		headers = [h.strip().lower() for h in content[header_row].split()]
		# Find column indices
		col_idx = {h: i for i, h in enumerate(headers)}
		# For each data row, check if Opmerking contains 'te decoderen met Dec_'
		for row in content[header_row+1:]:
			parts = row.split(None, len(headers)-1)
			if len(parts) < len(headers):
				continue
			naam = parts[col_idx.get('naam', 0)]
			opm = parts[col_idx.get('opmerking', -1)] if 'opmerking' in col_idx else ''
			import re as _re
			# Detect composite key: "in combinatie met ... te decoderen met Dec_X.asc"
			composite = None
			dec_table_title = None
			if 'in combinatie met' in opm.lower() and 'te decoderen met dec_' in opm.lower():
				m = _re.search(r'in combinatie met ([A-Za-z0-9_]+) te decoderen met (Dec_[A-Za-z0-9_]+)\.asc', opm, _re.IGNORECASE)
				if m:
					composite = m.group(1)
					dec_table_title = m.group(2) + '.asc'
			elif 'te decoderen met dec_' in opm.lower():
				m = _re.search(r'te decoderen met (Dec_[A-Za-z0-9_]+)\.asc', opm)
				if m:
					dec_table_title = m.group(1) + '.asc'
				else:
					continue
			else:
				continue
			if dec_table_title:
				if dec_table_title in dec_tables:
					dec_tables_used.add(dec_table_title)
					dec_tables_not_used.discard(dec_table_title)
			var_norm = normalize_name(naam, naming_func)
			print(f"[decode_fields][DEBUG][vakken] Checking column: '{naam}' (normalized: '{var_norm}') | Opmerking: '{opm}'")
			print(f"[decode_fields][DEBUG][vakken] Main DataFrame columns: {list(result_df.columns)}")
			# Find DEC table
			dec_table = dec_tables.get(dec_table_title)
			if dec_table is None:
				print(f"[decode_fields][DEBUG][vakken] DEC table not loaded for {dec_table_title}")
				continue
			# Find code column(s) in DEC table
			dec_content = None
			for t in meta['tables']:
				if t['table_title'] == dec_table_title:
					dec_content = t.get('content', [])
					break
			if not dec_content or len(dec_content) < 2:
				continue
			dec_code_col = dec_content[1].split()[0]
			dec_code_col_norm = normalize_name(dec_code_col, naming_func)
			join_df = dec_table.rename({c: normalize_name(c, naming_func) for c in dec_table.columns})
			# Composite key join
			if composite:
				composite_norm = normalize_name(composite, naming_func)
				# Find second key in DEC table (should be second column)
				if len(dec_content) > 2:
					dec_code_col2 = dec_content[2].split()[0]
					dec_code_col2_norm = normalize_name(dec_code_col2, naming_func)
				else:
					print(f"[decoder][vakken] Could not find second key for composite join in {dec_table_title}")
					continue
				# Prepare both columns in main and join_df
				for col in [var_norm, composite_norm]:
					if col not in result_df.columns:
						closest = difflib.get_close_matches(col, result_df.columns, n=1)
						print(f"[decoder][vakken] Skipping {col} (composite join) - not in main DataFrame.")
						print(f"[decoder][vakken] Available columns: {list(result_df.columns)}")
						if closest:
							print(f"[decoder][vakken] Closest match: {closest[0]}")
						continue
					result_df = result_df.with_columns(
						pl.col(col).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(col)
					)
				for col in [dec_code_col_norm, dec_code_col2_norm]:
					if col in join_df.columns:
						join_df = join_df.with_columns(
							pl.col(col).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(col)
						)
				try:
					dec_cols = [c for c in join_df.columns if c not in [dec_code_col_norm, dec_code_col2_norm]]
					before_rows = result_df.height
					joined = result_df.join(
						join_df,
						left_on=[var_norm, composite_norm],
						right_on=[dec_code_col_norm, dec_code_col2_norm],
						how='left',
					)
					for col in dec_cols:
						new_col = f"{var_norm}__{col}"
						result_df = result_df.with_columns(
							joined[col].alias(new_col)
						)
					after_rows = result_df.height
					decode_summary.append(f"Decoded {naam} + {composite} ({before_rows} rows, {len(dec_cols)} columns added) [vakken-composite]")
					unmatched = joined.filter(
						pl.col(var_norm).is_not_null() & pl.col(composite_norm).is_not_null() & pl.col(dec_cols[0]).is_null()
					)
					if unmatched.height > 0:
						sample_codes = list(zip(unmatched[var_norm].unique().to_list()[:5], unmatched[composite_norm].unique().to_list()[:5]))
						print(f"[decoder][vakken] Unmatched codes for {naam} + {composite} with {dec_table_title}: {sample_codes}")
				except Exception as e:
					print(f"[decoder][vakken] Error decoding {naam} + {composite} with {dec_table_title}: {e}")
			else:
				if var_norm not in result_df.columns:
					closest = difflib.get_close_matches(var_norm, result_df.columns, n=1)
					print(f"[decoder][vakken] Skipping {naam} (normalized: {var_norm}) - not in main DataFrame.")
					print(f"[decoder][vakken] Available columns: {list(result_df.columns)}")
					if closest:
						print(f"[decoder][vakken] Closest match: {closest[0]}")
					continue
				# Normalize main df code column to string and strip whitespace
				result_df = result_df.with_columns(
					pl.col(var_norm).cast(pl.Utf8).str.strip_chars().alias(var_norm)
				)
				if dec_code_col_norm in join_df.columns:
					join_df = join_df.with_columns(
						pl.col(dec_code_col_norm).cast(pl.Utf8).str.strip_chars().alias(dec_code_col_norm)
					)
				try:
					dec_cols = [c for c in join_df.columns if c != dec_code_col_norm]
					before_rows = result_df.height
					joined = result_df.join(
						join_df,
						left_on=var_norm,
						right_on=dec_code_col_norm,
						how='left',
					)
					for col in dec_cols:
						new_col = f"{var_norm}__{col}"
						result_df = result_df.with_columns(
							joined[col].alias(new_col)
						)
					after_rows = result_df.height
					decode_summary.append(f"Decoded {naam} ({before_rows} rows, {len(dec_cols)} columns added) [vakken]")
					unmatched = joined.filter(pl.col(var_norm).is_not_null() & pl.col(dec_cols[0]).is_null())
					if unmatched.height > 0:
						sample_codes = unmatched[var_norm].unique().to_list()[:5]
						print(f"[decoder][vakken] Unmatched codes for {naam} with {dec_table_title}: {sample_codes}")
				except Exception as e:
					print(f"[decoder][vakken] Error decoding {naam} with {dec_table_title}: {e}")
	# Decoding summary and DEC tables used/not used can be logged elsewhere if needed
	# --- Apply variable-level mappings from variable_metadata.json (if present) ---
	try:
		var_maps = load_variable_mappings(None, naming_func=naming_func)
		if var_maps:
			import difflib as _difflib
			print(f"[decode_fields][VAR_MAP] Applying variable mappings to DataFrame columns...")
			for var_norm, info in var_maps.items():
				mapping = info.get('mapping') if isinstance(info, dict) else info
				orig_name = info.get('orig_name') if isinstance(info, dict) else None
				# If exact normalized column not present, try fuzzy matching against cleaned/original headers
				chosen_col = var_norm
				if var_norm not in result_df.columns:
					# Candidates: normalized result_df columns and normalized original headers
					candidates = list(result_df.columns)
					# also include normalized versions of original headers (from norm_map)
					candidates += list(norm_map.keys())
					# also try cleaning original headers and normalizing
					cleaned_candidates = []
					for orig in orig_columns:
						try:
							cn = normalize_name(clean_header_name(orig), naming_func)
							cleaned_candidates.append(cn)
						except Exception:
							pass
					candidates += cleaned_candidates
					closest = _difflib.get_close_matches(var_norm, candidates, n=3)
					if closest:
						# prefer a candidate that is actually a column in result_df
						pick = None
						for c in closest:
							if c in result_df.columns:
								pick = c
								break
						if pick is None and closest[0] in norm_map:
							pick = closest[0]
						if pick is None:
							pick = closest[0]
						chosen_col = pick
						print(f"[decode_fields][VAR_MAP] Mapping for '{var_norm}' (orig: '{orig_name}') not found; using closest match '{chosen_col}'")
					else:
						print(f"[decode_fields][VAR_MAP] Mapping for '{var_norm}' (orig: '{orig_name}') present but column missing. Closest columns: {closest}")
						continue
				try:
					# Ensure code column is string and stripped
					result_df = result_df.with_columns(
						pl.col(chosen_col).cast(pl.Utf8).str.strip_chars().alias(chosen_col)
					)
					# sample up to 10 distinct values from the column for diagnostics
					try:
						sample_vals = result_df[chosen_col].unique().to_list()[:10]
					except Exception:
						sample_vals = []
					map_keys = list(mapping.keys()) if isinstance(mapping, dict) else []
					sample_map_keys = map_keys[:10]
					# compute simple intersection counts (case-insensitive)
					lower_map_keys = {k.lower() for k in map_keys if isinstance(k, str)}
					matched = 0
					matched_examples = []
					for sv in sample_vals:
						if sv is None:
							continue
						s = str(sv).strip()
						if s in mapping or s.lower() in lower_map_keys:
							matched += 1
							matched_examples.append(s)
					print(f"[decode_fields][VAR_MAP][DIAG] '{var_norm}' (orig: '{orig_name}') sample_values={sample_vals[:5]} sample_map_keys={sample_map_keys} matched_sample_count={matched}/{len(sample_vals)}")
					# Replace original column values with mapped labels (no extra _label column)
					try:
						src_vals = result_df[chosen_col].to_list()
					except Exception:
						src_vals = []
					mapped_vals = []
					unq_unmapped_examples = []
					total_non_null = 0
					mapped_count = 0
					seen_unmapped = set()
					for v in src_vals:
						if v is None:
							mapped_vals.append(None)
							continue
						s = str(v).strip()
						found = None
						if s == '':
							total_non_null += 1
							for k in mapping:
								if isinstance(k, str) and 'leeg' in k.lower():
									found = mapping[k]
									break
							mapped_vals.append(found if found is not None else s)
							if found is None:
								if s not in seen_unmapped:
									unq_unmapped_examples.append(s)
									seen_unmapped.add(s)
							else:
								mapped_count += 1
							continue
						total_non_null += 1
						# Try all reasonable variants for lookup
						variants = [s, s.upper()]
						if s.isdigit():
							variants.append(s.zfill(2))
							try:
								variants.append(int(s))
							except Exception:
								pass
						# Try each variant
						for variant in variants:
							if variant in mapping:
								mapped_vals.append(mapping[variant])
								mapped_count += 1
								break
						else:
							# Fallback: try case-insensitive match and int-string equivalence
							matched = None
							for k, val in mapping.items():
								if isinstance(k, str) and k.lower() == s.lower():
									matched = val
									break
								try:
									if isinstance(k, int) and str(k) == s:
										matched = val
										break
								except Exception:
									pass
							mapped_vals.append(matched if matched is not None else s)
							if matched is None:
								if s not in seen_unmapped:
									unq_unmapped_examples.append(s)
									seen_unmapped.add(s)
							else:
								mapped_count += 1
					# Replace column in DataFrame
					try:
						result_df = result_df.with_columns(
							pl.Series(mapped_vals).alias(chosen_col)
						)
					except Exception:
						# fallback: create Series with explicit dtype
						result_df = result_df.with_columns(
							pl.Series(mapped_vals).cast(pl.Utf8).alias(chosen_col)
						)
					print(f"[decode_fields][VAR_MAP] '{chosen_col}' (orig: '{orig_name}'): total non-null={total_non_null}, mapped={mapped_count}, sample_unmapped={unq_unmapped_examples[:5]} (replaced in-place)")
				except Exception as e:
					print(f"[decode_fields][VAR_MAP][ERROR] Applying mapping for {var_norm}: {e}")
	except Exception as e:
		print(f"[decode_fields][DEBUG] Error applying variable mappings: {e}")

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

