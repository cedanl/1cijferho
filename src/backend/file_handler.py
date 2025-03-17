import streamlit as st
import polars as pl
import io


def detect_csv_separator(file_content):
    """Detect if a CSV file uses comma or semicolon as separator"""
    # Count separators in the entire file
    comma_count = file_content.count(",")
    semicolon_count = file_content.count(";")
    return ";" if semicolon_count > comma_count else ","


def load_file(file_path):
    """Load a file into a Polars DataFrame with proper format detection"""
    try:
        # Get file extension
        file_extension = file_path.split(".")[-1].lower()

        # Read file based on extension
        if file_extension == "xlsx":
            df = pl.read_excel(file_path)
        elif file_extension == "parquet":
            df = pl.read_parquet(file_path)
        else:  # CSV
            # Try different encodings
            encodings = ["utf-8", "latin1", "cp1252"]
            for encoding in encodings:
                try:
                    # Read file content
                    with open(file_path, "r", encoding=encoding) as f:
                        file_content = f.read()
                    # Detect separator
                    separator = detect_csv_separator(file_content)
                    # Read CSV with detected separator
                    df = pl.read_csv(
                        file_path,
                        separator=separator,
                        encoding=encoding,
                        low_memory=False,  # Handle large files better
                    )
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise UnicodeDecodeError(
                    f"Could not read file with any of these encodings: {encodings}"
                )

        return df

    except Exception as e:
        raise Exception(f"Error loading file: {str(e)}")


def file_handler():
    """Handle file uploads in the Streamlit app"""
    st.sidebar.info("Maximum file size: 1GB")
    uploaded_file = st.sidebar.file_uploader(
        "Upload data file",
        type=["xlsx", "csv", "parquet"],
        help="Upload an Excel, CSV, or Parquet file containing student data",
        key="data_file_upload",  # Unique key for file uploader
    )

    if uploaded_file is not None:
        try:
            # Get file extension
            file_extension = uploaded_file.name.split(".")[-1].lower()

            # Show file info
            file_size_mb = uploaded_file.size / (1024 * 1024)

            # Read file based on extension
            if file_extension == "xlsx":
                df = pl.read_excel(uploaded_file)
            elif file_extension == "parquet":
                df = pl.read_parquet(uploaded_file)
            else:  # CSV
                # Try different encodings
                encodings = ["utf-8", "latin1", "cp1252"]
                for encoding in encodings:
                    try:
                        # Read file content
                        file_content = uploaded_file.getvalue().decode(encoding)
                        # Detect separator
                        separator = detect_csv_separator(file_content)
                        # Read CSV with detected separator
                        df = pl.read_csv(
                            io.StringIO(file_content),
                            separator=separator,
                            low_memory=False,  # Handle large files better
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise UnicodeDecodeError(
                        f"Could not read file with any of these encodings: {encodings}"
                    )

            # Store in session state
            st.session_state.df = df

            # Show data info
            rows, cols = df.shape
            st.sidebar.success(f"✅ File loaded successfully!")
            st.sidebar.info(f"📊 Data shape: {rows:,} rows × {cols:,} columns")

            return True

        except Exception as e:
            st.sidebar.error(f"Error loading file: {str(e)}")
            if "df" in st.session_state:
                del st.session_state.df
            return False

    # Clear session state if no file is uploaded
    if "df" in st.session_state and uploaded_file is None:
        del st.session_state.df
    return None
