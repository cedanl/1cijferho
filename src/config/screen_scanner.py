import os
import ast
import re


def extract_page_info(file_path):
    """Extract page title and icon from a Python file"""
    # Try different encodings
    encodings = ["utf-8", "latin1", "cp1252"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as file:
                tree = ast.parse(file.read())

                # Default values
                title = os.path.splitext(os.path.basename(file_path))[0]
                icon = "📊"  # Default icon

                # Look for st.title() or st.header() calls
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call):
                        if isinstance(node.func, ast.Attribute):
                            if node.func.attr in ["title", "header"]:
                                if len(node.args) > 0 and isinstance(
                                    node.args[0], ast.Str
                                ):
                                    title = node.args[0].s
                                    break

                return title, icon
        except UnicodeDecodeError:
            continue

    # If all encodings fail, use default values
    return os.path.splitext(os.path.basename(file_path))[0], "📊"


def get_screens():
    """Get all available screens from the frontend/Modules directory"""
    try:
        # Get the absolute path to the src directory
        src_dir = os.path.dirname(os.path.dirname(__file__))
        modules_dir = os.path.join(src_dir, "frontend", "Modules")

        # Dictionary to store screen information
        screens = {}

        # Walk through the Modules directory
        for root, _, files in os.walk(modules_dir):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    full_path = os.path.join(root, file)

                    # Extract page info with encoding handling
                    title, icon = extract_page_info(full_path)

                    # Get module name without extension
                    module_name = os.path.splitext(file)[0]

                    # Create relative path from src directory
                    rel_path = os.path.relpath(full_path, src_dir)
                    # Convert path to module path format
                    module_path = os.path.splitext(rel_path)[0].replace(os.sep, ".")

                    # Get subdirectory relative to Modules
                    subdirectory = os.path.relpath(root, modules_dir)
                    if subdirectory == ".":
                        subdirectory = ""

                    # Add to screens dictionary
                    screens[module_path] = {
                        "name": module_name,
                        "title": title,
                        "icon": icon,
                        "path": module_path,
                        "subdirectory": subdirectory,
                    }

        return screens

    except Exception as e:
        print(f"Error scanning screens: {str(e)}")
        return {}


def group_pages_by_subdirectory(pages):
    """Group pages by their subdirectory"""
    pages_by_subdirectory = {}

    for page in pages.values():
        subdirectory = page.get("subdirectory", "")

        if subdirectory not in pages_by_subdirectory:
            pages_by_subdirectory[subdirectory] = []

        # Create page info dict with required fields
        page_info = {
            "name": page["name"],
            "title": page["title"],
            "icon": page["icon"],
            "path": page["path"],
        }

        pages_by_subdirectory[subdirectory].append(page_info)

    return pages_by_subdirectory
