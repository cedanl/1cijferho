import os
import shutil
from rich.console import Console

def move_dec_csv_files():
    """Move CSV files starting with 'Dec' from data/01-input to data/02-output"""
    console = Console()
    
    # Create output directory if it doesn't exist
    os.makedirs("data/02-output", exist_ok=True)
    
    # Check if input directory exists
    if not os.path.exists("data/01-input"):
        console.print("[bold red]Error: data/01-input directory does not exist[/bold red]")
        return
    
    # Process files
    moved_files = []
    for filename in os.listdir("data/01-input"):
        # Check if file starts with 'dec' (case insensitive) and ends with .csv
        if filename.lower().startswith("dec") and filename.lower().endswith(".csv"):
            # Move file
            source = os.path.join("data/01-input", filename)
            destination = os.path.join("data/02-output", filename)
            shutil.move(source, destination)
            moved_files.append(filename)
            console.print(f"Moved: {filename}")
    
    # Print summary with color based on whether files were moved
    if moved_files:
        console.print(f"[bold green]Total files moved: {len(moved_files)}[/bold green]")
    else:
        console.print("[bold yellow]No files found matching 'Dec*.csv' pattern[/bold yellow]")

# Run the function
if __name__ == "__main__":
    move_dec_csv_files()