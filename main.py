# main.py
#
# market-data-loader -- entry point / loader launcher.
#
# Lists all available loaders under loaders/, prompts for a choice,
# clears the screen, and runs the selected loader's run() function.
#
# To add a new loader: create loaders/<name>_loader.py with a run()
# function, then add it to the LOADERS list below.
#
# Usage:
#   python main.py

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# -- Registered loaders -- (display_name, module_path_under_loaders) --------
LOADERS = [
    ("bhavcopy_loader", "loaders.bhavcopy_loader"),
    ("bhavcopy_downloader_loader", "loaders.bhavcopy_downloader_loader"),
]


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def print_loader_menu():
    print("List Of Loaders:")
    for idx, (display_name, _) in enumerate(LOADERS, start=1):
        print(f"{idx}) {display_name}")


def prompt_choice():
    raw = input("\nEnter your choice: ").strip()
    try:
        choice = int(raw)
    except ValueError:
        print(f"Invalid input: '{raw}' is not a number.")
        sys.exit(1)

    if choice < 1 or choice > len(LOADERS):
        print(f"Invalid choice: {choice}. Must be between 1 and {len(LOADERS)}.")
        sys.exit(1)

    return choice


def main():
    print_loader_menu()
    choice = prompt_choice()

    _, module_path = LOADERS[choice - 1]

    clear_screen()

    module = __import__(module_path, fromlist=["run"])
    module.run()


if __name__ == "__main__":
    main()
