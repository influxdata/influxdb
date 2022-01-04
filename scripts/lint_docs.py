#!/usr/bin/env python3

import argparse
import re
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description="Lint docs")
parser.add_argument("docs", help="docs directory")
args = parser.parse_args()

regex = re.compile('\[.+\]\((?P<link>[^)]+)\)')

success = True

for md_path in sorted(Path(args.docs).rglob("*.md")):
    print(f"Checking \"{md_path}\"")

    with open(md_path) as file:
        links = {match.group('link'): idx + 1 for (idx, line) in enumerate(file) for match in regex.finditer(line)}
        for link, line_number in links.items():
            if link.startswith('https://') or link.startswith('#'):
                continue

            if link.startswith('http://'):
                print(f"FAIL: Non-SSL URL {link} in {md_path}:{line_number}", file=sys.stderr)
                success = False
                continue

            if not Path(md_path.parent, link).exists():
                print(f"FAIL: Link {link} not found for {md_path}:{line_number}", file=sys.stderr)
                success = False

if not success:
    exit(1)

print("OK")
