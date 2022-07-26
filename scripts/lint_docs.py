#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path


LINK_REGEX = re.compile(r'\[.+\]\((?P<link>[^)]+)\)')


def main() -> None:
    parser = argparse.ArgumentParser(description="Lint docs")
    parser.add_argument("docs", help="docs directory")
    args = parser.parse_args()

    success = True

    for md_path in sorted(Path(args.docs).rglob("*.md")):
        print(f"Checking \"{md_path}\"")

        with open(md_path) as file:
            links = {
                match.group('link'): idx + 1
                for (idx, line) in enumerate(file)
                for match in LINK_REGEX.finditer(line)
            }

            print(f"  found {len(links)} links")

            for link, line_number in links.items():
                # link that are not checked:
                # - safe destination (HTTPS)
                # - anchors
                # - localhost (here we allow HTTP w/o encryption)
                if link.startswith('https://') or link.startswith('#') or link.startswith('http://localhost:'):
                    continue

                # special warning for unsafe links
                if link.startswith('http://'):
                    print(f"FAIL: Non-SSL URL {link} in {md_path}:{line_number}", file=sys.stderr)
                    success = False
                    continue

                # assuming link to local file
                link = link.split("#", maxsplit=1)[0]
                if not Path(md_path.parent, link).exists():
                    print(f"FAIL: Link {link} not found for {md_path}:{line_number}", file=sys.stderr)
                    success = False

    if not success:
        exit(1)

    print("OK")


if __name__ == "__main__":
    main()
