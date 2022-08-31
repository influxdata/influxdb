#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script that updates the arrow dependencies in iox
#
# installation:
# pip install tomlkit requests
#
# Update all arrow crates deps to a specific version:
#
# python update_arrow_deps.py 13
#

import argparse
from pathlib import Path

# use tomlkit as it preserves comments and other formatting
import tomlkit

def update_version_cargo_toml(cargo_toml, new_version):
    print('updating if needed {}'.format(cargo_toml.absolute()))
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    for section in ("dependencies", "dev-dependencies"):
        for (dep_name, constraint) in doc.get(section, {}).items():
            if dep_name in ("arrow", "parquet", "arrow-flight"):
                if type(constraint) == tomlkit.items.String:
                    # handle constraint that is (only) a string like '12',
                    doc[section][dep_name] = new_version
                elif type(constraint) == dict:
                    # handle constraint that is itself a struct like
                    # {'version': '12', 'features': ['prettyprint']}
                    doc[section][dep_name]["version"] = new_version
                elif type(constraint) == tomlkit.items.InlineTable:
                    # handle constraint that is itself a struct like
                    # {'version': '12', 'features': ['prettyprint']}
                    doc[section][dep_name]["version"] = new_version
                else:
                    print("Unknown type for {} {}: {}", dep_name, constraint, type(constraint))

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def main():
    parser = argparse.ArgumentParser(description='Update arrow dep versions.')
    parser.add_argument('new_version', type=str, help='new arrow version')

    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.absolute()

    for cargo_toml in repo_root.rglob('Cargo.toml'):
        update_version_cargo_toml(cargo_toml, args.new_version)



if __name__ == "__main__":
    main()
