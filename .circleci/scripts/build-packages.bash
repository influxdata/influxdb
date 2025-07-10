#!/bin/bash
set -e

apt-get update
apt-get install --no-install-recommends --yes \
asciidoc        \
build-essential \
git             \
python3         \
python3-dev     \
python3-venv    \
rpm             \
ruby-dev        \
xmlto
gem install fpm
python3 -m venv /tmp/venv
/tmp/venv/bin/pip install -r .circleci/scripts/package/requirements.txt
/tmp/venv/bin/python .circleci/scripts/package/build.py