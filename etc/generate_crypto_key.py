#!/usr/bin/env python
#
#   Copyright 2019  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import binascii
import os
import re
import sys

filename = sys.argv[1]

cryptokeys = {
    "warp.hash.class": 128,
    "warp.hash.labels": 128,
    "warp.hash.index": 128,
    "warp.hash.token": 128,
    "warp.hash.app": 128,
    "warp.aes.token": 256,
    "warp.aes.scripts": 256,
    "warp.aes.metasets": 256,
    "warp.aes.logging": 256,
}

s = open(filename).read()

for key, value in cryptokeys.items():
    s = re.sub(key + " = .*",
               key + "= hex:" + binascii.hexlify(os.urandom(int(value / 8))).decode("utf-8"), s)

f = open(filename, 'w')
f.write(s)
f.close()
