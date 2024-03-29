#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import subprocess
import sys

EXCLUDED_PREFIXES = ['./.', './third_party', './build', './release']

call_args = ['/usr/bin/env', 'python', './third_party/cpplint/cpplint.py',
             '--filter=-legal/copyright,-build/header_guard,-build/c++11',
             '--linelength=120', '--extensions=hpp,cpp']

print "Running cpplint on entire source tree. This may take a minute or two..."

for (dirpath, dirnames, filenames) in os.walk('.'):
    filtered = False
    for prefix in EXCLUDED_PREFIXES:
        if dirpath.startswith(prefix):
            filtered = True
    if not filtered:
        for filename in filenames:
            if filename.endswith('.hpp') or filename.endswith('.cpp'):
                call_args.append(dirpath + '/' + filename)

sys.exit(subprocess.call(call_args))
