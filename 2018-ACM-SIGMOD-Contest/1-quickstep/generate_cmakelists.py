import os
import re
import sys
from sets import Set;

paths = { '.': 1 }
exact_excludes = [ '.' ]
prefix_excludes = [ './build', './third_party' ]

third_parties = {
    'glog/logging': [ 'glog' ],
    'gflags/gflags': [ '${GFLAGS_LIB_NAME}' ],
    'gtest/gtest': [ 'gtest', 'gtest_main' ]
}

def ModuleName(rec):
  if len(rec["path"]) == 0:
    return 'project_' + rec["name"]
  else:
    return 'project_' + rec["path"].replace('_', '').replace('/', '_') + '_' + rec["name"]

def EmptySrcName(d, rec):
  depth = d + rec["path"].count("/")
  if depth == 0:
    return "empty_src.cpp"
  else:
    return "/".join([".."] * depth) + "/empty_src.cpp"

info = dict()

# Get all files
for path in paths: # ./src
  for root, dirs, files in os.walk(path):  # ./src/Rcpp
    if root in exact_excludes:
      continue
    excluded = False
    for prefix in prefix_excludes:
      if root.startswith(prefix):
        excluded = True
        break
    if excluded:
      continue

    for f in files:
      fn = None
      if f.endswith('.hpp'):
        fn = f[:-4]
        ext = 'hpp'
      elif f.endswith('.h'):
        fn = f[:-2]
        ext = 'h'
      elif f.endswith('.cpp'):
        fn = f[:-4]
        ext = 'cpp'

      if (fn):
        if len(root) == len(path):
          fp = fn
        else:
          fp = root[len(path)+1:] + "/" + fn

        if (fp not in info):
          info[fp] = dict()
          info[fp]["name"] = fn
          info[fp]["path"] = root[len(path)+1:]
          info[fp]["root"] = path

        info[fp][ext] = True

header_pattern = re.compile('#include.*["<](.*)[.]h(pp)?[">].*')
main_func_pattern = re.compile('(void|int)\s+main\(.*')
gtest_pattern = re.compile('^TEST_F\(.*\)');

# Figure out dependencies
for fp in info:
  r = info[fp]
  filenames = []
  if ('hpp' in r):
    filenames.append(fp + '.hpp')
  if ('h' in r):
    filenames.append(fp + '.h')
  if ('cpp' in r):
    filenames.append(fp + '.cpp')

  r['deps'] = []
  for filename in filenames:
    with open(r["root"] + "/" + filename, 'r') as f:
      for line in f:
        m = header_pattern.search(line)
        if m:
          dep = m.group(1)
          if (dep in info) or (dep in third_parties):
            r['deps'].append(dep)
        if main_func_pattern.search(line):
          r['exec'] = True
        if gtest_pattern.search(line):
          r['exec'] = True
          r['test'] = True

# Generate cmakelists
cmakelists = dict()

for fp in info:
  r = info[fp]

  path = r["root"] + "/" + r["path"]
  if (path not in cmakelists):
    cmakelists[path] = { "libs": [], "deps": [], "subs": [], "tests": [] }

  cl = cmakelists[path]

  if 'exec' in r:
    lib_str = 'add_executable'
  else:
    lib_str = 'add_library'
  lib_str += '(' + ModuleName(r) + ' '

  if ('cpp' in r):
    lib_str += r["name"] + '.cpp'
  else:
    lib_str += EmptySrcName(paths[r["root"]], r)
  if ('hpp' in r):
    lib_str += ' ' + r["name"] + '.hpp'
  if ('h' in r):
    lib_str += ' ' + r["name"] + '.h'
  lib_str += ')'

  if 'test' in r:
    is_test = True
    lib_str = 'if(BUILD_TESTS)\n' + '  ' + lib_str + '\nendif()'
    test_str = 'if(BUILD_TESTS)\n  add_test' + '(' + ModuleName(r) + ' ' + ModuleName(r) + ')\nendif()'
    cl["tests"].append(test_str)
  else:
    is_test = False

  cl["libs"].append(lib_str)

  if ('deps' not in r):
    continue

  if is_test:
    spaces = '  '
  else:
    spaces = ''

  dep_items = []
  for dep in r['deps']:
    if dep in third_parties:
      for third_party_lib in third_parties[dep]:
        dep_items.append(spaces + '                      ' + third_party_lib)
    elif dep != fp:
      dep_items.append(spaces + '                      ' + ModuleName(info[dep]))

  if len(dep_items) > 0:
    dep_items = list(Set(dep_items))
    dep_items.sort()

    deps_str = ''
    if is_test:
      deps_str += 'if(BUILD_TESTS)\n'

    deps_str += spaces + 'target_link_libraries(' + ModuleName(r) + '\n'
    deps_str += '\n'.join(dep_items)
    deps_str += ')'

    if is_test:
      deps_str += '\nendif()'

    cl["deps"].append(deps_str)

# Identify subdirectories
for path in cmakelists:
  lastpos = len(path)
  while True:
    pos = path.rfind('/', 0, lastpos)
    if pos < 0:
      break

    subpath = path[:pos]
    if subpath in cmakelists:
      cl = cmakelists[subpath]
      sub_str = 'add_subdirectory(' + path[pos+1:] + ')'
      cl["subs"].append(sub_str)

    lastpos = pos

# Create CMakeLists.txt
for path in cmakelists:
  cl = cmakelists[path]
  subs = cl["subs"]
  subs.sort()
  libs = cl["libs"]
  libs.sort()
  deps = cl["deps"]
  deps.sort()
  tests = cl["tests"]
  tests.sort()

  with open(path + '/CMakeLists.txt', 'w') as f:
    if len(subs) > 0:
      f.write('\n'.join(subs))
      f.write('\n\n')

    f.write('# Declare micro-libs:\n')
    if len(libs) > 0:
      f.write('\n'.join(libs))
      f.write('\n')
    f.write('\n')

    f.write('# Link dependencies:\n')
    if len(deps) > 0:
      f.write('\n'.join(deps))
      f.write('\n')

    if len(tests) > 0:
      f.write('\n# Tests:\n')
      f.write('\n'.join(tests))
      f.write('\n')
