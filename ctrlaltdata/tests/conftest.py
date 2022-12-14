from ..config import enabled_modules

collect_ignore_glob = []
collect_ignore = []
public_modules = ['qad', 'compustat', 'fred']

for module in public_modules:
    # do not test module files if module not enabled
    if module not in enabled_modules:
        collect_ignore_glob.append(f"unit/test_{module}/*.py")

    # if none of qad and compustat enabled, do not test api or units module
    if 'qad' not in enabled_modules and 'compustat' not in enabled_modules:
        collect_ignore += ['unit/test_api.py', 'unit/test_units.py', 'unit/test_panel_constructors.py']
