# Examples package 

import importlib, sys
# Expose json_generator as a top-level module name so example scripts can simply `import json_generator`
if 'json_generator' not in sys.modules:
    sys.modules['json_generator'] = importlib.import_module(__name__ + '.json_generator') 