import os
luts_path = dict()
luts_path['parent'] = os.path.join(os.path.dirname(os.path.dirname(__file__)),'luts')
luts_path['station'] = os.path.join(luts_path.get('parent'),'station')
luts_path['code_tables'] = os.path.join(luts_path.get('parent'),'code_tables')
