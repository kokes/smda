"""
Extracts URLs from pd.read_csv in various notebooks

Notebooks downloaded from here
https://blog.jetbrains.com/datalore/2020/12/17/we-downloaded-10-000-000-jupyter-notebooks-from-github-this-is-what-we-learned/

aws s3 sync s3://github-notebooks-update1/ data/
"""

import os
import json
import re
import sys
from json.decoder import JSONDecodeError

dirname = sys.argv[1]
urls = set()
for filename in os.listdir(dirname):
    full_filename = os.path.join(dirname, filename)
    with open(full_filename, "rb") as f:
        try:
            nb = json.load(f)
        except (JSONDecodeError, UnicodeDecodeError):
            print("removing", full_filename)
            os.remove(full_filename)

        for cell in nb.get("cells", []):
            if cell["cell_type"] != "code":
                continue
            
            source = cell["source"]
            if not source:
                continue
            if isinstance(source, str):
                source = source.split("\n")

            for line in source:
                if "pd.read_csv" not in line:
                    continue
                if not ("https://" in line or "http://" in line):
                    continue
                url = re.findall(r"(https?://.+?)['\"]", line)[0]
                urls.add(url)

with open("urls.txt", "wt") as fw:
    for url in sorted(urls):
        fw.write(url)
        fw.write("\n")
