#!/usr/bin/env python3
import os
import sys

GX_DIR = os.path.join(os.path.dirname(__file__), "..", "data_quality")

os.chdir(GX_DIR)

try:
    import great_expectations as gx
except ImportError:
    print("Installing great_expectations...")
    os.system(f"{sys.executable} -m pip install great_expectations==0.18.19 -q")
    import great_expectations as gx

print(f"Great Expectations version: {gx.__version__}")
print(f"Working dir: {os.getcwd()}")

context = gx.get_context(context_root_dir=GX_DIR)

print("\nBuilding Data Docs from existing expectation suites...")
context.build_data_docs()

docs_path = os.path.join(GX_DIR, "uncommitted", "data_docs", "local_site", "index.html")

if os.path.exists(docs_path):
    print(f"\nData Docs generated successfully.")
    print(f"Location: {docs_path}")
    print("\nNginx will serve it at: http://localhost:8085")
    print("Restart the data-docs container if nginx was already running:")
    print("  docker restart streamflow-data-docs")
else:
    print(f"\nERROR: index.html not found at expected location: {docs_path}")
    print("Check the GX context root directory configuration.")
    sys.exit(1)
