import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Cho phep chay pytest tu thu muc goc ma khong can cai package
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("FEATURE_SPEC_PATH", str(ROOT / "config" / "features" / "feature_spec.yml"))
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("SERVICE_NAME", "pytest")
