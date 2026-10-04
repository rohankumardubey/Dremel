#!/usr/bin/env python3
"""Run the mandatory 64-query cross-engine validation without timing iterations."""

import subprocess
import sys
from pathlib import Path

script = Path(__file__).with_name("run_benchmark.py")
raise SystemExit(
    subprocess.call([sys.executable, str(script), "--validate-only", *sys.argv[1:]])
)
