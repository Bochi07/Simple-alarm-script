"""pytest 根路径注入：确保能 import 仓库根目录下的业务模块。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
