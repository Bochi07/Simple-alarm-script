# -*- coding: utf-8 -*-
"""monitor-agent 测试总入口（无 pytest 依赖）。

用法：
  python3 tests/run_tests.py
  sudo bash install.sh test
"""
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import env  # noqa: E402

env.setup()

import test_alerting  # noqa: E402
import test_recovery  # noqa: E402
import test_skip_notify  # noqa: E402


def main() -> int:
    total = 0
    for mod in (test_alerting, test_skip_notify, test_recovery):
        print(f"\n== {mod.__name__} ==")
        total += mod.run()
    print(f"\n全部通过: {total} 项")
    return 0


if __name__ == "__main__":
    sys.exit(main())
