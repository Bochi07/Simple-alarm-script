"""进程名 / 可执行名匹配逻辑单测（纯函数，无系统依赖）。"""
from __future__ import annotations

from collectors import _name_matches


class TestNameMatches:
    def test_exact_match(self):
        assert _name_matches(["nginx"], "nginx") is True

    def test_exact_no_substring_false_positive(self):
        # 精确匹配：nginx 不应再误匹配 nginx-foo
        assert _name_matches(["nginx"], "nginx-foo") is False

    def test_multi_names(self):
        assert _name_matches(["dockerd", "docker"], "docker") is True
        assert _name_matches(["dockerd", "docker"], "dockerd") is True

    def test_case_insensitive(self):
        assert _name_matches(["NGINX"], "nginx") is True

    def test_wildcard_prefix(self):
        assert _name_matches(["java*"], "java-foo") is True
        assert _name_matches(["java*"], "python") is False

    def test_question_mark(self):
        # ? 在 fnmatch 中恰好匹配一个字符
        assert _name_matches(["nginx: master?"], "nginx: master0") is True
        assert _name_matches(["nginx: master?"], "nginx: master") is False
