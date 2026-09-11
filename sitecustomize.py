"""Ajusta o runtime Java usado pelo projeto antes de iniciar o Spark.

O Spark 3.5.x trabalha corretamente com JDK 8/11/17/21, mas falha em ambientes
que defaultam para Java 25, onde o suporte a javax.security.auth.Subject foi
removido. Esta correção seleciona automaticamente um JDK compatível quando o
projeto é executado a partir da raiz.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _java_major(version_output: str) -> str | None:
    for marker in ("version \"", "version \"21", "version \"17", "version \"11", "version \"8"):
        if marker in version_output:
            for token in ("21", "17", "11", "8"):
                if f'version "{token}' in version_output:
                    return token
    return None


def _is_compatible_java_home(java_home: Path) -> bool:
    java_bin = java_home / "bin" / "java"
    if not java_bin.exists():
        return False

    try:
        result = subprocess.run(
            [str(java_bin), "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except OSError:
        return False

    version_output = result.stdout or ""
    return _java_major(version_output) in {"8", "11", "17", "21"}


def _choose_java_home() -> str | None:
    explicit_home = os.environ.get("JAVA_HOME")
    if explicit_home:
        candidate = Path(explicit_home)
        if _is_compatible_java_home(candidate):
            return str(candidate)

    candidates: list[Path] = []
    sdkman_dir = Path("/usr/local/sdkman/candidates/java")
    if sdkman_dir.exists():
        for preferred in ("21", "17", "11", "8"):
            matches = sorted(
                [path for path in sdkman_dir.iterdir() if path.is_dir() and preferred in path.name],
                key=lambda path: path.name,
                reverse=True,
            )
            candidates.extend(matches)

    for base_dir in (
        Path("/usr/lib/jvm"),
        Path("/usr/lib/jvm/default-java"),
        Path("/opt"),
    ):
        if base_dir.exists():
            candidates.extend([path for path in base_dir.rglob("bin") if path.parent.name == "bin"])

    seen: set[str] = set()
    for candidate in candidates:
        java_home = candidate.parent if candidate.name == "bin" else candidate
        key = str(java_home)
        if key in seen:
            continue
        seen.add(key)
        if _is_compatible_java_home(java_home):
            return str(java_home)

    return None


def _configure_java_home() -> None:
    chosen = _choose_java_home()
    if not chosen:
        return

    os.environ["JAVA_HOME"] = chosen
    os.environ["PATH"] = str(Path(chosen) / "bin") + os.pathsep + os.environ.get("PATH", "")


_configure_java_home()
