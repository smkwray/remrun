"""Run an isolated two-instance Syncthing race check for remrun's verify-only policy.

This script is intentionally standalone and stdlib-only. It starts two local Syncthing
instances with separate homes and folders, connects them through the REST API, writes a payload on
the producer side, and waits for the consumer side to receive the same bytes. remrun does not write
to the consumer folder during the run; the assertion is that Syncthing converges without
``~syncthing~`` temp leftovers or ``sync-conflict`` files.

It is a live harness, not a default unit test. Use:

    python scripts/syncthing_race_harness.py --syncthing syncthing
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any


NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


_HELP_CACHE: dict[tuple[str, tuple[str, ...]], str] = {}


@dataclass
class Instance:
    name: str
    home: Path
    folder: Path
    gui_port: int
    proc: subprocess.Popen
    api_key: str = ""
    device_id: str = ""

    @property
    def api_base(self) -> str:
        return f"http://127.0.0.1:{self.gui_port}"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _help_text(syncthing: str, *args: str) -> str:
    key = (syncthing, args)
    if key in _HELP_CACHE:
        return _HELP_CACHE[key]
    try:
        proc = subprocess.run([syncthing, *args, "--help"], check=False,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        text = ""
    else:
        text = proc.stdout or ""
    _HELP_CACHE[key] = text
    return text


def _supports_serve_flag(syncthing: str, flag: str) -> bool:
    return flag in _help_text(syncthing)


def build_start_args(syncthing: str, home: Path, port: int) -> list[str]:
    args = [
        syncthing,
        "--home", str(home),
        "--no-browser",
        "--no-restart",
        "--no-upgrade",
        f"--gui-address=127.0.0.1:{port}",
    ]
    if _supports_serve_flag(syncthing, "--no-default-folder"):
        args.append("--no-default-folder")
    return args


def litter_paths(*roots: Path) -> list[Path]:
    out: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            name = p.name.lower()
            if name.startswith("~syncthing~") or "sync-conflict" in name:
                out.append(p)
    return sorted(out)


def api(inst: Instance, method: str, path: str, data: Any | None = None) -> Any:
    body = None if data is None else json.dumps(data).encode("utf-8")
    req = urllib.request.Request(inst.api_base + path, data=body, method=method)
    req.add_header("X-API-Key", inst.api_key)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def read_api_key(home: Path) -> str:
    cfg = home / "config.xml"
    if not cfg.exists():
        return ""
    try:
        root = ET.parse(cfg).getroot()
        key = root.findtext("./gui/apikey") or ""
        return key.strip()
    except (ET.ParseError, OSError):
        return ""


def start_instance(name: str, syncthing: str, root: Path) -> Instance:
    home = root / f"{name}-home"
    folder = root / f"{name}-folder"
    home.mkdir(parents=True, exist_ok=True)
    folder.mkdir(parents=True, exist_ok=True)
    port = free_port()
    log = (root / f"{name}.log").open("wb")
    args = build_start_args(syncthing, home, port)
    proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT,
                            creationflags=NO_WINDOW)
    return Instance(name=name, home=home, folder=folder, gui_port=port, proc=proc)


def wait_ready(inst: Instance, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if inst.proc.poll() is not None:
            raise RuntimeError(f"{inst.name} exited early with code {inst.proc.returncode}")
        key = read_api_key(inst.home)
        if key:
            inst.api_key = key
            try:
                status = api(inst, "GET", "/rest/system/status")
                inst.device_id = str(status["myID"])
                return
            except (urllib.error.URLError, KeyError, json.JSONDecodeError):
                pass
        time.sleep(0.5)
    raise TimeoutError(f"{inst.name} did not become API-ready within {timeout_s}s")


def default_object(inst: Instance, kind: str) -> dict[str, Any]:
    return dict(api(inst, "GET", f"/rest/config/defaults/{kind}"))


def connect_pair(a: Instance, b: Instance, folder_id: str) -> None:
    for left, right in ((a, b), (b, a)):
        dev = default_object(left, "device")
        dev.update({
            "deviceID": right.device_id,
            "name": right.name,
            "addresses": ["dynamic"],
            "autoAcceptFolders": False,
            "introducer": False,
        })
        api(left, "POST", "/rest/config/devices", dev)

        folder = default_object(left, "folder")
        folder.update({
            "id": folder_id,
            "label": "remrun-race",
            "path": str(left.folder),
            "type": "sendreceive",
            "rescanIntervalS": 1,
            "fsWatcherEnabled": True,
            "devices": [{"deviceID": left.device_id}, {"deviceID": right.device_id}],
        })
        api(left, "POST", "/rest/config/folders", folder)


def wait_for_file(path: Path, expected_hash: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists() and path.is_file():
            try:
                if sha256(path) == expected_hash:
                    return
            except OSError:
                pass
        time.sleep(0.5)
    got = sha256(path) if path.exists() and path.is_file() else "(missing)"
    raise TimeoutError(f"{path} did not converge to expected hash; got {got}")


def write_payload(path: Path, size_mib: int) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = hashlib.sha256(b"remrun-syncthing-race").digest()
    remaining = size_mib * 1024 * 1024
    with path.open("wb") as f:
        while remaining > 0:
            chunk = (block * min(4096, max(1, remaining // len(block) + 1)))[:remaining]
            f.write(chunk)
            remaining -= len(chunk)
    return sha256(path)


def shutdown_instance(inst: Instance) -> None:
    key = inst.api_key or read_api_key(inst.home)
    if not key:
        return
    inst.api_key = key
    try:
        api(inst, "POST", "/rest/system/shutdown")
    except Exception:  # noqa: BLE001 - cleanup is best-effort
        pass


def remove_tree_retry(path: Path, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while True:
        shutil.rmtree(path, ignore_errors=True)
        if not path.exists() or time.time() >= deadline:
            return
        time.sleep(0.25)


def run_harness(*, syncthing: str, workdir: Path, timeout_s: float = 90.0,
                size_mib: int = 16, keep: bool = False) -> dict[str, Any]:
    syncthing_bin = shutil.which(syncthing) or (syncthing if Path(syncthing).exists() else "")
    if not syncthing_bin:
        raise FileNotFoundError(f"syncthing binary not found: {syncthing!r}")

    workdir.mkdir(parents=True, exist_ok=True)
    a = start_instance("producer", syncthing_bin, workdir)
    b = start_instance("consumer", syncthing_bin, workdir)
    instances = [a, b]
    try:
        for inst in instances:
            wait_ready(inst, timeout_s)
        connect_pair(a, b, "remrun-race")

        # Seed a small file first so we know the pair is actually connected before the race payload.
        seed_hash = write_payload(a.folder / "seed.bin", 1)
        wait_for_file(b.folder / "seed.bin", seed_hash, timeout_s)

        payload_hash = write_payload(a.folder / "out" / "race-output.bin", size_mib)
        wait_for_file(b.folder / "out" / "race-output.bin", payload_hash, timeout_s)

        litter = litter_paths(a.folder, b.folder)
        if litter:
            raise RuntimeError("Syncthing litter found: " + ", ".join(str(p) for p in litter))

        return {
            "ok": True,
            "producer": str(a.folder),
            "consumer": str(b.folder),
            "payload_mib": size_mib,
            "payload_sha256": payload_hash,
            "litter": [],
        }
    finally:
        for inst in instances:
            shutdown_instance(inst)
        time.sleep(0.5)
        for inst in instances:
            if inst.proc.poll() is None:
                inst.proc.terminate()
        for inst in instances:
            try:
                inst.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                inst.proc.kill()
        if not keep:
            remove_tree_retry(workdir)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--syncthing", default=os.environ.get("SYNCTHING_BIN", "syncthing"))
    p.add_argument("--workdir", type=Path,
                   default=Path(tempfile.mkdtemp(prefix="remrun-syncthing-race-")))
    p.add_argument("--timeout-s", type=float, default=90.0)
    p.add_argument("--size-mib", type=int, default=16)
    p.add_argument("--keep", action="store_true", help="keep temp homes/folders/logs")
    args = p.parse_args(argv)
    try:
        result = run_harness(syncthing=args.syncthing, workdir=args.workdir,
                             timeout_s=args.timeout_s, size_mib=args.size_mib,
                             keep=args.keep)
    except Exception as exc:  # noqa: BLE001 - CLI harness should report concise failure
        print(json.dumps({"ok": False, "error": str(exc), "workdir": str(args.workdir)},
                         indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
