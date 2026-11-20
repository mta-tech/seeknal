from __future__ import annotations

import inspect
import re
from pathlib import Path

import yaml

from seeknal.report_server import config as config_mod
from seeknal.report_server.config import ServerConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "deploy" / "docker-compose.report-server.yml"
DEPLOY_SCRIPT_PATH = REPO_ROOT / "deploy" / "deploy-report-server.sh"
DOCKER_README_PATH = REPO_ROOT / "docker" / "report-server" / "README.md"


def _parse_compose_env(entries: list[str] | dict[str, str]) -> dict[str, str]:
    if isinstance(entries, dict):
        return {str(k): str(v) for k, v in entries.items()}
    env_map: dict[str, str] = {}
    for entry in entries:
        key, _, val = str(entry).partition("=")
        env_map[key.strip()] = val.strip()
    return env_map


def _extract_default(expr: str) -> str:
    match = re.fullmatch(r"\$\{[A-Z0-9_]+:-(.*)\}", expr)
    assert match is not None, f"Expected compose default expansion ${{VAR:-default}}, got {expr!r}"
    return match.group(1)


def test_compose_file_auth_default_loopback_bind_and_env_vars_match_server_config(
    monkeypatch,
) -> None:
    raw = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    service = raw["services"]["report-server"]

    # 1. Port mapping binds 127.0.0.1 by default
    ports = service["ports"]
    assert len(ports) == 1
    assert "127.0.0.1" in ports[0]
    assert ports[0] == "${SEEKNAL_REPORT_SERVER_BIND_ADDRESS:-127.0.0.1}:8787:8787"

    # 2. Auth mode default is api_key (not open)
    env_map = _parse_compose_env(service["environment"])
    auth_mode_expr = env_map["SEEKNAL_REPORT_SERVER_AUTH_MODE"]
    auth_mode_default = _extract_default(auth_mode_expr)
    assert auth_mode_default != "open"
    assert auth_mode_default == "api_key"

    # 3. Every SEEKNAL_REPORT_SERVER_* env var in compose exists in ServerConfig.from_env
    from_env_source = inspect.getsource(ServerConfig.from_env)
    expected_limit_defaults = {
        "SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES": str(config_mod.DEFAULT_MAX_UPLOAD_BYTES),
        "SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS": str(config_mod.DEFAULT_MAX_TAR_MEMBERS),
        "SEEKNAL_REPORT_SERVER_MAX_UNPACKED_BYTES": str(config_mod.DEFAULT_MAX_UNPACKED_BYTES),
        "SEEKNAL_REPORT_SERVER_MAX_PATH_DEPTH": str(config_mod.DEFAULT_MAX_PATH_DEPTH),
        "SEEKNAL_REPORT_SERVER_MAX_MEMBER_NAME_LENGTH": str(config_mod.DEFAULT_MAX_MEMBER_NAME_LENGTH),
        "SEEKNAL_REPORT_SERVER_MAX_CONCURRENT_PUBLISHES": str(config_mod.DEFAULT_MAX_CONCURRENT_PUBLISHES),
    }
    for env_key, expected_default in expected_limit_defaults.items():
        assert env_key in env_map, f"Missing {env_key} in docker-compose.report-server.yml"
        assert _extract_default(env_map[env_key]) == expected_default

    for env_key, expr in env_map.items():
        assert env_key.startswith("SEEKNAL_REPORT_SERVER_")
        assert env_key in from_env_source, f"{env_key} is not read by ServerConfig.from_env()"
        default_val = _extract_default(expr)
        if default_val:
            monkeypatch.setenv(env_key, default_val)
        else:
            monkeypatch.delenv(env_key, raising=False)

    monkeypatch.setenv("SEEKNAL_REPORT_SERVER_KEYS", "compose-test-key")
    cfg = ServerConfig.from_env()
    assert cfg.auth_mode == "api_key"
    assert cfg.api_keys == ["compose-test-key"]
    assert cfg.max_upload_bytes == config_mod.DEFAULT_MAX_UPLOAD_BYTES
    assert cfg.max_tar_members == config_mod.DEFAULT_MAX_TAR_MEMBERS
    assert cfg.max_unpacked_bytes == config_mod.DEFAULT_MAX_UNPACKED_BYTES
    assert cfg.max_path_depth == config_mod.DEFAULT_MAX_PATH_DEPTH
    assert cfg.max_member_name_length == config_mod.DEFAULT_MAX_MEMBER_NAME_LENGTH
    assert cfg.max_concurrent_publishes == config_mod.DEFAULT_MAX_CONCURRENT_PUBLISHES


def test_deploy_script_and_readme_security_guards() -> None:
    script = DEPLOY_SCRIPT_PATH.read_text(encoding="utf-8")
    readme = DOCKER_README_PATH.read_text(encoding="utf-8")

    # 1. Script reads deploy/.env.report-server for the pre-deploy auth guard
    assert ".env.report-server" in script
    assert "SEEKNAL_REPORT_SERVER_KEYS" in script
    assert "SEEKNAL_REPORT_SERVER_AUTH_MODE" in script
    assert "SEEKNAL_REPORT_SERVER_BIND_ADDRESS" in script

    # 2. SSH options use StrictHostKeyChecking=accept-new and never StrictHostKeyChecking=no
    assert "StrictHostKeyChecking=accept-new" in script
    assert "StrictHostKeyChecking=no" not in script

    # 3. Tar bundle uses mktemp + EXIT trap, excludes .env*, extracts into a fresh seeknal-src
    #    (removing any stale deploy/.env.report-server), and streams remote .env with umask 077
    assert "mktemp" in script
    assert "trap " in script and "EXIT" in script
    assert "/tmp/seeknal-report-src.tar.gz" not in script
    assert "--exclude='.env'" in script
    assert "--exclude='.env.*'" in script
    assert "--exclude='deploy/.env.report-server'" in script
    assert "rm -rf $REMOTE_DIR/seeknal-src" in script
    assert "rm -f $REMOTE_DIR/seeknal-src/deploy/.env.report-server" in script
    assert 'umask 077; cat > $REMOTE_DIR/.env" < "$ENV_FILE"' in script

    # 4. Bounded health-check polling loop exits non-zero on failure before "Deployment Complete"
    health_loop_pos = script.find("seq 1 30")
    health_fail_exit_pos = script.find("exit 1", health_loop_pos)
    deploy_complete_pos = script.find("=== Deployment Complete ===")
    assert health_loop_pos != -1, "Expected bounded health-check loop (seq 1 30)"
    assert health_fail_exit_pos != -1, "Expected non-zero exit when health check fails"
    assert deploy_complete_pos != -1
    assert health_loop_pos < health_fail_exit_pos < deploy_complete_pos

    # 5. Plain-HTTP / TLS proxy note and .env.report-server parser / /healthz notes present
    assert "plain HTTP" in script
    assert "TLS" in script
    assert "plain HTTP" in readme
    assert "TLS" in readme
    assert ".env.report-server" in readme
    assert "/healthz" in readme
