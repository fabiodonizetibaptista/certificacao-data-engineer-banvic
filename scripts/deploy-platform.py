#!/usr/bin/env python3

"""
Secure launcher for the BanVic Terraform platform root.

PURPOSE
-------
Terraform owns the Kubernetes resources. This launcher is intentionally
restricted to responsibilities that Terraform should not perform itself:

1. validate operational preconditions before manipulating credentials;
2. obtain sensitive inputs without echoing them to the terminal;
3. reuse existing Kubernetes Secret values during normal reapplies;
4. transport sensitive values only in the environment of the Terraform
   subprocess;
5. prevent dangerous recovery states involving a persistent PostgreSQL PVC.

SECURITY BOUNDARY
-----------------
Sensitive values must NOT be persisted in:
- source code;
- .tfvars files;
- Terraform outputs;
- Git;
- the parent shell environment;
- launcher logs.

The corresponding Terraform variables are `sensitive` + `ephemeral` and are
written to Kubernetes through `kubernetes_secret_v1.data_wo`.

IMPORTANT
---------
`data_wo_revision` controls when Terraform rewrites Secret data. It is NOT a
complete credential-rotation workflow. PostgreSQL password rotation requires a
coordinated database operation and intentionally remains outside normal
bootstrap/reapply behavior.
"""

from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Repository and platform identity
# ---------------------------------------------------------------------------
# Resolve paths from this script rather than from the caller's working
# directory. This makes the launcher deterministic whether it is executed from
# the repository root, $HOME, CI, or another directory.
REPO_ROOT = Path(__file__).resolve().parents[1]
PLATFORM_DIR = REPO_ROOT / "infra" / "terraform" / "platform"

# The cluster root creates this kubeconfig. Using it explicitly prevents the
# platform deployment from depending on whichever context happens to be active
# in ~/.kube/config.
DEFAULT_KUBECONFIG = Path.home() / ".kube" / "banvic-local-config"
DEFAULT_CONTEXT = "kind-banvic-local"
DEFAULT_NAMESPACE = "banvic"


# ---------------------------------------------------------------------------
# Kubernetes object names
# ---------------------------------------------------------------------------
# Keep names centralized because the Terraform resources, Helm values and
# legacy bootstrap scripts must all agree on these contracts.
POSTGRES_SECRET = "postgres-secret"
AIRFLOW_RUNTIME_SECRET = "airflow-runtime-secret"
AIRFLOW_API_SECRET = "airflow-api-secret"
AIRFLOW_ADMIN_SECRET = "airflow-admin-secret"
POSTGRES_PVC = "postgres-data"


# ---------------------------------------------------------------------------
# Sensitive environment contract
# ---------------------------------------------------------------------------
# These TF_VAR values are permitted only in the child Terraform process.
# They must never be exported globally by the caller.
SENSITIVE_TF_VARS = (
    "TF_VAR_postgres_password",
    "TF_VAR_airflow_api_secret_key",
    "TF_VAR_airflow_admin_password",
)

# Refuse execution if sensitive values already leaked into the parent shell.
# This keeps the launcher as the single controlled transport mechanism.
FORBIDDEN_PARENT_SECRET_VARS = (
    *SENSITIVE_TF_VARS,
    "POSTGRES_PASSWORD",
    "AIRFLOW_API_SECRET_KEY",
    "AIRFLOW_ADMIN_PASSWORD",
)


class PreflightError(RuntimeError):
    """Expected operational/safety failure detected before Terraform runs."""


def run(
    command: list[str],
    *,
    env: Optional[dict[str, str]] = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    """
    Execute a command without invoking a shell.

    Avoiding shell=True is deliberate: values are never interpolated into a
    shell command string, which reduces quoting mistakes and accidental secret
    exposure.
    """
    return subprocess.run(
        command,
        cwd=PLATFORM_DIR,
        env=env,
        text=True,
        capture_output=capture_output,
        check=True,
    )


def ensure_binary(name: str) -> None:
    """Fail early when an operational dependency is unavailable."""
    if shutil.which(name) is None:
        raise PreflightError(
            f"Dependência obrigatória não encontrada: {name}"
        )


def reject_unsafe_logging() -> None:
    """
    Block Terraform debug logging while Secrets are in flight.

    Provider/debug logs may contain request payloads or other sensitive
    diagnostic data even when Terraform normally redacts sensitive values.
    """
    for name in ("TF_LOG", "TF_LOG_PROVIDER"):
        value = os.environ.get(name, "").strip()

        if value and value.upper() != "OFF":
            raise PreflightError(
                f"{name} está habilitado. Desabilite o debug logging "
                "antes de manipular Secrets."
            )


def reject_inherited_secrets() -> None:
    """
    Prevent accidental reuse of credentials exported in the caller's shell.

    The launcher must obtain or recover credentials itself so that the parent
    environment is not silently treated as a source of truth.
    """
    found = [
        name
        for name in FORBIDDEN_PARENT_SECRET_VARS
        if os.environ.get(name)
    ]

    if found:
        raise PreflightError(
            "Variáveis sensíveis já estão presentes no ambiente do shell. "
            "Remova-as antes de executar o launcher. "
            f"Variáveis detectadas: {', '.join(found)}"
        )


def kubectl_base(args: argparse.Namespace) -> list[str]:
    """
    Build every kubectl command with an explicit kubeconfig and context.

    This protects the baseline cluster from an accidental operation caused by
    the user's currently selected global Kubernetes context.
    """
    return [
        "kubectl",
        "--kubeconfig",
        str(args.kubeconfig),
        "--context",
        args.context,
    ]


def kubernetes_object_json(
    args: argparse.Namespace,
    resource_type: str,
    name: str,
) -> Optional[dict]:
    """
    Retrieve one Kubernetes object without printing it.

    `--ignore-not-found` lets the preflight model absence as a state instead of
    treating it as an exceptional kubectl failure.
    """
    command = kubectl_base(args)

    # Namespace is cluster-scoped. All other resources inspected here live in
    # the BanVic namespace.
    if resource_type != "namespace":
        command += ["--namespace", args.namespace]

    command += [
        "get",
        resource_type,
        name,
        "--ignore-not-found",
        "-o",
        "json",
    ]

    result = run(command, capture_output=True)

    if not result.stdout.strip():
        return None

    return json.loads(result.stdout)


def secret_values(
    args: argparse.Namespace,
    secret_name: str,
) -> Optional[Dict[str, str]]:
    """
    Read and decode a Kubernetes Secret entirely in process memory.

    Kubernetes returns Secret data as Base64. Decoding happens internally and
    no decoded value is printed, logged, written to disk or returned to the
    shell.
    """
    obj = kubernetes_object_json(
        args,
        "secret",
        secret_name,
    )

    if obj is None:
        return None

    encoded = obj.get("data", {})
    decoded: Dict[str, str] = {}

    for key, value in encoded.items():
        decoded[key] = base64.b64decode(value).decode("utf-8")

    return decoded


def require_keys(
    values: Dict[str, str],
    required: set[str],
    secret_name: str,
) -> None:
    """
    Validate Secret structure without exposing values.

    Reporting missing key names is safe and useful; reporting Secret contents
    is intentionally forbidden.
    """
    missing = sorted(required - set(values))

    if missing:
        raise PreflightError(
            f"Secret '{secret_name}' existe, mas não contém todas as "
            f"chaves obrigatórias. Chaves ausentes: {', '.join(missing)}"
        )


def prompt_secret_twice(label: str) -> str:
    """
    Obtain an initial credential without terminal echo.

    Double entry protects bootstrap from silently provisioning a mistyped
    password that would then become the persistent database credential.
    """
    first = getpass.getpass(f"{label}: ")
    second = getpass.getpass(
        f"Confirme {label.lower()}: "
    )

    if not first:
        raise PreflightError(
            f"{label} não pode ser vazio."
        )

    if first != second:
        raise PreflightError(
            f"Os valores informados para '{label}' diferem."
        )

    return first


def validate_postgres_pair(
    postgres: Dict[str, str],
    runtime: Dict[str, str],
) -> str:
    """
    Prove that postgres-secret and airflow-runtime-secret are consistent.

    These two Secrets form one credential domain. Allowing them to diverge
    would make PostgreSQL appear healthy while Airflow authentication fails.
    """
    require_keys(
        postgres,
        {
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
        },
        POSTGRES_SECRET,
    )

    require_keys(
        runtime,
        {
            "TARGET_POSTGRES_HOST",
            "TARGET_POSTGRES_PORT",
            "TARGET_POSTGRES_DATABASE",
            "TARGET_POSTGRES_USER",
            "TARGET_POSTGRES_PASSWORD",
            "AIRFLOW_CONN_BANVIC_DW",
        },
        AIRFLOW_RUNTIME_SECRET,
    )

    # Compare values internally, but never include either side in the error.
    if (
        postgres["POSTGRES_USER"]
        != runtime["TARGET_POSTGRES_USER"]
    ):
        raise PreflightError(
            "PostgreSQL e Airflow runtime possuem usuários divergentes."
        )

    if (
        postgres["POSTGRES_DB"]
        != runtime["TARGET_POSTGRES_DATABASE"]
    ):
        raise PreflightError(
            "PostgreSQL e Airflow runtime possuem databases divergentes."
        )

    if (
        postgres["POSTGRES_PASSWORD"]
        != runtime["TARGET_POSTGRES_PASSWORD"]
    ):
        raise PreflightError(
            "PostgreSQL e Airflow runtime possuem senhas divergentes."
        )

    # AIRFLOW_CONN_BANVIC_DW is consumed independently by Airflow. Validate it
    # against the discrete runtime keys so we do not preserve a structurally
    # valid but semantically inconsistent connection definition.
    try:
        connection = json.loads(
            runtime["AIRFLOW_CONN_BANVIC_DW"]
        )
    except json.JSONDecodeError as exc:
        raise PreflightError(
            "AIRFLOW_CONN_BANVIC_DW não contém JSON válido."
        ) from exc

    expected_consistency = (
        connection.get("conn_type") == "postgres"
        and connection.get("host")
        == runtime["TARGET_POSTGRES_HOST"]
        and connection.get("login")
        == runtime["TARGET_POSTGRES_USER"]
        and connection.get("password")
        == runtime["TARGET_POSTGRES_PASSWORD"]
        and connection.get("schema")
        == runtime["TARGET_POSTGRES_DATABASE"]
        and str(connection.get("port"))
        == runtime["TARGET_POSTGRES_PORT"]
    )

    if not expected_consistency:
        raise PreflightError(
            "AIRFLOW_CONN_BANVIC_DW diverge das demais chaves do "
            "airflow-runtime-secret."
        )

    # Returning only the password is intentional: Terraform rebuilds the
    # remaining derived values from non-sensitive variables.
    return postgres["POSTGRES_PASSWORD"]


def resolve_postgres_password(
    args: argparse.Namespace,
    pvc_exists: bool,
    postgres: Optional[Dict[str, str]],
    runtime: Optional[Dict[str, str]],
) -> str:
    """
    Apply the PostgreSQL recovery state machine.

    State tuple:
        (PVC exists, postgres-secret exists, runtime-secret exists)

    Allowed:
        F,F,F -> clean bootstrap
        F,T,T -> credentials preserved; persistent storage may be recreated
        T,T,T -> normal reapply

    Blocked:
        T,F,F -> PVC may contain a DB initialized with an unknown password
        every partial Secret pair -> inconsistent credential domain

    The critical safety rule is that a persistent PVC must never be paired
    automatically with a newly invented database password.
    """
    pg_exists = postgres is not None
    runtime_exists = runtime is not None

    state = (
        pvc_exists,
        pg_exists,
        runtime_exists,
    )

    if state == (False, False, False):
        print(
            "Preflight PostgreSQL: bootstrap inicial permitido "
            "(PVC e Secrets ausentes)."
        )
        return prompt_secret_twice(
            "Senha local do PostgreSQL"
        )

    if state == (False, True, True):
        print(
            "Preflight PostgreSQL: Secrets existentes e PVC ausente. "
            "Credenciais existentes serão preservadas."
        )
        return validate_postgres_pair(
            postgres,
            runtime,
        )

    if state == (True, True, True):
        print(
            "Preflight PostgreSQL: estado persistente consistente. "
            "Credenciais existentes serão reutilizadas."
        )
        return validate_postgres_pair(
            postgres,
            runtime,
        )

    if state == (True, False, False):
        raise PreflightError(
            "PVC PostgreSQL existe, mas os dois Secrets de credenciais "
            "estão ausentes. Recuperação manual obrigatória para evitar "
            "criação de uma senha incompatível com o banco persistido."
        )

    if state in {
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
    }:
        raise PreflightError(
            "Estado parcial/inconsistente entre postgres-secret e "
            "airflow-runtime-secret. Nenhuma alteração será realizada."
        )

    # Defensive branch: all eight Boolean combinations are intentionally
    # modeled above. Reaching this point means the implementation changed.
    raise PreflightError(
        "Estado PostgreSQL não reconhecido pelo preflight."
    )


def resolve_api_secret(
    args: argparse.Namespace,
    current: Optional[Dict[str, str]],
) -> str:
    """
    Preserve the Airflow API key by default.

    Re-generating it on every deployment could invalidate tokens and cause
    unnecessary component restarts. Initial bootstrap generates a
    cryptographically secure value only in memory.
    """
    if current is not None:
        require_keys(
            current,
            {"api-secret-key"},
            AIRFLOW_API_SECRET,
        )

        print(
            "Preflight Airflow API: chave existente será preservada."
        )
        return current["api-secret-key"]

    print(
        "Preflight Airflow API: nova chave será gerada em memória."
    )
    return secrets.token_urlsafe(64)


def resolve_admin_password(
    args: argparse.Namespace,
    current: Optional[Dict[str, str]],
) -> str:
    """
    Preserve the Airflow admin password on normal reapplies.

    Password rotation is an explicit operational action because the Helm
    createUserJob is responsible for synchronizing the credential with Airflow.
    """
    if current is not None:
        require_keys(
            current,
            {
                "AIRFLOW_ADMIN_ROLE",
                "AIRFLOW_ADMIN_USERNAME",
                "AIRFLOW_ADMIN_EMAIL",
                "AIRFLOW_ADMIN_FIRST_NAME",
                "AIRFLOW_ADMIN_LAST_NAME",
                "AIRFLOW_ADMIN_PASSWORD",
            },
            AIRFLOW_ADMIN_SECRET,
        )

        print(
            "Preflight Airflow admin: senha administrativa existente "
            "será preservada."
        )
        return current["AIRFLOW_ADMIN_PASSWORD"]

    print(
        "Preflight Airflow admin: credencial inicial necessária."
    )
    return prompt_secret_twice(
        "Senha do administrador do Airflow"
    )


def terraform_child_environment(
    *,
    postgres_password: str,
    api_secret: str,
    admin_password: str,
    args: argparse.Namespace,
) -> dict[str, str]:
    """
    Create an environment exclusively for Terraform child processes.

    We copy os.environ instead of mutating it. Therefore TF_VAR secret values
    never become part of the parent Python process environment or the caller's
    shell environment.

    Note: environment variables can still be observable by sufficiently
    privileged processes on the host while Terraform is running. The scope here
    is preventing persistence and accidental shell/session leakage.
    """
    child_env = dict(os.environ)

    # Sensitive values: ephemeral Terraform inputs.
    child_env["TF_VAR_postgres_password"] = (
        postgres_password
    )
    child_env["TF_VAR_airflow_api_secret_key"] = (
        api_secret
    )
    child_env["TF_VAR_airflow_admin_password"] = (
        admin_password
    )

    # The Kubernetes target used by Terraform MUST be identical to the target
    # already inspected by the kubectl preflight above. Without these explicit
    # variables, a CLI override could validate one cluster/context while
    # Terraform silently used the defaults from variables.tf and changed
    # another environment.
    child_env["TF_VAR_kubeconfig_path"] = str(args.kubeconfig)
    child_env["TF_VAR_kubernetes_context"] = args.context
    child_env["TF_VAR_namespace"] = args.namespace

    # Revisions are non-sensitive control metadata. They make an intentional
    # Secret rewrite explicit instead of tying rotation to a changed value.
    child_env["TF_VAR_postgres_credentials_revision"] = str(
        args.postgres_revision
    )
    child_env["TF_VAR_airflow_api_secret_revision"] = str(
        args.api_revision
    )
    child_env["TF_VAR_airflow_admin_secret_revision"] = str(
        args.admin_revision
    )

    return child_env


def terraform_plan(
    child_env: dict[str, str],
) -> None:
    """Run a read-only Terraform plan using the ephemeral child environment."""
    run(
        [
            "terraform",
            "plan",
            "-input=false",
        ],
        env=child_env,
    )


def terraform_apply(
    child_env: dict[str, str],
) -> None:
    """
    Apply exactly the plan that was reviewed/generated in this invocation.

    The saved plan is stored in a restrictive temporary directory and removed
    immediately afterwards. `data_wo` prevents the Secret values themselves
    from being persisted in the Terraform plan/state.
    """
    temp_dir = Path(
        tempfile.mkdtemp(
            prefix="banvic-platform-plan-"
        )
    )
    plan_path = temp_dir / "platform.tfplan"

    try:
        # Generate one immutable plan while the same ephemeral values are in
        # memory. This avoids plan/apply divergence.
        run(
            [
                "terraform",
                "plan",
                "-input=false",
                f"-out={plan_path}",
            ],
            env=child_env,
        )

        # Apply exactly the plan above rather than asking Terraform to
        # recalculate infrastructure intent.
        run(
            [
                "terraform",
                "apply",
                "-input=false",
                str(plan_path),
            ],
            env=child_env,
        )

    finally:
        # The plan should not contain data_wo values, but cleanup is still
        # performed immediately as defense in depth.
        try:
            plan_path.unlink(missing_ok=True)
            temp_dir.rmdir()
        except OSError:
            # Cleanup metadata must never hide the actual Terraform outcome.
            pass


def parse_args() -> argparse.Namespace:
    """Define the intentionally small operational surface of the launcher."""
    parser = argparse.ArgumentParser(
        description=(
            "Secure preflight and ephemeral-input launcher for the "
            "BanVic Terraform platform."
        )
    )

    parser.add_argument(
        "action",
        choices=("plan", "apply"),
        help="Terraform action executed after the secure preflight.",
    )

    parser.add_argument(
        "--kubeconfig",
        type=Path,
        default=DEFAULT_KUBECONFIG,
        help="Explicit kubeconfig produced by the Terraform cluster root.",
    )

    parser.add_argument(
        "--context",
        default=DEFAULT_CONTEXT,
        help="Explicit Kubernetes context targeted by the platform.",
    )

    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="Namespace containing BanVic platform resources.",
    )

    # Separate revision domains are deliberate:
    # - PostgreSQL + runtime must move together;
    # - Airflow API has an independent lifecycle;
    # - Airflow admin has an independent lifecycle.
    parser.add_argument(
        "--postgres-revision",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--api-revision",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--admin-revision",
        type=int,
        default=1,
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Any temporary files produced by this process should be private by
    # default. This specifically protects saved Terraform plan files.
    os.umask(0o077)

    try:
        # Fail before touching credentials when the local runtime is incomplete
        # or configured for unsafe verbose logging.
        ensure_binary("kubectl")
        ensure_binary("terraform")
        reject_unsafe_logging()
        reject_inherited_secrets()

        if not PLATFORM_DIR.is_dir():
            raise PreflightError(
                f"Terraform platform root não encontrado: {PLATFORM_DIR}"
            )

        if not args.kubeconfig.is_file():
            raise PreflightError(
                f"Kubeconfig não encontrado: {args.kubeconfig}"
            )

        # A missing namespace is a legitimate state for the very first
        # deployment of a clean cluster. Terraform remains the sole owner of
        # namespace creation; the launcher must not create it imperatively.
        namespace = kubernetes_object_json(
            args,
            "namespace",
            args.namespace,
        )

        if namespace is None:
            print(
                f"Preflight Kubernetes: namespace '{args.namespace}' ausente. "
                "Bootstrap inicial será realizado declarativamente pelo Terraform."
            )

            # Namespaced resources cannot exist before the namespace itself.
            # Model that state directly instead of issuing kubectl queries that
            # would fail against a namespace that has not been created yet.
            pvc_exists = False
            postgres_secret = None
            runtime_secret = None
            api_secret_current = None
            admin_secret_current = None
        else:
            # Read only the object states necessary to make the recovery
            # decision. No Secret value is printed.
            pvc_exists = (
                kubernetes_object_json(
                    args,
                    "pvc",
                    POSTGRES_PVC,
                )
                is not None
            )

            postgres_secret = secret_values(
                args,
                POSTGRES_SECRET,
            )
            runtime_secret = secret_values(
                args,
                AIRFLOW_RUNTIME_SECRET,
            )
            api_secret_current = secret_values(
                args,
                AIRFLOW_API_SECRET,
            )
            admin_secret_current = secret_values(
                args,
                AIRFLOW_ADMIN_SECRET,
            )

        # Resolve credentials in memory according to the current operational
        # state. Existing credentials are preserved whenever the state is safe.
        postgres_password = resolve_postgres_password(
            args,
            pvc_exists,
            postgres_secret,
            runtime_secret,
        )

        api_secret = resolve_api_secret(
            args,
            api_secret_current,
        )

        admin_password = resolve_admin_password(
            args,
            admin_secret_current,
        )

        # Sensitive TF_VAR values exist only in this dictionary and in the
        # Terraform subprocess environment.
        child_env = terraform_child_environment(
            postgres_password=postgres_password,
            api_secret=api_secret,
            admin_password=admin_password,
            args=args,
        )

        print(
            "Preflight concluído. Valores sensíveis permanecerão apenas "
            "na memória deste processo e no ambiente do subprocesso Terraform."
        )

        if args.action == "plan":
            terraform_plan(child_env)
        else:
            terraform_apply(child_env)

        return 0

    except PreflightError as exc:
        # Operational failures describe only state/metadata and never values.
        print(
            f"ERRO DE PREFLIGHT: {exc}",
            file=sys.stderr,
        )
        return 2

    except subprocess.CalledProcessError as exc:
        print(
            f"ERRO: comando externo terminou com código "
            f"{exc.returncode}.",
            file=sys.stderr,
        )
        return exc.returncode or 1

    finally:
        # Python cannot guarantee cryptographic zeroization of immutable
        # strings. Removing references is still worthwhile and process exit
        # releases the remaining memory shortly afterwards.
        if "child_env" in locals():
            for key in SENSITIVE_TF_VARS:
                child_env.pop(key, None)


if __name__ == "__main__":
    raise SystemExit(main())
