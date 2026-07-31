#!/bin/bash

# Get to a predictable directory, the directory of this script.
cd "$(dirname "$0")" || exit 1

TLE_ENV_FILE="${TLE_ENV_FILE:-environment}"

# Build/update helpers never need bot credentials. Strip both inherited values
# and values that will later be sourced from the private environment file.
without_bot_secrets() {
    env \
        -u BOT_TOKEN \
        -u GEMINI_API_KEYS \
        -u XAI_API_KEY \
        -u XAI_API_KEYS \
        -u CF_API_KEY \
        -u CF_API_SECRET \
        -u ODDS_API_KEY \
        -u FOOTBALL_DATA_API_KEY \
        "$@"
}

environment_mode() {
    without_bot_secrets stat -c '%a' "$1" 2>/dev/null ||
        without_bot_secrets stat -f '%Lp' "$1" 2>/dev/null
}

validate_environment_file() {
    [[ -e "${TLE_ENV_FILE}" ]] || return 0
    if [[ ! -f "${TLE_ENV_FILE}" ]]; then
        echo "Refusing non-file environment path: ${TLE_ENV_FILE}" >&2
        exit 1
    fi
    local mode
    mode="$(environment_mode "${TLE_ENV_FILE}")" || {
        echo "Could not inspect ${TLE_ENV_FILE} permissions." >&2
        exit 1
    }
    case "${mode}" in
        400|600) ;;
        *)
            echo "Refusing ${TLE_ENV_FILE} with mode ${mode}; run: chmod 600 ${TLE_ENV_FILE}" >&2
            exit 1
            ;;
    esac
}

file_venv_dir() {
    [[ -f "${TLE_ENV_FILE}" ]] || return 0
    # Preserve the template's launcher setting without sourcing the file (and
    # therefore its credentials) into a setup process. Only literal quoted or
    # unquoted values are accepted; shell expansion belongs in the caller.
    local line value=''
    while IFS= read -r line; do
        case "${line}" in
            'export VENV_DIR='*) value="${line#export VENV_DIR=}" ;;
            'VENV_DIR='*) value="${line#VENV_DIR=}" ;;
            *) continue ;;
        esac
    done < "${TLE_ENV_FILE}"
    if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
        value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
        value="${value:1:${#value}-2}"
    fi
    printf '%s' "${value}"
}

run_bot() {
    local bot_python="$1"
    if [[ -f "${TLE_ENV_FILE}" ]]; then
        # Only this process receives file-backed credentials. `exec` replaces
        # the shell so no secret-bearing launcher remains after the bot exits.
        (
            set -a
            . "${TLE_ENV_FILE}"
            set +a
            exec "${bot_python}" -m tle
        )
    else
        "${bot_python}" -m tle
    fi
}

validate_environment_file
if [[ -z "${VENV_DIR:-}" ]]; then
    VENV_DIR="$(file_venv_dir)" || exit 1
fi

if [[ -n "${VENV_DIR}" ]]; then
    echo "Activating virtual environment in ${VENV_DIR}."
    without_bot_secrets python3 -m venv "${VENV_DIR}"
    . "${VENV_DIR}/bin/activate"
fi

bootstrap_cairo() {
    if [[ "${TLE_CAIRO_BOOTSTRAP:-1}" == "0" ]]; then
        return
    fi

    local cairo_exports
    if cairo_exports="$(without_bot_secrets poetry run python -m tle.util.cairo_bootstrap)"; then
        while IFS= read -r cairo_export; do
            case "${cairo_export}" in
                LD_LIBRARY_PATH=*|PKG_CONFIG_PATH=*|TLE_ALLOW_COLOR_EMOJI=1)
                    export "${cairo_export}"
                    ;;
                "")
                    ;;
                *)
                    echo "Ignoring unexpected Cairo bootstrap output: ${cairo_export}" >&2
                    ;;
            esac
        done <<< "${cairo_exports}"
    else
        echo 'Cairo bootstrap helper failed; continuing with default Cairo.' >&2
    fi
}

while true; do
    without_bot_secrets git pull
    without_bot_secrets poetry install
    bootstrap_cairo
    BOT_PYTHON="$(without_bot_secrets poetry env info --executable)" || exit 1
    if [[ ! -x "${BOT_PYTHON}" ]]; then
        echo 'Poetry did not return an executable bot interpreter.' >&2
        exit 1
    fi
    run_bot "${BOT_PYTHON}"

    echo '==================================================================='
    echo '=                       Restarting                                ='
    echo '==================================================================='
done
