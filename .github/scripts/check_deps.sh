#!/usr/bin/env bash

# Checks that the dependencies and extras declared in pixi.toml are mirrored in pyproject.toml.

set -euo pipefail

exit_code=0

# conda package names that differ from their PyPI counterpart
pypi_name() {
    case "$1" in
    msgpack-python) echo "msgpack" ;;
    python-duckdb) echo "duckdb" ;;
    ibm_db) echo "ibm-db" ;;
    ibm_db_sa) echo "ibm-db-sa" ;;
    ibis-mssql) echo "ibis-framework[mssql]" ;;
    ibis-postgres) echo "ibis-framework[postgres]" ;;
    *) echo "$1" ;;
    esac
}

# dependencies that only exist to work around conda-forge packaging issues and
# therefore must not end up in the published PyPI metadata
is_conda_only() {
    case "$1" in
    # python is provided by the interpreter, see requires-python
    python) return 0 ;;
    # only needed because conda-forge's bcpandas still imports pkg_resources
    setuptools) return 0 ;;
    *) return 1 ;;
    esac
}

# $1: pixi.toml table, $2: pyproject.toml array, $3: description used in error messages
check_dependencies() {
    local pixi_table="$1" pyproject_array="$2" description="$3"
    local expected=0

    while read -r package version; do
        if is_conda_only "${package}"; then
            continue
        fi
        expected=$((expected + 1))
        dependency="$(pypi_name "${package}") ${version}"
        if [[ $(yq -r "${pyproject_array} | map(. == \"${dependency}\") | any" pyproject.toml) == "false" ]]; then
            echo "${description}: '${dependency}' is missing in pyproject.toml"
            exit_code=1
        fi
    done < <(yq -r "${pixi_table} | to_entries | .[] | \"\(.key) \(.value)\"" pixi.toml)

    # catches dependencies that were removed from pixi.toml but are still in pyproject.toml
    local actual
    actual=$(yq -r "${pyproject_array} | length" pyproject.toml)
    if [[ ${actual} != "${expected}" ]]; then
        echo "${description}: pyproject.toml declares ${actual} dependencies, but pixi.toml declares ${expected}"
        exit_code=1
    fi
}

check_dependencies '.package.run-dependencies' '.project.dependencies' 'dependencies'

while read -r extra; do
    if [[ $(yq -r ".project.optional-dependencies | has(\"${extra}\")" pyproject.toml) == "false" ]]; then
        echo "extra '${extra}': missing in [project.optional-dependencies] of pyproject.toml"
        exit_code=1
        continue
    fi
    check_dependencies \
        ".package.extra-dependencies.\"${extra}\"" \
        ".project.optional-dependencies.\"${extra}\"" \
        "extra '${extra}'"
done < <(yq -r '.package.extra-dependencies | keys | .[]' pixi.toml)

# catches extras that were removed from pixi.toml but are still in pyproject.toml
while read -r extra; do
    if [[ $(yq -r ".package.extra-dependencies | has(\"${extra}\")" pixi.toml) == "false" ]]; then
        echo "extra '${extra}': not defined in [package.extra-dependencies] of pixi.toml"
        exit_code=1
    fi
done < <(yq -r '.project.optional-dependencies // {} | keys | .[]' pyproject.toml)

exit "${exit_code}"
