#!/usr/bin/env bash
# Fail when a release artifact (.tar.gz, .zip or .whl) holds anything that came from the Microsoft
# WS-Print / WS-Scan specification packages: schemas (.xsd, .wsdl), Word documents, the downloaded
# zips, or anything under docs/reference or an upstream/ directory.
#
# CI fetches those packages (Get-WsdSpecs.ps1 -AcceptLicense) only to run the tests; they are
# gitignored and are never part of a build. This is the last check before an artifact leaves CI.
#
# usage: check-artifacts.sh <file>...
set -euo pipefail

py=${PYTHON:-$(command -v python3 || command -v python)}
forbidden='\.(xsd|wsdl|docx?|zip)$|(^|/)docs/reference(/|$)|(^|/)upstream(/|$)'
status=0

list_zip() {
  "$py" -c 'import sys, zipfile; print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' "$1"
}

for f in "$@"; do
  case "$f" in
    *.tar.gz|*.tgz) names=$(tar -tzf "$f") ;;
    *.zip|*.whl) names=$(list_zip "$f") ;;
    *) continue ;;
  esac
  hits=$(printf '%s\n' "$names" | grep -Ei "$forbidden" || true)
  count=$(printf '%s\n' "$names" | grep -c . || true)
  if [[ -n "$hits" ]]; then
    echo "::error::$f contains Microsoft specification files:"
    printf '%s\n' "$hits"
    status=1
  else
    echo "ok: $f ($count entries, no specification files)"
  fi
done
exit "$status"
