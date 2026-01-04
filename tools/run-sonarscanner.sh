#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SONAR_TOKEN:-}" ]]; then
  echo "SONAR_TOKEN environment variable is required." >&2
  exit 1
fi

if [[ -z "${SONAR_HOST_URL:-}" ]]; then
  echo "SONAR_HOST_URL environment variable is required." >&2
  exit 1
fi

SONAR_PROJECT_KEY=${SONAR_PROJECT_KEY:-"Rabbit_WorkerHost"}
SONAR_ORG=${SONAR_ORG:-""}

dotnet tool run dotnet-sonarscanner begin \
  /k:"${SONAR_PROJECT_KEY}" \
  /d:sonar.login="${SONAR_TOKEN}" \
  /d:sonar.host.url="${SONAR_HOST_URL}" \
  $( [[ -n "${SONAR_ORG}" ]] && printf '/o:%s' "${SONAR_ORG}" ) \
  /d:sonar.cs.vstest.reportsPaths="tests/WorkerHost.Tests/TestResults/*.trx" \
  /d:sonar.cs.opencover.reportsPaths="tests/WorkerHost.Tests/TestResults/coverage.opencover.xml"

dotnet build WorkerHostSolution.sln

dotnet test WorkerHostSolution.sln \
  /p:CollectCoverage=true \
  /p:CoverletOutputFormat=opencover \
  /p:CoverletOutput=tests/WorkerHost.Tests/TestResults/coverage

dotnet tool run dotnet-sonarscanner end /d:sonar.login="${SONAR_TOKEN}"
