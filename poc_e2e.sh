#!/usr/bin/env bash
# Local e2e for C128 — Klaw getConnectorOverview cross-team Connect secret disclosure + default Jasypt key
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../src/klaw-r29" && pwd)"
export JAVA_HOME="${JAVA_HOME:-/workspace/bounty-research/tools/jdk-21.0.13+9}"
export PATH="$JAVA_HOME/bin:$PATH"
cd "$ROOT"
./mvnw -pl core -Dspotless.check.skip=true -Dexec.skip=true \
  -Dtest=KafkaConnectControllerServiceTest#C128_getConnectorOverview_foreignTeam_leaksJasyptEncryptedSecrets+C128_getConnectorDetailsPerEnvToEdit_foreignTeam_stillBlocked \
  test
