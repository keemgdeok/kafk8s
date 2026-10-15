#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if (( $# == 0 )); then set -- test installDist labBundle; fi
docker run --rm --name kafk8s-build --label io.github.keemgdeok.kafk8s=build --memory 1g --cpus 2 \
  -v "$PWD:/workspace" -w /workspace -e GRADLE_USER_HOME=/workspace/.tools/gradle-home \
  eclipse-temurin:21-jdk@sha256:f9bf7e9fe2f9bc4657537c1a7e20c768f0c3bafca41598263be4714783dd62eb \
  ./gradlew --no-daemon "$@"
