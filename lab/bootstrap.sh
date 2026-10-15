#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) asset=kind-darwin-arm64 ;;
  Linux-aarch64|Linux-arm64) asset=kind-linux-arm64 ;;
  Linux-x86_64) asset=kind-linux-amd64 ;;
  *) echo 'Use an official kind 0.33.0 binary for this platform.' >&2; exit 2 ;;
esac
mkdir -p .tools
curl -fsSL --max-time 90 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/$asset" -o .tools/kind
curl -fsSL --max-time 30 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/$asset.sha256sum" -o .tools/kind.sha256sum
expected=$(awk '{print $1}' .tools/kind.sha256sum)
printf '%s  .tools/kind\n' "$expected" | shasum -a 256 -c -
chmod +x .tools/kind
curl -fsSL --max-time 90 https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.3.0/strimzi-1.3.0.tar.gz -o .tools/strimzi.tar.gz
printf '%s  .tools/strimzi.tar.gz\n' 0b87f42a08032467f41ca5e9cf778aad52e59111e09ae425b94755d08da380e7 | shasum -a 256 -c -
tar -xzf .tools/strimzi.tar.gz -C .tools
docker pull kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
docker pull eclipse-temurin:21-jdk@sha256:f9bf7e9fe2f9bc4657537c1a7e20c768f0c3bafca41598263be4714783dd62eb
docker pull quay.io/strimzi/kafka:1.3.0-kafka-4.3.1@sha256:1f189b1f0f32684302d8b018b1d21b411e2469a9c465fbdc18fa3599cb5bbdad
