#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
go test -p=2 -count=1 -v "$@" \
  pkg/scalers/scaler.go \
  pkg/scalers/groundcover_scaler.go \
  pkg/scalers/groundcover_scaler_test.go
