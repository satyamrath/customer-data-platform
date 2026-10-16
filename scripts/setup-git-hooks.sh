#!/usr/bin/env bash
set -e

git config core.hooksPath .githooks
chmod +x .githooks/*
echo "Git hooks configured successfully."
