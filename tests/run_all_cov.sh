#!/usr/bin/env sh
export AIIDA_PATH='.';
mkdir -p '.aiida';
pytest --cov-report=xml --cov=aiida_fleur
