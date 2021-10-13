#!/usr/bin/env bash
rm -f *.svg
rm -f *.yaml
python main.py
generators/yml2dot/yml2dot.exe delta_dependency_specs.yaml | dot -Tsvg > delta_dependency_specs.svg
generators/yml2dot/yml2dot.exe processed_dependency_specs.yaml | dot -Tsvg > processed_dependency_specs.svg
generators/yml2dot/yml2dot.exe stage_dependency_specs.yaml | dot -Tsvg > stage_dependency_specs.svg
generators/yml2dot/yml2dot.exe raw_dependency_specs.yaml | dot -Tsvg > raw_dependency_specs.svg