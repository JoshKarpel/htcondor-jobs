#!/usr/bin/env bash

set -e

pytest --cov

codecov -t f53345d1-71af-4dfa-ade6-16ce5bb3cba6
