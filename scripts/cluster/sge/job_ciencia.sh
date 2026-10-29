#!/bin/bash
#$ -N wiki_ciencia
#$ -cwd
#$ -V
#$ -j y
#$ -o results/cluster_ciencia.log
#$ -l h_rt=04:00:00
#$ -pe smp 4
#$ -l m_mem_free=16G
#$ -binding linear:4

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "${SCRIPT_DIR}/run_task.sh" topic_ciencia podman
