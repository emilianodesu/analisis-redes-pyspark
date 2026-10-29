#!/bin/bash
#SBATCH --job-name=wiki_hits
#SBATCH --output=results/cluster_hits.log
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "${SCRIPT_DIR}/run_task.sh" hits podman
