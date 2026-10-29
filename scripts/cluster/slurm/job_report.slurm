#!/bin/bash
#SBATCH --job-name=wiki_report
#SBATCH --output=results/cluster_report.log
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "${SCRIPT_DIR}/run_task.sh" finalize podman
