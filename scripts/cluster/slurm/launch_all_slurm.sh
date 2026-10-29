#!/bin/bash
# Lanza los 4 cálculos paralelos en Slurm y encadena el reporte final con dependency=afterok
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p results

echo "Enviando 4 jobs paralelos a Slurm..."
J1=$(sbatch --parsable "${SCRIPT_DIR}/job_hits.slurm")
J2=$(sbatch --parsable "${SCRIPT_DIR}/job_ciencia.slurm")
J3=$(sbatch --parsable "${SCRIPT_DIR}/job_musica.slurm")
J4=$(sbatch --parsable "${SCRIPT_DIR}/job_deportes.slurm")

echo "Jobs enviados: HITS ($J1), Ciencia ($J2), Música ($J3), Deportes ($J4)"
echo "Enviando job de reporte encadenado con dependencia afterok..."
sbatch --dependency=afterok:$J1:$J2:$J3:$J4 "${SCRIPT_DIR}/job_report.slurm"

echo ""
echo "¡Los 5 trabajos han sido enviados exitosamente a Slurm!"
echo "Consulta el estado en cualquier momento con: squeue -u \$USER"
