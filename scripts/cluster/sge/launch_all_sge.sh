#!/bin/bash
# Lanza los 4 cálculos paralelos en un clúster SGE / Grid Engine y encadena el reporte final con hold_jid
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p results

echo "Enviando 4 jobs paralelos al gestor de colas..."
qsub "${SCRIPT_DIR}/job_hits.sh"
qsub "${SCRIPT_DIR}/job_ciencia.sh"
qsub "${SCRIPT_DIR}/job_musica.sh"
qsub "${SCRIPT_DIR}/job_deportes.sh"

echo "Enviando job de reporte encadenado (esperará a que terminen los 4 anteriores)..."
qsub -hold_jid wiki_hits,wiki_ciencia,wiki_musica,wiki_deportes "${SCRIPT_DIR}/job_report.sh"

echo ""
echo "¡Los 5 trabajos han sido enviados exitosamente!"
echo "Consulta el estado en cualquier momento con: qstat"
