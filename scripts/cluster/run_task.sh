#!/bin/bash
# Ejecutor genérico de subtareas de análisis para nodos de cómputo en clúster
# Uso: ./run_task.sh <target> [podman|direct]
# Subtareas válidas: hits, topic_ciencia, topic_musica, topic_deportes, finalize
set -euo pipefail

TARGET="${1:?Debes especificar la subtarea (hits, topic_ciencia, topic_musica, topic_deportes, finalize)}"
MODE="${2:-podman}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/cluster_env.sh"

JOB_IDENTIFIER="${JOB_ID:-${SLURM_JOB_ID:-$$}}"
TMP_SPARK="${SCRATCH_DIR}/spark-tmp-${TARGET}-${JOB_IDENTIFIER}"
mkdir -p "${TMP_SPARK}"

echo "=== Iniciando subtarea '${TARGET}' en $(hostname) a las $(date) ==="
echo "Directorio de trabajo: ${WORK_DIR}"
echo "Memoria de Driver Spark: ${SPARK_DRIVER_MEMORY}"

if [ "${MODE}" = "podman" ]; then
    LOCAL_ROOT="${SCRATCH_DIR}/podman-${USER:-user}-${JOB_IDENTIFIER}"
    LOCAL_RUN="${SCRATCH_DIR}/podman-run-${USER:-user}-${JOB_IDENTIFIER}"
    mkdir -p "${LOCAL_ROOT}" "${LOCAL_RUN}"

    cleanup() {
        echo "Limpiando almacenamiento temporal..."
        rm -rf "${TMP_SPARK}"
        podman unshare rm -rf "${LOCAL_ROOT}" "${LOCAL_RUN}" 2>/dev/null || rm -rf "${LOCAL_ROOT}" "${LOCAL_RUN}" 2>/dev/null || true
    }
    trap cleanup EXIT

    POD="podman --root ${LOCAL_ROOT} --runroot ${LOCAL_RUN}"

    if [ -f "${CONTAINER_TAR}" ]; then
        echo "Cargando imagen de contenedor local..."
        ${POD} load -i "${CONTAINER_TAR}"
    fi

    echo "Ejecutando contenedor..."
    CMD="python3 run.py wikipedia --target ${TARGET}"
    if [ "${TARGET}" = "finalize" ]; then
        CMD="python3 run.py wikipedia --target finalize && python3 status.py"
    fi

    ${POD} run --rm \
      --net=host \
      --tmpfs /tmp:rw,exec \
      --tmpfs /var/tmp:rw,exec \
      -v "${WORK_DIR}:${WORK_DIR}" \
      -w "${WORK_DIR}" \
      -e NSLOTS="${NSLOTS}" \
      -e TMPDIR=/var/tmp -e SPARK_LOCAL_DIRS="${TMP_SPARK}" \
      -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY}" \
      -e PYSPARK_SUBMIT_ARGS="--driver-memory ${SPARK_DRIVER_MEMORY} pyspark-shell" \
      -e _JAVA_OPTIONS="-Xmx${SPARK_DRIVER_MEMORY}" \
      -e MKL_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 -e OMP_NUM_THREADS=1 \
      "${CONTAINER_IMAGE}" \
      bash -c "${CMD}"

else
    # Ejecución nativa directa (sin contenedor)
    cleanup() {
        rm -rf "${TMP_SPARK}"
    }
    trap cleanup EXIT

    export TMPDIR="${SCRATCH_DIR}"
    export SPARK_LOCAL_DIRS="${TMP_SPARK}"
    cd "${WORK_DIR}"

    if [ "${TARGET}" = "finalize" ]; then
        python3 run.py wikipedia --target finalize
        python3 status.py
    else
        python3 run.py wikipedia --target "${TARGET}"
    fi
fi

echo "=== Subtarea '${TARGET}' completada con éxito a las $(date) ==="
