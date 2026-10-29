#!/bin/bash
# Configuración base para ejecución en clúster / HPC
# Estas variables pueden personalizarse o sobreescribirse desde el entorno antes de lanzar los trabajos.

export WORK_DIR="${WORK_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
export SCRATCH_DIR="${SCRATCH_DIR:-/var/tmp}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-48g}"
export PYSPARK_SUBMIT_ARGS="--driver-memory ${SPARK_DRIVER_MEMORY} pyspark-shell"
export _JAVA_OPTIONS="-Xmx${SPARK_DRIVER_MEMORY}"
export CONTAINER_IMAGE="${CONTAINER_IMAGE:-spark-analisis:latest}"
export CONTAINER_TAR="${CONTAINER_TAR:-${WORK_DIR}/spark-analisis.tar}"
export NSLOTS="${NSLOTS:-4}"
