# Guía de Despliegue Opcional en Clúster / HPC

Este directorio contiene scripts y plantillas para desplegar opcionalmente el pipeline de grafos a gran escala (PageRank, HITS y Topic PageRank sobre Wikipedia) en entornos de cómputo de alto rendimiento (**HPC**), ya sea mediante gestores de colas (**Slurm**, **SGE / Grid Engine**) o directamente mediante contenedores (**Podman / Docker**).

---

## 1. Arquitectura de Ejecución Paralela

El análisis completo sobre grafos masivos (como Wikipedia, con más de 7.2M de nodos y 235M de aristas) puede ejecutarse de dos formas:
1. **Local secuencial (predeterminado):** `python3 continue_run.py config.json` ejecuta las etapas de manera secuencial y resistente con checkpoints intermedios.
2. **Paralelo desacoplado (en clúster):** Las etapas independientes de cálculo pueden distribuirse en nodos de cómputo separados mediante el flag `--target`:
   - `--target hits`: Calcula Authority y Hub scores (HITS).
   - `--target topic_ciencia`: Calcula Topic-Specific PageRank para tópicos de ciencia.
   - `--target topic_musica`: Calcula Topic-Specific PageRank para tópicos de música.
   - `--target topic_deportes`: Calcula Topic-Specific PageRank para tópicos de deportes.
   - `--target finalize`: Consolida métricas cruzadas (correlación de Pearson, solapamiento top-20) y evalúa el notebook final.

---

## 2. Variables de Configuración (`cluster_env.sh`)

Antes de lanzar trabajos, puedes ajustar las siguientes variables en `scripts/cluster/cluster_env.sh` o definirlas en tus variables de entorno:

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `WORK_DIR` | Directorio raíz del proyecto (compartido entre nodos) | Ruta absoluta del proyecto |
| `SCRATCH_DIR` | Disco local rápido del nodo (scratch/tmp) para shuffle | `/var/tmp` |
| `SPARK_DRIVER_MEMORY` | Memoria máxima de heap asignada a la JVM de Spark | `48g` |
| `CONTAINER_IMAGE` | Nombre o tag de la imagen de contenedor | `spark-analisis:latest` |
| `CONTAINER_TAR` | Ruta al archivo `.tar` si la imagen se carga localmente | `${WORK_DIR}/spark-analisis.tar` |
| `NSLOTS` | Cores asignados por el gestor de colas al job | `4` |

---

## 3. Consideraciones Técnicas Clave para Spark en HPC

1. **Asignación de Heap en tiempo de inicio (Py4J):**
   PySpark arranca la JVM antes de que `SparkSession.builder` procese configuraciones. Por ello, `run_task.sh` exporta explícitamente:
   ```bash
   export PYSPARK_SUBMIT_ARGS="--driver-memory ${SPARK_DRIVER_MEMORY} pyspark-shell"
   export _JAVA_OPTIONS="-Xmx${SPARK_DRIVER_MEMORY}"
   ```
2. **Sistemas de archivos compartidos (NFS/Lustre) y bloqueos:**
   Para evitar colisiones de SQLite y bloqueos de capas de contenedores sobre sistemas de archivos de red compartidos, el almacenamiento de Podman se redirige al almacenamiento local del nodo (`/var/tmp`) y se limpia automáticamente al finalizar el trabajo mediante trampas `trap EXIT`.
3. **Bibliotecas nativas (`libsnappyjava.so`):**
   Si el scratch compartido está montado con la opción `noexec`, se deben montar `/tmp` y `/var/tmp` virtuales en RAM con permisos de ejecución (`--tmpfs /tmp:rw,exec`), evitando errores de carga nativa.

---

## 4. Lanzamiento

### Vía Slurm
```bash
cd scripts/cluster/slurm
bash launch_all_slurm.sh
```
Los 4 cálculos correrán en paralelo en nodos distintos y el job de consolidación (`job_report.slurm`) se iniciará automáticamente tan pronto concluyan los 4 anteriores mediante `--dependency=afterok`.

### Vía SGE / Sun Grid Engine / UGE
```bash
cd scripts/cluster/sge
bash launch_all_sge.sh
```
El reporte se encadena automáticamente usando `-hold_jid wiki_hits,wiki_ciencia,wiki_musica,wiki_deportes`.

### Ejecución Directa en un Nodo Específico
Para ejecutar una subtarea manualmente:
```bash
bash scripts/cluster/run_task.sh hits podman
# O directamente sobre el entorno virtual local:
bash scripts/cluster/run_task.sh hits direct
```
