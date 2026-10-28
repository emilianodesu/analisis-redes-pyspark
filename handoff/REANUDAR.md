# Traspaso al siguiente agente — 28 de octubre de 2026

## Objetivo y estado
Entregar `analisis_redes.ipynb` en español, ejecutado, con HITS, PageRank y Topic-Sensitive PageRank sobre los tres grafos completos. No sustituir Wikipedia por una muestra. PySpark local, Python 3.11, Java 17. El equipo original tiene 24 GiB RAM.

- HepPh: 34,546 nodos; 421,534 aristas limpias; todos los rankings terminados y convergentes.
- Spotify: 156,326 nodos; 600,772 aristas recíprocas; todos los rankings terminados y convergentes. El espejo público y sus limitaciones están documentados en README.
- Wikipedia: extracción completa; 7,249,099 artículos canónicos, 235,645,261 aristas. PageRank terminado (54 iteraciones). HITS incompleto; último avance observado en `estado.json`. Faltan HITS, tres temas, mezcla, comparaciones y reporte final.
- El pipeline local se detuvo expresamente para el traspaso. Los PID y estados de ejecución antiguos no son válidos en el equipo destino.
- `handoff/` contiene copias informativas de auditorías y resúmenes. **No son archivos de reanudación.** El notebook conserva resultados parciales reales y no es una entrega completa.

## Importar el estado computacional
GitHub contiene código y resultados resumidos. Copiar por disco externo/red privada, por separado, `transferencia_redes.tar` y `transferencia_redes.tar.sha256` de la carpeta original T3. No subir el TAR a GitHub.

El TAR contiene los grafos procesados de los tres datasets y las carpetas de resultados de esos datasets, incluyendo PageRank de Wikipedia. No contiene el XML comprimido, los lotes XML, caché de arXiv, temporales Spark ni entornos. Es suficiente para continuar los algoritmos sin repetir extracción ni adquisición. Para conservar también las fuentes originales, transferir `data/raw/` por separado.

En un clon nuevo, instalar Python 3.11 y Java 17 y definir JAVA_HOME:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
.venv/bin/python scripts/transfer.py import /ruta/transferencia_redes.tar
.venv/bin/python -m pytest -q tests
```

Importar antes de iniciar el pipeline. El importador verifica SHA-256 y preserva mtimes nanosegundo: `config.json` compara tamaño y mtime de cada Parquet. No copiar Parquet con herramientas que cambien timestamps sin adaptar y verificar su identidad. La importación rechaza sobrescribir archivos existentes; ante fallo usar un clon limpio.

## Decisión pendiente sobre HITS
La implementación ACTUAL sigue usando residual global L1 < 1e-8, máximo 200 iteraciones; HITS normaliza vectores en L2. Se discutió usar cambio L2 < 1e-6 y comprobar estabilidad del top 20; **NO se implementó ese cambio**. El usuario solicitó migrar antes de resolverlo.

Los checkpoints internos de Spark no permiten reanudar este HITS entre procesos; se reiniciará desde cero. PageRank y ambos datasets terminados se reutilizan. Antes de volver a lanzar HITS conviene implementar y probar el nuevo criterio, separado del de PageRank, documentarlo en notebook y resultados, y añadir checkpoints persistentes con manifiesto/identidad del grafo para futuras reanudaciones. No afirmar que L1 y L2 o sus tolerancias son equivalentes, ni prometer un tiempo de convergencia.

Para continuar con el criterio ACTUAL:

```sh
.venv/bin/python run.py wikipedia
.venv/bin/python run.py report
```

Si se modifica el criterio de HITS, manejar explícitamente la configuración/reutilización de resultados: no borrar PageRank ni relabelar resultados anteriores como si se hubieran calculado con el criterio nuevo. Validar HITS frente a NetworkX en grafos pequeños y documentar norma, tolerancia, estabilidad y estado convergente/no convergente.

## Recursos y ejecución
Spark usa local[4], heap JVM 10 GiB, persistencia en disco. Los temporales pueden ocupar más de 150 GiB: dimensionar el destino y mantener margen; el programa falla con menos de 15 GiB libres. No ejecutar dos pipelines simultáneos. Mantener la máquina despierta y conectada. En Linux puede usarse tmux; caffeinate es sólo macOS. `continue_run.py` no es un servicio instalado ni se inicia automáticamente al clonar.

El historial de error por disco puede ser antiguo. Verificar proceso real, timestamps y avance, no sólo `running.json`. `run.py report` ejecuta el notebook sin calcular redes por defecto. No regenerar con `build_notebook.py` sin volver a ejecutar después, porque se pierden las salidas existentes.

## Límites de este repositorio
No contiene credenciales, limpieza de la Mac, archivos de H&M, datasets completos ni entornos de ejecución. No ejecutar limpieza del sistema como parte de esta tarea. No hay resultados finales de Wikipedia HITS o temas todavía.
