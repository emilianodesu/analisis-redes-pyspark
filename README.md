# T3 — HITS, PageRank y PageRank sensible a tópico

El entregable es **`analisis_redes.ipynb`**, en español. Conserva `src/` junto al notebook: allí están las implementaciones usadas por las celdas. Las salidas guardadas muestran resultados reales y marcan las etapas pendientes; no sustituyen Wikipedia completa por el piloto.

## Entorno

Se preparó `.venv` con Python 3.11 y `.runtime` con Java 17. Spark usa cuatro trabajadores locales y 10 GB de heap; el equipo tiene 24 GB de RAM. No ejecutar dos pipelines Spark simultáneamente.

Para reinstalar en otra máquina, instalar Python 3.11 y Java 17, crear un entorno virtual e instalar `requirements.lock.txt`. Definir `JAVA_HOME` si el JDK no está en `.runtime/jdk-17*/Contents/Home`. Seleccionar `.venv/bin/python` como kernel en el editor de notebooks.

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
```

## Ejecución

Desde esta carpeta:

```sh
.venv/bin/python -m pytest -q tests
.venv/bin/python run.py pilot
.venv/bin/python run.py hepph
.venv/bin/python run.py spotify
.venv/bin/python run.py wikipedia
.venv/bin/python run.py report
```

`run.py all` procesa los tres datasets por orden y registra errores por dataset para permitir el progreso de los demás. `report` ejecuta todas las celdas del notebook para incorporar las salidas disponibles sin recalcular redes. En el notebook, `EJECUTAR_PIPELINE = True` permite iniciar los cálculos completos desde una celda; no activarlo mientras exista otra ejecución activa.

Para el grafo completo de Wikipedia, también es posible ejecutar subtareas específicas de forma desacoplada mediante el parámetro `--target`:
```sh
.venv/bin/python run.py wikipedia --target hits
.venv/bin/python run.py wikipedia --target topic_ciencia
.venv/bin/python run.py wikipedia --target topic_musica
.venv/bin/python run.py wikipedia --target topic_deportes
.venv/bin/python run.py wikipedia --target finalize
```

Consultar progreso sin iniciar cálculos: `.venv/bin/python status.py`. El coordinador `continue_run.py` puede mantener las etapas largas en ejecución; su estado se guarda en `results/background_status.json` y sus logs en `results/background_*.log`. Cada dataset terminado actualiza el notebook. El equipo debe permanecer encendido y con red; si se interrumpe, `run.py all` reutiliza etapas terminadas y descargas parciales compatibles.

### Despliegue opcional en Clúster / HPC

Para entornos de alto rendimiento o servidores de cómputo en clúster (**Slurm**, **SGE / Grid Engine** o contenedores **Podman / Docker**), el directorio [`scripts/cluster/`](scripts/cluster/) incluye plantillas automatizadas para lanzar las subtareas de Wikipedia en paralelo en nodos independientes y encadenar la consolidación final del reporte. Consulta [`scripts/cluster/README.md`](scripts/cluster/README.md) para detalles de configuración y uso.

## Archivos y reanudación

- `data/raw/`: descargas, manifiestos SHA-256 y caché de arXiv.
- `data/processed/<dataset>/`: nodos, aristas y auditoría de limpieza.
- `data/pilot/`: piloto de Wikipedia, separado de los resultados completos.
- `results/<dataset>/`: rankings Parquet, top 20 CSV, convergencia, comparación y estado.
- `results/wiki_pilot.json`: estimación de recursos y su incertidumbre.
- `results/*_error.json`: errores de adquisición/procesamiento, cuando ocurren.

La descarga de Wikipedia se reanuda desde `.part` solo si la identidad remota coincide. Si cambia `latest`, se detiene para evitar mezclar versiones. La extracción confirma cada lote con un renombrado atómico; tras una interrupción vuelve a recorrer el XML y omite los lotes ya escritos. Esto evita repetir escrituras, aunque sí repite descompresión y lectura.

Los algoritmos terminados se reutilizan. Para procesos de larga duración (como HITS sobre Wikipedia), el pipeline guarda checkpoints intermedios periódicos en disco (cada 5 iteraciones en `results/<dataset>/_checkpoint_hits/`), lo que permite que una ejecución interrumpida se reanude automáticamente desde el último punto de control sin recalcular desde la iteración 1. No modificar datos ni reglas dentro de una carpeta de resultados existente; conservarla aparte para una nueva ejecución.

## Definiciones y límites

- Wikipedia: artículos canónicos de namespace 0, enlaces explícitos del wikitext, redirecciones resueltas. No se expanden plantillas. Las categorías se utilizan para semillas, no como nodos.
- HepPh: dirección del artículo que cita al citado. Metadatos de arXiv mediante identificadores antiguos, completando ceros iniciales. Los artículos sin metadatos permanecen en el grafo.
- Spotify: colaboraciones no dirigidas como dos aristas recíprocas; no se inventa orientación. Géneros determinan semillas cuando están disponibles.
- En esta ejecución, Spotify se obtuvo de la copia pública `raquelanamb/spotify-feature-network` porque Google Storage falló por validación TLS. Los tamaños coinciden con la API de Kaggle y los SHA-256 locales están en `data/raw/spotify`; no se verificó un checksum oficial. Se conservan seis artistas presentes solo en las aristas.
- PageRank: `alpha=0.85`, masa colgante distribuida con el vector de teleportación. HITS usa norma L2. Tolerancia global L1 `1e-8` y máximo 200 iteraciones.
- Conjuntos temáticos solapados; proporción de semillas en el top 20 es una medida descriptiva, no una evaluación independiente de precisión.
- El piloto de las primeras 1,000 páginas sirve para dimensionar. Su extrapolación no garantiza duración ni espacio máximos. Se detiene la extracción con menos de 15 GiB libres.

## Fuentes

- https://dumps.wikimedia.org/enwiki/latest/
- https://snap.stanford.edu/data/cit-HepPh.html
- https://www.kaggle.com/datasets/jfreyberg/spotify-artist-feature-collaboration-network/
- https://info.arxiv.org/help/api/user-manual.html
- https://spark.apache.org/docs/3.5.7/rdd-programming-guide.html

El notebook incluye las referencias y explica la interpretación de cada algoritmo.

## Continuar en otro equipo

Leer [el traspaso al siguiente agente](handoff/REANUDAR.md) y [el último estado observado](handoff/estado.json). El repositorio incluye resultados resumidos; para reutilizar grafos y rankings completos descarga el paquete con `python3 scripts/download_transfer.py` desde la [Release migration-v1](https://github.com/emilianodesu/analisis-redes-pyspark/releases/tag/migration-v1). El notebook todavía está incompleto para HITS y temas de Wikipedia. El cambio de tolerancia discutido no está implementado.
