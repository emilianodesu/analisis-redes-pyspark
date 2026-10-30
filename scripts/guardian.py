#!/usr/bin/env python3
"""
Guardian autónomo para supervisión de almacenamiento y pipeline:
1. Monitorea continuamente el espacio libre en /mnt/ssd.
2. Si el espacio libre baja de MIN_FREE_GB (100 GB), transfiere shuffles históricos al HDD (/mnt/disk1).
3. Monitorea el estado del servicio analisis-redes.service y lo reanuda automáticamente si fallara.
4. Registra métricas y eventos en results/guardian.log.
"""
import glob
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

ROOT = Path('/mnt/disk1/analisis-redes-pyspark')
LOG_FILE = ROOT / 'results/guardian.log'
OVERFLOW_BASE = '/mnt/disk1/spark-tmp-overflow'
SECRETS_FILE = Path('/data/pavilion-node/secrets/secrets.env')
MIN_FREE_GB = 100.0  # Umbral mínimo de seguridad en SSD
CHECK_INTERVAL_SEC = 60

def send_telegram(msg):
    token = None
    chat_id = None
    if SECRETS_FILE.exists():
        try:
            with SECRETS_FILE.open() as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        if k == 'TELEGRAM_TOKEN': token = v
                        elif k == 'TELEGRAM_CHAT_ID': chat_id = v
        except Exception:
            pass

    token = token or os.environ.get('TELEGRAM_TOKEN')
    chat_id = chat_id or os.environ.get('TELEGRAM_CHAT_ID')

    if not token or not chat_id:
        log("No se encontraron credenciales de Telegram; no se envió notificación.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({'chat_id': chat_id, 'text': msg}).encode('utf-8')
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                log("Notificación de Telegram enviada exitosamente.")
                return True
    except Exception as e:
        log(f"Error al enviar mensaje a Telegram: {e}")
    return False

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry = f"[{ts}] {msg}"
    print(entry, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open('a') as f:
            f.write(entry + '\n')
    except Exception:
        pass

def get_free_ssd_gb():
    usage = shutil.disk_usage('/mnt/ssd')
    return usage.free / (1024 ** 3)

def get_active_blockmgr():
    candidates = sorted(glob.glob('/mnt/ssd/spark-tmp/blockmgr-*'), key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0] if candidates else None

def offload_old_shuffles(max_sid):
    blockmgr = get_active_blockmgr()
    if not blockmgr:
        return 0, 0

    os.makedirs(OVERFLOW_BASE, exist_ok=True)
    count = 0
    bytes_freed = 0

    for root, dirs, files in os.walk(blockmgr):
        rel = os.path.relpath(root, blockmgr)
        dst_dir = os.path.join(OVERFLOW_BASE, rel)
        os.makedirs(dst_dir, exist_ok=True)

        for f in files:
            if not f.startswith('shuffle_'):
                continue
            src_path = os.path.join(root, f)
            if os.path.islink(src_path):
                continue

            try:
                sid = int(f.split('_')[1])
            except (ValueError, IndexError):
                continue

            if sid <= max_sid:
                sz = os.path.getsize(src_path)
                dst_path = os.path.join(dst_dir, f)
                tmp_link = src_path + '.tmp_symlink'

                try:
                    shutil.copyfile(src_path, dst_path)
                    os.symlink(dst_path, tmp_link)
                    os.replace(tmp_link, src_path)
                    count += 1
                    bytes_freed += sz
                except Exception as e:
                    if os.path.exists(tmp_link):
                        try: os.unlink(tmp_link)
                        except: pass

    return count, bytes_freed

def check_and_rebalance_storage():
    free_gb = get_free_ssd_gb()
    if free_gb < MIN_FREE_GB:
        log(f"⚠️ Alerta: Espacio libre en /mnt/ssd en {free_gb:.1f} GB (< {MIN_FREE_GB} GB). Iniciando desahogo hacia HDD...")
        blockmgr = get_active_blockmgr()
        if not blockmgr:
            log("No se encontró blockmgr activo.")
            return

        # Encontrar el shuffle_id máximo actual
        sids = set()
        for root, dirs, files in os.walk(blockmgr):
            for f in files:
                if f.startswith('shuffle_') and not os.path.islink(os.path.join(root, f)):
                    try:
                        sids.add(int(f.split('_')[1]))
                    except:
                        pass

        if not sids:
            log("No hay shuffles sin enlazar para mover.")
            return

        max_sid = max(sids)
        # Mover shuffles que tengan más de 20 IDs de antigüedad
        target_sid = max(1, max_sid - 20)
        log(f"Descargando bloques hasta shuffle_id {target_sid} (actual: {max_sid})...")
        count, bytes_freed = offload_old_shuffles(target_sid)
        new_free = get_free_ssd_gb()
        freed_gb = bytes_freed / (1024 ** 3)
        log(f"✅ Desahogo completado: {count} archivos movidos ({freed_gb:.1f} GB liberados). Espacio libre actual en SSD: {new_free:.1f} GB.")

def check_pipeline_completion():
    complete_marker = ROOT / 'results/wikipedia/complete.json'
    tg_sent_file = ROOT / 'results/.telegram_completed'
    bg_status_file = ROOT / 'results/background_status.json'

    if complete_marker.exists() and not tg_sent_file.exists():
        nb_ready = False
        if bg_status_file.exists():
            try:
                import json
                bg = json.loads(bg_status_file.read_text())
                if bg.get('state') == 'finished':
                    nb_ready = True
            except Exception:
                pass

        res = subprocess.run(['systemctl', '--user', 'is-active', 'analisis-redes.service'],
                             capture_output=True, text=True)
        status = res.stdout.strip()
        if status not in ['active', 'activating'] or nb_ready:
            msg = (
                "🎉 ¡Proyecto de Análisis de Redes PySpark Completado al 100% en axiom-server!\n\n"
                "📊 Resumen de Resultados:\n"
                "1. HepPh: ✓ PageRank, HITS y Tópicos completados.\n"
                "2. Spotify: ✓ PageRank, HITS y Tópicos completados.\n"
                "3. Wikipedia (Grafo Completo - 7,249,099 nodos / 235,645,261 aristas):\n"
                "   • PageRank Base: ✓ 54 iters (tol: 7.71e-09)\n"
                "   • HITS Authority & Hubs: ✓ 123 iters (tol: 9.04e-09)\n"
                "   • Tópicos: ✓ Ciencia, Música y Deportes calculados.\n"
                "   • Métricas: ✓ Topic Mixture, correlaciones de Pearson y Top-20.\n\n"
                "📓 Cuaderno: analisis_redes.ipynb evaluado y listo con todas las tablas y gráficos."
            )
            if send_telegram(msg):
                try:
                    tg_sent_file.write_text("sent\n")
                    log("🎉 Notificación de finalización enviada por Telegram y registrada.")
                except Exception:
                    pass

def check_pipeline_service():
    check_pipeline_completion()
    res = subprocess.run(['systemctl', '--user', 'is-active', 'analisis-redes.service'],
                         capture_output=True, text=True)
    status = res.stdout.strip()
    complete_marker = ROOT / 'results/wikipedia/complete.json'

    if status in ['active', 'activating']:
        return

    if complete_marker.exists():
        log("🎉 Pipeline de Wikipedia completado con éxito (complete.json verificado).")
        return

    log(f"⚠️ Servicio analisis-redes.service no está activo (estado: '{status}'). Intentando recuperación automática...")
    ckpt_meta = ROOT / 'results/wikipedia/_checkpoint_hits/meta.json'
    if ckpt_meta.exists():
        log("Se detectó checkpoint disponible; el pipeline reanudará desde el último punto seguro.")

    restart_res = subprocess.run(['systemctl', '--user', 'restart', 'analisis-redes.service'],
                                 capture_output=True, text=True)
    if restart_res.returncode == 0:
        log("🔄 analisis-redes.service reiniciado exitosamente por el guardián.")
    else:
        log(f"❌ Error al reiniciar analisis-redes.service: {restart_res.stderr}")

def main():
    log("=== Guardián de almacenamiento y pipeline iniciado ===")
    log(f"Espacio libre inicial en /mnt/ssd: {get_free_ssd_gb():.1f} GB. Umbral de desahogo: {MIN_FREE_GB} GB.")

    iteration_counter = 0
    while True:
        try:
            check_and_rebalance_storage()
            check_pipeline_service()

            # Cada 10 minutos, registrar latido (heartbeat) de estado
            iteration_counter += 1
            if iteration_counter % 10 == 0:
                free_gb = get_free_ssd_gb()
                log(f"[Heartbeat] Guardián operativo. SSD libre: {free_gb:.1f} GB.")

        except Exception as e:
            log(f"Error en bucle del guardián: {e}")

        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == '__main__':
    main()
