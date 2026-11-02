import sys
import subprocess
import threading
import os
import signal
import time
import queue
import json
import shutil
import tempfile
from datetime import datetime
import streamlit.components.v1 as components

# Install streamlit jika belum ada
try:
    import streamlit as st
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit"])
    import streamlit as st


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JADWAL_FILE = os.path.join(BASE_DIR, "jadwal_streaming.json")
LOG_FILE = os.path.join(BASE_DIR, "log_streaming_terakhir.txt")
NAMA_HARI = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

# ============================================================
# GLOBAL STATE STREAMLIT
# ============================================================
_DEFAULTS = {
    "ffmpeg_process": None,
    "ffmpeg_thread": None,
    "streaming": False,
    "started_by_scheduler": False,
    "log_queue": None,
    "logs": [],
    "scheduler_started": False,
    "jadwal": None,
    "jumlah_putaran": 12,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v
if st.session_state["log_queue"] is None:
    st.session_state["log_queue"] = queue.Queue()


def add_log(msg):
    """Masukkan log ke queue agar aman dipakai dari thread."""
    try:
        st.session_state["log_queue"].put(str(msg))
    except Exception:
        print(msg)


def drain_logs():
    """Ambil log dari queue lalu tampilkan di Streamlit."""
    try:
        while not st.session_state["log_queue"].empty():
            st.session_state["logs"].append(st.session_state["log_queue"].get_nowait())
    except Exception:
        pass

    return "\n".join(st.session_state["logs"][-30:])


def _mask_key(text, key):
    """Samarkan stream key supaya tidak bocor di log layar."""
    if key and key in text:
        return text.replace(key, "***")
    return text


def stop_ffmpeg():
    """Hentikan hanya proses FFmpeg milik aplikasi ini, bukan semua ffmpeg di server."""
    process = st.session_state.get("ffmpeg_process")

    if process and process.poll() is None:
        try:
            if os.name == "nt":
                process.terminate()
            else:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)

            time.sleep(1)

            if process.poll() is None:
                if os.name == "nt":
                    process.kill()
                else:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)

        except Exception as e:
            add_log(f"Error saat menghentikan FFmpeg: {e}")

    st.session_state["ffmpeg_process"] = None
    st.session_state["streaming"] = False
    st.session_state["started_by_scheduler"] = False


# ============================================================
# KASET LOOP MULUS (anti-jeda)
# Videomu disambung N kali menjadi SATU file panjang dengan
# timeline yang kontinu -> tidak ada titik putus tiap putaran.
# -stream_loop hanya dipakai di ujung kaset, jadi jeda (kalau
# pun ada) hanya mungkin muncul sekali per kaset penuh,
# bukan tiap putaran.
# ============================================================
def nama_kaset(video_path, jumlah):
    base = os.path.splitext(os.path.basename(video_path))[0]
    aman = "".join(c if (c.isalnum() or c in "._-") else "_" for c in base)[:60]
    folder = os.path.dirname(os.path.abspath(video_path))
    return os.path.join(folder, f"KASET_x{jumlah}_{aman}.mp4")


def buat_kaset_loop(video_path, jumlah, log_callback):
    """Sambung video sebanyak `jumlah` kali jadi satu file mulus (tanpa re-encode)."""
    out = nama_kaset(video_path, jumlah)

    if os.path.exists(out) and os.path.getsize(out) > 0:
        log_callback(f"Kaset sudah ada, dipakai ulang: {os.path.basename(out)}")
        return out

    if not os.path.exists(video_path):
        log_callback("Video sumber tidak ditemukan.")
        return None

    ukuran = os.path.getsize(video_path)
    perkiraan = ukuran * jumlah
    folder = os.path.dirname(out) or "."
    bebas = shutil.disk_usage(folder).free
    if perkiraan > bebas * 0.9:
        log_callback(
            f"Gagal: kaset butuh ~{perkiraan / 1e9:.1f} GB, disk bebas {bebas / 1e9:.1f} GB. "
            "Kecilkan jumlah putaran."
        )
        return None

    daftar = None
    try:
        fd, daftar = tempfile.mkstemp(suffix=".txt", prefix="kaset_")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            ap = os.path.abspath(video_path).replace("'", "'\\''")
            for _ in range(jumlah):
                f.write(f"file '{ap}'\n")

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "concat", "-safe", "0", "-i", daftar,
            "-c", "copy", "-movflags", "+faststart", "-y", out,
        ]
        log_callback(f"Membuat kaset {jumlah}x ... (~{perkiraan / 1e6:.0f} MB, tanpa re-encode)")
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0 or not os.path.exists(out) or os.path.getsize(out) == 0:
            log_callback(f"Gagal membuat kaset: {(r.stderr or '')[-500:]}")
            return None
        log_callback(f"Kaset jadi: {os.path.basename(out)}")
        return out
    finally:
        if daftar and os.path.exists(daftar):
            try:
                os.remove(daftar)
            except Exception:
                pass


def run_ffmpeg(video_path, stream_key, is_vertical, log_callback):
    # ✅ Server YouTube Live (Secure RTMP)
    output_url = f"rtmps://a.rtmps.youtube.com/live2/{stream_key}"

    def _log(msg):
        msg = _mask_key(str(msg), stream_key)
        log_callback(msg)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    # ============================================================
    # FILTER VIDEO UNTUK LIVE LOOP SMOOTH
    # - fps stabil 30
    # - scale aman
    # - format yuv420p kompatibel YouTube
    # - setpts reset timestamp agar loop tidak patah
    # ============================================================
    if is_vertical:
        vf_filter = (
            "scale=720:1280:force_original_aspect_ratio=decrease,"
            "pad=720:1280:(ow-iw)/2:(oh-ih)/2,"
            "fps=30,format=yuv420p,setpts=N/(30*TB)"
        )
    else:
        vf_filter = (
            "scale=1280:720:force_original_aspect_ratio=decrease,"
            "pad=1280:720:(ow-iw)/2:(oh-ih)/2,"
            "fps=30,format=yuv420p,setpts=N/(30*TB)"
        )

    # ============================================================
    # COMMAND FFMPEG UNTUK LOOP LIVE LEBIH HALUS
    # Catatan:
    # - stream_loop -1 tetap dipakai agar fungsi loop tidak berubah,
    #   TAPI sumbernya kini kaset loop (satu timeline kontinu) sehingga
    #   titik putus hanya mungkin sekali per kaset penuh, bukan tiap putaran
    # - -fflags +genpts membuat timestamp baru
    # - aresample async membantu audio tidak putus saat loop
    # - keyframe/GOP 60 cocok untuk 30fps (rekomendasi YouTube 2 detik)
    # - zerolatency + CBR membuat live lebih stabil
    # ============================================================
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "info",

        "-re",
        "-stream_loop", "-1",

        "-fflags", "+genpts",
        "-avoid_negative_ts", "make_zero",

        "-i", video_path,

        "-vf", vf_filter,

        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-profile:v", "main",
        "-level", "4.1",
        "-pix_fmt", "yuv420p",

        "-r", "30",
        "-g", "60",
        "-keyint_min", "60",
        "-sc_threshold", "0",

        "-b:v", "2500k",
        "-minrate", "2500k",
        "-maxrate", "2500k",
        "-bufsize", "5000k",

        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-af", "aresample=async=1:first_pts=0",

        "-flvflags", "no_duration_filesize",
        "-f", "flv",
        output_url
    ]

    _log("Menjalankan FFmpeg:\n" + " ".join(_mask_key(a, stream_key) for a in cmd))

    try:
        popen_kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "bufsize": 1
        }

        if os.name != "nt":
            popen_kwargs["preexec_fn"] = os.setsid
        else:
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

        process = subprocess.Popen(cmd, **popen_kwargs)
        st.session_state["ffmpeg_process"] = process

        for line in process.stdout:
            if line:
                _log(line.strip())

        process.wait()

    except Exception as e:
        _log(f"Error: {e}")

    finally:
        st.session_state["streaming"] = False
        st.session_state["ffmpeg_process"] = None
        st.session_state["started_by_scheduler"] = False
        _log("Streaming selesai atau dihentikan.")


def mulai_stream(video_path, stream_key, is_vertical, jumlah_putaran, by_scheduler=False):
    """Mulai streaming; pakai kaset loop bila sudah ada, kalau belum pakai video asli."""
    if st.session_state.get("streaming"):
        return False

    kaset = nama_kaset(video_path, jumlah_putaran)
    sumber = kaset if os.path.exists(kaset) else video_path
    label = "⏰ Scheduler: " if by_scheduler else ""
    add_log(f"{label}Sumber streaming: {os.path.basename(sumber)}")

    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== {datetime.now():%Y-%m-%d %H:%M:%S} mulai: {os.path.basename(sumber)} ===\n")
    except Exception:
        pass

    st.session_state["streaming"] = True
    st.session_state["started_by_scheduler"] = by_scheduler
    st.session_state["logs"] = []

    st.session_state["ffmpeg_thread"] = threading.Thread(
        target=run_ffmpeg,
        args=(sumber, stream_key, is_vertical, add_log),
        daemon=True
    )
    st.session_state["ffmpeg_thread"].start()
    return True


# ============================================================
# JADWAL OTOMATIS
# ============================================================
def simpan_jadwal(jd):
    st.session_state["jadwal"] = jd
    try:
        with open(JADWAL_FILE, "w", encoding="utf-8") as f:
            json.dump(jd, f, ensure_ascii=False, indent=2)
    except Exception as e:
        add_log(f"Gagal menyimpan jadwal: {e}")


def muat_jadwal():
    if st.session_state.get("jadwal") is None and os.path.exists(JADWAL_FILE):
        try:
            with open(JADWAL_FILE, encoding="utf-8") as f:
                st.session_state["jadwal"] = json.load(f)
        except Exception:
            st.session_state["jadwal"] = None


def _dalam_jadwal(jd, now):
    mulai = datetime.strptime(jd["jam_mulai"], "%H:%M").time()
    sls = jd.get("jam_selesai") or ""
    selesai = datetime.strptime(sls, "%H:%M").time() if sls else None
    t = now.time()
    if selesai is None:
        return t >= mulai
    if mulai < selesai:
        return mulai <= t < selesai
    return t >= mulai or t < selesai  # jadwal lewat tengah malam


def scheduler_loop():
    """Thread latar: cek tiap 10 detik, jalankan/hentikan stream sesuai jadwal."""
    while True:
        time.sleep(10)
        try:
            jd = st.session_state.get("jadwal")
            if not jd or not jd.get("aktif"):
                continue
            now = datetime.now()
            streaming = st.session_state.get("streaming", False)
            by_sched = st.session_state.get("started_by_scheduler", False)
            hari_ok = now.weekday() in jd.get("hari", [0, 1, 2, 3, 4, 5, 6])
            in_win = hari_ok and _dalam_jadwal(jd, now)

            if in_win and not streaming:
                key = st.session_state.get("stream_key_input", "")
                if not key:
                    add_log("⏰ Jadwal tiba tapi stream key kosong — isi stream key dulu.")
                    continue
                video = jd.get("video")
                if not video or not os.path.exists(video):
                    add_log("⏰ Video jadwal tidak ditemukan.")
                    continue
                add_log("⏰ Jadwal mulai — streaming otomatis jalan.")
                mulai_stream(video, key, jd.get("is_vertical", False),
                             jd.get("putaran", 12), by_scheduler=True)
            elif not in_win and streaming and by_sched and jd.get("jam_selesai"):
                add_log("⏰ Jadwal selesai — streaming otomatis berhenti.")
                stop_ffmpeg()
        except Exception as e:
            try:
                add_log(f"Scheduler error: {e}")
            except Exception:
                pass


def main():
    st.set_page_config(
        page_title="Streaming YouTube Live",
        page_icon="🟥",
        layout="wide"
    )

    try:
        st.config.set_option("server.maxUploadSize", 1000)
    except Exception:
        pass

    # ✅ Perbaikan: cek ffmpeg di awal dengan pesan yang jelas
    if not shutil.which("ffmpeg"):
        st.error("ffmpeg tidak ditemukan di sistem ini. Install dulu: `sudo apt install ffmpeg`")
        st.stop()

    muat_jadwal()

    if not st.session_state.get("scheduler_started"):
        threading.Thread(target=scheduler_loop, daemon=True).start()
        st.session_state["scheduler_started"] = True

    st.title("Live Streaming ke YouTube")

    show_ads = st.checkbox("Tampilkan Iklan", value=False)
    if show_ads:
        st.subheader("Iklan Sponsor")
        components.html(
            """
            <div style="background:#f0f2f6;padding:20px;border-radius:10px;text-align:center">
                <p style="color:#888">Iklan akan muncul di sini</p>
            </div>
            """,
            height=200
        )

    video_files = [
        f for f in os.listdir(".")
        if f.lower().endswith((".mp4", ".flv"))
    ]

    st.write("Video yang tersedia:")
    selected_video = st.selectbox("Pilih video", video_files) if video_files else None

    uploaded_file = st.file_uploader(
        "Atau upload video baru (mp4/flv - codec H264/AAC)",
        type=["mp4", "flv"]
    )

    if uploaded_file:
        with open(uploaded_file.name, "wb") as f:
            f.write(uploaded_file.read())
        st.success("Video berhasil diupload!")
        video_path = uploaded_file.name
    elif selected_video:
        video_path = selected_video
    else:
        video_path = None

    # ✅ Stream Key YouTube Live
    st.text_input("YouTube Stream Key", type="password", key="stream_key_input",
                  help="Dapatkan dari YouTube Studio (Live Control Room)")
    is_vertical = st.checkbox("Mode Vertikal (YouTube Shorts Live - 720x1280)")

    # ============================================================
    # KASET LOOP MULUS
    # ============================================================
    st.subheader("🎞️ Kaset Loop Mulus (anti-jeda)")
    st.caption(
        "Kaset = videomu disambung N kali menjadi SATU file panjang dengan timeline "
        "yang kontinu, jadi tidak ada titik putus di tiap putaran. Dibuat sekali, "
        "tanpa re-encode (cepat, kualitas utuh)."
    )
    jumlah = st.slider("Jumlah putaran per kaset", 2, 100,
                       st.session_state.get("jumlah_putaran", 12),
                       key="jumlah_putaran")

    if video_path and os.path.exists(video_path):
        perkiraan_mb = os.path.getsize(video_path) * jumlah / 1e6
        st.write(f"Perkiraan ukuran kaset: ±{perkiraan_mb:.0f} MB")
        kaset = nama_kaset(video_path, jumlah)
        if os.path.exists(kaset):
            st.success(f"Kaset siap: {os.path.basename(kaset)} — streaming akan memakai kaset ini.")
            if st.button("🗑️ Hapus kaset ini"):
                try:
                    os.remove(kaset)
                    st.success("Kaset dihapus.")
                except Exception as e:
                    st.error(f"Gagal menghapus: {e}")
                st.rerun()
        if st.button("🎞️ Buat Kaset Loop"):
            with st.spinner("Menyambung video menjadi kaset..."):
                buat_kaset_loop(video_path, jumlah, add_log)
            st.rerun()
    else:
        st.info("Pilih atau upload video dulu untuk membuat kaset.")

    # ============================================================
    # JADWAL OTOMATIS
    # ============================================================
    with st.expander("⏰ Jadwal Otomatis"):
        jd = st.session_state.get("jadwal") or {}
        aktif = st.checkbox("Aktifkan jadwal otomatis", value=jd.get("aktif", False))
        jm_def = jd.get("jam_mulai", "19:00")
        js_def = jd.get("jam_selesai", "21:00")
        jm = st.time_input("Jam mulai", value=datetime.strptime(jm_def, "%H:%M").time())
        pakai_selesai = st.checkbox("Berhenti otomatis pada jam selesai",
                                    value=bool(jd.get("jam_selesai")))
        js = st.time_input("Jam selesai", value=datetime.strptime(js_def, "%H:%M").time())
        hari_def = [NAMA_HARI[i] for i in jd.get("hari", [0, 1, 2, 3, 4, 5, 6])]
        hari_nama = st.multiselect("Hari", NAMA_HARI, default=hari_def)

        if st.button("💾 Simpan Jadwal"):
            if not video_path:
                st.error("Pilih video dulu sebelum menyimpan jadwal.")
            else:
                simpan_jadwal({
                    "aktif": aktif,
                    "jam_mulai": jm.strftime("%H:%M"),
                    "jam_selesai": js.strftime("%H:%M") if pakai_selesai else "",
                    "hari": [NAMA_HARI.index(h) for h in hari_nama],
                    "video": os.path.abspath(video_path),
                    "is_vertical": is_vertical,
                    "putaran": jumlah,
                })
                st.success("Jadwal tersimpan.")

        jd_now = st.session_state.get("jadwal")
        if jd_now and jd_now.get("aktif"):
            rentang = f"{jd_now['jam_mulai']}–{jd_now['jam_selesai']}" if jd_now.get("jam_selesai") \
                else f"mulai {jd_now['jam_mulai']} (berhenti manual)"
            st.info(f"Jadwal aktif: {', '.join(NAMA_HARI[i] for i in jd_now['hari'])} — {rentang}")
        st.caption("Syarat: aplikasi ini tetap terbuka dan stream key terisi saat jadwal tiba.")

    col1, col2 = st.columns(2)

    with col1:
        start_clicked = st.button(
            "Mulai Streaming",
            disabled=st.session_state.get("streaming", False)
        )

    with col2:
        stop_clicked = st.button(
            "Hentikan Streaming",
            disabled=not st.session_state.get("streaming", False)
        )

    log_placeholder = st.empty()

    if start_clicked:
        stream_key = st.session_state.get("stream_key_input", "")
        if not video_path or not stream_key:
            st.error("Video dan stream key harus diisi!")
        else:
            mulai_stream(video_path, stream_key, is_vertical,
                         st.session_state.get("jumlah_putaran", 12))
            st.success("Streaming dimulai ke YouTube!")

    if stop_clicked:
        stop_ffmpeg()
        st.warning("Streaming dihentikan!")

    current_logs = drain_logs()
    log_placeholder.text(current_logs)

    if st.session_state.get("streaming", False):
        st.info("Status: Streaming aktif")
    else:
        st.info("Status: Tidak streaming")

    # ✅ Perbaikan: log tampil otomatis ter-update selama streaming
    if st.session_state.get("streaming", False):
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
