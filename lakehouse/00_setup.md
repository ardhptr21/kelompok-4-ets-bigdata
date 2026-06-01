# Panduan Instalasi, Setup, dan Cara Menjalankan Data Lakehouse

## 1.1 Prasyarat Sistem

- **Docker** dan **Docker Compose** telah terinstal.
- **Python >= 3.13** telah terinstal.
- Disarankan menggunakan tool **uv** (pengelola paket Python cepat) atau `pip` standar.

## 1.2 Langkah 1: Kloning & Pengaturan Lingkungan Python

Buka terminal di direktori proyek dan buat virtual environment:

```bash
# Membuat virtual environment dan memasang dependensi menggunakan 'uv'
uv venv
source .venv/bin/activate
uv pip install -e .
```

> [!NOTE]
> Jika tidak menggunakan `uv`, gunakan perintah bawaan Python:
> ```bash
> python3 -m venv .venv
> source .venv/bin/activate
> pip install -r pyproject.toml
> ```

## 1.3 Langkah 2: Menjalankan Infrastruktur Docker

Jalankan skrip pembantu `up.sh` untuk menyalakan kluster Hadoop dan Kafka secara otomatis, sekaligus menginisialisasi topik Kafka dan folder HDFS:

```bash
chmod +x up.sh down.sh
./up.sh
```

## 1.4 Langkah 3: Menjalankan Data Ingestion (Kafka Producers & Consumer)

Buka terminal baru (pastikan virtual environment aktif) dan jalankan konsumen data sebagai jembatan penyimpanan ke HDFS:

```bash
python kafka/consumer_to_hdfs.py
```

Buka terminal baru untuk menjalankan produsen data saham (Yahoo Finance / Simulasi):

```bash
python kafka/producer_api.py
```

Buka terminal baru untuk menjalankan produsen data berita (RSS Reader / Simulasi):

```bash
python kafka/producer_rss.py
```

## 1.5 Langkah 4: Menjalankan Bronze Layer

> [!NOTE]
> Fitur Security Manager yang digunakan Hadoop sudah tidak didukung lagi di Java versi terbaru, gunakan versi Java 21 di dalam virtual environment sebelum menjalankan kode Python:
> ```bash
> export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
> export PATH="$JAVA_HOME/bin:$PATH"
> ```

Buka terminal baru (pastikan virtual environment aktif) dan jalankan kode bronze layer setelah ada data masuk ke dalam HDFS:

```bash
python lakehouse/01_bronze.py
```

Verifikasi data berhasil diproses dengan:

```bash
docker exec hadoop-namenode hdfs dfs -ls -R /data/saham/lakehouse/bronze
```

## 1.6 Langkah 5: Menjalankan Silver Layer

Setelah bronze berhasil dibuat, jalankan transformasi silver untuk membersihkan data API dan RSS:

```bash
python lakehouse/02_silver.py
```

Verifikasi hasilnya di HDFS dengan:

```bash
docker exec hadoop-namenode hdfs dfs -ls -R /data/saham/lakehouse/silver
```

Jika HDFS belum siap, skrip silver akan mencoba menyimpan ke fallback lokal di `lakehouse/lakehouse_data/silver/`.



