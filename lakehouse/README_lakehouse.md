## 1) Diagram Arsitektur — Sebelum vs Sesudah

Sebelum (ETS lama):

```text
Producer/API   -> Kafka -> Consumer -> HDFS (raw JSON)
											\-> Spark analysis.py (langsung baca JSON)
```

Sesudah (dengan Lakehouse):

```text
Producer/API  -> Kafka -> Consumer -> HDFS (raw JSON)
																				 |
																				 v
																Bronze (Delta, raw + metadata)
																				 |
																				 v
																Silver (Delta, cleaned & typed)
																				 |
																				 v
																Gold (Delta, aggregated / joined)
																				 |
																				 v
																Dashboard / Reporting
```

## 2) Penjelasan Setiap Transformasi di Silver — Mengapa dilakukan

Transformasi utama (API):

- Parsing timestamp (`timestamp` -> `timestamp_ts`):
	- Mengapa: memungkinkan operasi waktu (window, groupBy hour, ordering) dan menghindari kesalahan saat mengurutkan/agg berdasarkan string.

- Casting numerik (`price_current`, `volume`, dsb. → `double`):
	- Mengapa: memastikan operasi aritmetika (avg, stddev, pct change) akurat dan tidak terjadi casting implicit yang salah.

- Normalisasi teks (`symbol`, `ticker` → uppercase/trim):
	- Mengapa: menghindari duplikat semantik (`bbca` vs `BBCA`) ketika melakukan groupBy/join.

- Filter nilai invalid (mis. `price_current` <= 0, `volume` < 0):
	- Mengapa: data sensor/feeds kadang berisi placeholder/errored values; mengeluarkan nilai tidak masuk akal mencegah outlier palsu mengacaukan statistik.

- Drop duplicates (mis. `symbol`+`timestamp_ts`):
	- Mengapa: mencegah double-counting ketika producer/consumer mengirim ulang pesan atau ketika file HDFS berisi duplikat.

Transformasi utama (RSS):

- Trim & normalisasi teks (`title`, `link`, `item_id`): meningkatkan konsistensi dan memudahkan deduplikasi.
- Parsing `published` & `timestamp` ke `published_ts`/`timestamp_ts`: memungkinkan analisis temporal pada berita.
- Drop duplicates berdasarkan `item_id`/`link`/`timestamp_ts`: menghindari hitungan berulang pada berita yang sama.

Catatan: semua transformasi di atas diterapkan di `lakehouse/02_silver.py` (fungsi `clean_api` dan `clean_rss`).

## 3) Berapa baris data yang hilang setelah cleaning? — Cara menghitung & Interpretasi

Bagian `02_silver.py` sudah menghitung dan mencetak jumlah baris sebelum dan sesudah cleaning (variabel `api_before`, `api_after`, `rss_before`, `rss_after`). Untuk analisis lebih lengkap, jalankan perintah ini setelah Bronze tersedia:

```bash
# jalankan silver (akan menulis hasil ke HDFS atau fallback lokal)
python lakehouse/02_silver.py

# contoh: lihat history / statistik Delta (local fallback) menggunakan pyspark
python - <<'PY'
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

builder = SparkSession.builder.appName('check_counts').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze_api='hdfs://localhost:8020/data/saham/lakehouse/bronze/api'
silver_api='hdfs://localhost:8020/data/saham/lakehouse/silver/api'

try:
		b = spark.read.format('delta').load(bronze_api)
		s = spark.read.format('delta').load(silver_api)
		print('API rows: bronze=', b.count(), 'silver=', s.count())
except Exception as e:
		print('Could not read HDFS Delta tables:', e)
finally:
		spark.stop()
PY
```

Interpretasi hasil:

- Jika banyak baris hilang karena `timestamp` null → data tidak punya informasi waktu; berdampak pada analisis temporal (hilang atau tidak dapat diurutkan).
- Jika banyak baris hilang karena `price_current` null atau <=0 → sumber data gagal mengambil harga; analisis return/volatility menjadi tidak akurat tanpa ini.
- Jika hilang karena deduplikasi → berarti duplikat nyata ada; ini berguna karena mencegah double-counting.

Contoh cara melaporkan persentase kehilangan (manual):

```text
lost_count = bronze_count - silver_count
lost_pct = lost_count / bronze_count * 100
```

Saran: tambahkan penjelasan ringkas di README setelah menjalankan `02_silver.py` dengan angka aktual (mis. "API: 10.000 -> 9.200 rows (8% hilang): 60% karena duplikat, 30% karena timestamp null, 10% karena harga invalid").

## 4) Perbandingan: Analisis Gold (yang diharapkan) vs Analisis Spark ETS lama

Perbedaan utama dan manfaat:

- Data kualitas & validitas:
	- ETS lama: membaca JSON mentah tanpa jaminan tipe → risk of silent errors.
	- Dengan Gold: data berasal dari Silver yang sudah bertipe dan bersih → agregasi lebih akurat.

- Reproducibility & time travel:
	- ETS lama: tidak mudah merekonstruksi dataset pada titik waktu tertentu.
	- Delta Lake: versi tabel tersimpan (time travel) → bisa membandingkan hasil analisis antar versi.

- Cross-source joins & enhanced metrics:
	- ETS lama: sulit menggabungkan API + RSS secara konsisten karena schema/timestamp mismatch.
	- Gold: join Silver(API) + Silver(RSS) memungkinkan analisis lanjutan (mis. korelasi berita vs price spike).

- Performance & maintenance:
	- Delta dapat melakukan compaction, predicate pushdown, dan metadata-driven listing → query lebih cepat dan biaya I/O lebih rendah dibanding scanning banyak file JSON.

Contoh tabel Gold yang memberi keuntungan nyata: `gold/saham_return`, `gold/saham_volatility`, `gold/saham_news_mention` (gabungan API & RSS untuk korelasi).

## 5) Screenshot: output tabel Delta & hasil Time Travel (petunjuk)

Silakan ambil screenshot pada langkah-langkah berikut dan simpan di `lakehouse/screenshots/`:

- `bronze_list.png`: output `docker exec hadoop-namenode hdfs dfs -ls -R /data/saham/lakehouse/bronze`
- `silver_list.png`: output `docker exec hadoop-namenode hdfs dfs -ls -R /data/saham/lakehouse/silver`
- `silver_history.png`: output dari PySpark:

```python
from delta.tables import DeltaTable
delta = DeltaTable.forPath(spark, 'hdfs://localhost:8020/data/saham/lakehouse/silver/api')
delta.history().show(truncate=False)
```

- `time_travel_compare.png`: hasil query versi berbeda:

```python
# contoh: versi saat ini vs versionAsOf=0
s_now = spark.read.format('delta').load(silver_path)
s_v0 = spark.read.format('delta').option('versionAsOf', 0).load(silver_path)
s_now.count(), s_v0.count()
```

Catatan: README ini tidak menyertakan file gambar — ambil screenshot di lingkungan kalian dan simpan di `lakehouse/screenshots/`.

## 6) Refleksi: Keuntungan nyata Delta Lake vs menyimpan langsung di HDFS/CSV

- ACID transactions: penulisan data bersifat atomik dan konsisten, menghindari partial-write atau corrupt states.
- Time Travel / Versioning: mudah melihat history dan mengembalikan data ke versi sebelumnya untuk audit atau reproduksi analisis.
- Schema enforcement & evolution: tipe kolom terjaga; dapat menangani evolusi schema dengan opsi `mergeSchema`.
- Performance: metadata-driven file listing dan optimisasi (compaction, Z-ordering) mengurangi I/O saat query.
- Concurrent writers/readers: Delta mendukung concurrency yang lebih baik daripada file-level JSON/CSV.
- Easier merges & upserts: `MERGE` membuat sinkronisasi incremental (CDC-like) mudah dibanding rewrite seluruh dataset.

---

Jika mau, saya bisa:

- Tambahkan ringkasan angka aktual (baris hilang dan penyebab) ke bagian 3 setelah kamu menjalankan `python lakehouse/02_silver.py` dan meng-upload hasilnya, atau saya bisa tambahkan kode kecil ke `02_silver.py` untuk menyimpan breakdown penyebab kehilangan ke sebuah JSON/CSV.
- Lanjutkan membuat `03_gold.py` untuk topik SahamMeter dan contoh tabel Gold.

Pilih langkah berikutnya.
