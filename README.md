# Twitter Collection & Preprocessing System

Sistem terintegrasi untuk mengumpulkan tweet setiap 2 jam, preprocess otomatis, dan simpan dengan backup.

## 🚀 Fitur Utama

### **📥 Collection**
- ✅ Mengumpulkan 10 tweet setiap 2 jam
- ✅ Hashtag: #indihome, #telkomIndonesia, #telkom, #gangguanTelkom
- ✅ Filter bahasa Indonesia
- ✅ Filter waktu 2 jam terakhir

### **🔧 Preprocessing**
- ✅ Lowercase conversion
- ✅ URL removal
- ✅ Mention removal (@username)
- ✅ Hashtag text extraction
- ✅ Number removal
- ✅ Punctuation removal
- ✅ Extra whitespace removal

### **💾 Storage**
- ✅ Timestamped backups
- ✅ Master file accumulation
- ✅ Duplicate removal

## 📁 File Structure

```
integrated_twitter_pipeline.py    # Main integrated pipeline
run_integrated_pipeline.bat      # Easy run script
create_integrated_task.bat       # Windows Task Scheduler setup
stop_twitter_collection.bat      # Stop all processes
backup/                          # Timestamped backups
├── tweets_raw_YYYYMMDD_HHMMSS.csv
└── tweets_processed_YYYYMMDD_HHMMSS.csv
tweets_master_raw.csv            # Accumulated raw data
tweets_master_processed.csv      # Accumulated processed data
config.yaml                      # Configuration file
```

## 🛠️ Cara Menjalankan

### **1. Continuous Mode (Recommended)**
```bash
python integrated_twitter_pipeline.py
```
atau
```bash
run_integrated_pipeline.bat
```

### **2. Windows Task Scheduler**
```bash
create_integrated_task.bat
```
Jalankan sebagai Administrator untuk setup otomatis.

### **3. Manual Test**
```bash
python daily_twitter_collection.py
```

## 🛑 Cara Stop

### **1. Continuous Mode**
- Tekan **`Ctrl + C`** di terminal

### **2. Task Scheduler**
```bash
stop_twitter_collection.bat
```

### **3. Manual Stop**
```cmd
taskkill /f /im python.exe
```

## ⏰ Jadwal Koleksi

Setiap 2 jam:
- 00:00, 02:00, 04:00, 06:00, 08:00, 10:00
- 12:00, 14:00, 16:00, 18:00, 20:00, 22:00

## 📊 Output Files

### **Backup Files (Timestamped)**
- `backup/tweets_raw_20250728_143000.csv` - Raw data dengan timestamp
- `backup/tweets_processed_20250728_143000.csv` - Processed data dengan timestamp

### **Master Files (Accumulated)**
- `tweets_master_raw.csv` - Semua raw data yang terkumpul
- `tweets_master_processed.csv` - Semua processed data yang terkumpul

## 🔄 Pipeline Steps

### **Step 1: Collection**
```python
# Mengumpulkan 10 tweet dengan hashtag yang ditentukan
df_raw = fetch_with_harvest(config)
```

### **Step 2: Preprocessing**
```python
# Preprocess text: lowercase, URL removal, mention removal, etc.
df_raw['processed_text'] = df_raw[text_column].apply(simple_clean_text)
```

### **Step 3: Backup**
```python
# Simpan dengan timestamp
backup_raw = f"backup/tweets_raw_{timestamp}.csv"
backup_processed = f"backup/tweets_processed_{timestamp}.csv"
```

### **Step 4: Master Files**
```python
# Append ke master files dan remove duplicates
df_master_raw = pd.concat([df_master_raw, df_raw], ignore_index=True)
df_master_processed = pd.concat([df_master_processed, df_processed], ignore_index=True)
```

## 📈 Monitoring

Script akan menampilkan:
- Jumlah tweet yang dikumpulkan
- Status preprocessing
- Sample processed tweets
- File yang dibuat
- Waktu koleksi berikutnya

## 🔧 Configuration

### **config.yaml**
```yaml
twitter_query: "#indihome OR #telkomIndonesia OR #telkom OR #gangguanTelkom lang:id"
max_tweets: 10
twitter_bearer_token: "your_token_here"
```

## 📝 Example Output

### **Raw Tweet:**
```
"RT @user123: #indihome sangat bagus! 😊 https://t.co/abc123 @telkom"
```

### **Processed Tweet:**
```
"rt user123 indihome sangat bagus telkom"
```

### **Files Created:**
```
backup/tweets_raw_20250728_143000.csv
backup/tweets_processed_20250728_143000.csv
tweets_master_raw.csv (updated)
tweets_master_processed.csv (updated)
```

## 🚨 Troubleshooting

### **1. No tweets collected**
- Check Twitter API token di `config.yaml`
- Verify hashtag spelling
- Check internet connection

### **2. Preprocessing errors**
- Check file permissions
- Verify file paths
- Check CSV structure

### **3. Storage issues**
- Check disk space
- Verify write permissions
- Check file paths

## 📞 Support

Jika ada masalah:
1. Check error messages
2. Verify dependencies
3. Check file permissions
4. Restart pipeline jika perlu

## 🎯 Use Cases

### **1. Real-time Monitoring**
- Monitor sentiment terhadap Telkom/Indihome
- Track trending topics
- Analyze customer complaints

### **2. Data Analysis**
- Sentiment analysis
- Topic modeling
- Trend analysis
- Customer feedback analysis

### **3. Reporting**
- Daily/weekly reports
- Trend reports
- Sentiment reports 