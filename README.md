# 📈 OpenAlgo → TimescaleDB Tick Recorder (हिंदी)

NSE के **50 शेयरों** का **1-सेकंड का live tick data** और **Level-2 market depth** अपने
खुद के सर्वर पर **TimescaleDB** में store करने की minimal Python script।

> Data source: **[OpenAlgo](https://docs.openalgo.in)** (self-hosted, 24+ Indian
> brokers — Zerodha / Angel One / Fyers / Upstox / Dhan / ICICI etc.)

---

## 🧱 Architecture (एक नज़र में)

```
 आपका Broker (Zerodha/Angel/…)
        │  (broker WebSocket)
        ▼
   OpenAlgo Server (localhost:5000)
        │  (WebSocket :8765, mode 2 = Quote, mode 3 = Depth/L2)
        ▼
   tick_recorder.py  ──►  Queue  ──►  Batch INSERT
                                          │
                                          ▼
                              TimescaleDB (hypertable)
                                  ├─ ticks   (OHLCV + LTP)
                                  └─ depth   (5-level L2)
```

* OpenAlgo एक **single broker connection** से सब symbols को multiplex करता है,
  इसलिए 50 शेयरों के लिए सिर्फ़ **1** broker WS subscription लगती है।
* `subscribe_quote` (mode 2) → हर update पर OHLCV + LTP + volume + ATP।
* `subscribe_depth` (mode 3) → 5-level **bid/ask order book** (L2)।
  *(Dhan पर 20-level तक support है, बाकी brokers पर 5-level।)*

---

## ✅ Pre-requisites

| | |
|---|---|
| Python | 3.10+ |
| TimescaleDB | 2.13+ (PostgreSQL 14/15/16 पर) |
| OpenAlgo | locally चल रहा हो (default `http://127.0.0.1:5000`) |
| Broker account | जिसमें live data subscription एक्टिव हो |

---

## 🚀 Setup — 5 Steps

### 1) Repo clone और dependencies
```bash
git clone https://github.com/m14035075-ops/Trading-TimescaleDB-Short-1.git
cd Trading-TimescaleDB-Short-1
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2) TimescaleDB तैयार करें
```bash
# एक नया database बनाएँ
createdb -U postgres marketdata

# Tables + hypertables + compression policy create करें
psql -U postgres -d marketdata -f schema.sql
```

`schema.sql` ये करता है:
* `ticks` और `depth` दो hypertables (1-day chunks)
* 7 दिन पुराने chunks पर **automatic compression** (~10x storage saving)
* Optional **retention** policy (180 दिन के बाद auto-delete) — comment-out है
* Optional **continuous aggregate** for 1-min OHLCV — comment-out है

### 3) OpenAlgo चालू करें
[OpenAlgo install guide](https://docs.openalgo.in/getting-started) देखकर अपने
broker के साथ login कर लें। Confirm करें कि `http://127.0.0.1:5000` खुलता है।
Settings पेज से अपनी **API key** copy कर लें।

### 4) Configuration
```bash
cp .env.example .env
nano .env          # API key, DB password आदि भर दें
```

ज़रूरी fields:
```ini
OPENALGO_API_KEY=...
PG_PASSWORD=...
RECORD_MODE=both          # quote / depth / both
```

`symbols.txt` को edit करके अपने 50 शेयर रख सकते हैं — एक line में
`EXCHANGE,SYMBOL` (default में Nifty 50 भरे हुए हैं)।

### 5) Recorder चलाएँ
```bash
python tick_recorder.py
```

Output कुछ ऐसा दिखेगा:
```
09:14:55 [INFO] Loaded 50 symbols from symbols.txt
09:14:55 [INFO] Mode = BOTH | Batch = 200 | Flush = 1.0s
09:14:55 [INFO] TimescaleDB connected → 127.0.0.1/marketdata
09:14:56 [INFO] Subscribing QUOTE for 50 symbols…
09:15:01 [INFO] Subscribing DEPTH (L2) for 50 symbols…
09:15:01 [INFO] Streaming live data… (Ctrl+C to stop)
```

---

## 🔁 Connection Drop / Recovery कैसे काम करता है

Script में **तीन-स्तरीय (3-layer)** safety है:

### Layer 1 — WebSocket reconnect (exponential backoff)
* `FeedManager.run()` एक loop में बैठा रहता है।
* जब OpenAlgo से connection टूटे (`self.client.connected == False`),
  script **2s → 4s → 8s → … → 60s (cap)** के backoff से दोबारा connect करती है।
* Reconnect होने के बाद **सभी symbols अपने आप re-subscribe** हो जाते हैं।

### Layer 2 — Database reconnect
* `DBWriter` thread अगर insert के दौरान fail हो जाए, तो connection बंद करके
  **हर 5s** में retry करता है। इस दौरान आने वाले ticks queue (max 1 lakh rows)
  में buffer रहते हैं।

### Layer 3 — Graceful shutdown
* `Ctrl+C` (SIGINT) पर pending buffer को **DB में flush किया जाता है**, फिर
  process clean exit करता है — कोई data loss नहीं।

> **तो data loss कब हो सकता है?**
> सिर्फ़ उन seconds का जब broker → OpenAlgo की link भी टूटी हो *और* आपका
> recorder भी बंद हो। Live ticks का nature ही ऐसा है — broker उन्हें replay नहीं
> करता। **Historical gap भरने** के लिए OpenAlgo का `client.history(...)` REST
> endpoint use करके आप बाद में 1-min/5-min OHLCV backfill कर सकते हैं।

---

## 🧪 Verify — DB में data आ रहा है?

```sql
-- last 1 minute में कितने ticks आए?
SELECT COUNT(*) FROM ticks WHERE time > now() - interval '1 minute';

-- RELIANCE का latest LTP
SELECT time, ltp, volume FROM ticks
WHERE symbol = 'RELIANCE'
ORDER BY time DESC LIMIT 5;

-- Order book snapshot (latest)
SELECT time, side, level, price, quantity
FROM depth
WHERE symbol = 'RELIANCE'
ORDER BY time DESC, side, level
LIMIT 10;

-- Storage usage
SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass))
FROM timescaledb_information.hypertables;
```

---

## 🛠️ Production में 24×7 चलाने के लिए

**systemd service** (`/etc/systemd/system/tick-recorder.service`):
```ini
[Unit]
Description=OpenAlgo Tick Recorder
After=network.target postgresql.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/Trading-TimescaleDB-Short-1
ExecStart=/home/ubuntu/Trading-TimescaleDB-Short-1/.venv/bin/python tick_recorder.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable --now tick-recorder
sudo journalctl -u tick-recorder -f      # live logs
```

Crash हो जाए तो `Restart=always` अपने आप दोबारा शुरू कर देगा। Pre/post-market के
hours में recorder को रोकने के लिए **cron + `systemctl start/stop`** लगा सकते हैं।

---

## 📊 Storage estimate

| | |
|---|---|
| Ticks/day | 50 × 1 tick/sec × ~22,500 sec ≈ **1.1 M rows** |
| Depth rows/day | 50 × 1 update/sec × 10 levels ≈ **11 M rows** |
| Raw size | ~1.5 GB/day |
| Compressed (7d बाद) | **~150 MB/day** (≈10x कम) |

---

## ⚙️ Tuning

| Variable | Default | क्या करता है |
|---|---|---|
| `BATCH_SIZE` | 200 | इतनी rows जमा होने पर DB में flush |
| `FLUSH_INTERVAL_SEC` | 1.0 | या इतने seconds में force flush |
| `RECONNECT_MIN/MAX` | 2 / 60 | reconnect backoff bounds |
| `RECORD_MODE` | both | `quote` only रखेंगे तो storage 90% बच जाती है |

अगर सिर्फ़ 1-second OHLCV चाहिए (L2 नहीं), तो `RECORD_MODE=quote` कर दें — यही
सबसे common production setup है।

---

## ❓ FAQ

**Q: 50 से ज़्यादा शेयर add कर सकते हैं?**
हाँ, बस `symbols.txt` में लाइनें जोड़ दें। OpenAlgo हज़ारों subscriptions
handle कर लेता है।

**Q: F&O (NFO/BFO) symbols भी जोड़ सकते हैं?**
हाँ — `NFO,NIFTY28NOV24FUT` जैसी line डाल दें।

**Q: Tick exact 1-second का है?**
Broker जब-जब tick भेजे, उतनी frequency पर हम store करते हैं। Quote mode पर
ज़्यादातर brokers 1-sec throttled feed देते हैं। हर tick का अपना `timestamp`
broker से ही आता है — हम वही DB में डालते हैं।

**Q: Multiple instances चला सकते हैं (sharding)?**
हाँ — `symbols.txt` को 2 fileों में बाँटकर 2 अलग processes चला दें। दोनों एक ही
TimescaleDB में लिखेंगे।

---

## 📂 File structure

```
Trading-TimescaleDB-Short-1/
├── schema.sql           # TimescaleDB hypertables + policies
├── symbols.txt          # 50 NSE symbols (editable)
├── requirements.txt
├── .env.example
├── tick_recorder.py     # main recorder (~270 lines)
└── README.md            # यह file
```

---

## 📝 License

MIT — स्वतंत्र रूप से use करें।
