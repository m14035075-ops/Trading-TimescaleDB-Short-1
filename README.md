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
> सिर्फ़ उन seconds का जब OpenAlgo *और* recorder **दोनों एक साथ बंद हों** (server
> reboot, OpenAlgo crash, power cut)। उस window की **live ticks lost हो जाती हैं**
> क्योंकि broker उन्हें replay नहीं करता। उन्हें भरने के लिए ⤵

---

## 🩹 Gap Recovery — `backfill.py`

जब दोनों एक साथ नीचे थे, उस window का **1-minute OHLCV** OpenAlgo के REST
`history()` API से लाकर `bars_1m` hypertable में भर देती है। यह 100% safe है —
`ON CONFLICT DO NOTHING` लगा है, इसलिए जितनी बार चलाएँ, duplicates नहीं बनेंगे।

```bash
# Recorder restart होने के तुरंत बाद चलाएँ — last gap अपने आप detect होगा
python backfill.py

# पिछले 3 दिन का data force-fill करना है?
python backfill.py --days 3

# सिर्फ़ एक symbol के लिए?
python backfill.py --symbol RELIANCE
```

**कैसे काम करता है:**
1. हर symbol का `bars_1m` में *last timestamp* पढ़ता है
2. वहाँ से अब तक का 1-min OHLCV broker से REST API के ज़रिए मँगवाता है
3. `bars_1m` में insert (primary key duplicate-safe)

**दो tables क्यों?**
| Table | किससे भरती है | Resolution | कब use करें |
|---|---|---|---|
| `ticks` | live WebSocket (real-time) | per-second | live monitoring, micro-strategies |
| `bars_1m` | REST history (gap-fill / EOD reconcile) | 1-minute | backtesting, gap recovery |

> **Tip:** systemd से `tick-recorder` के साथ ही एक छोटा daily cron लगा दें जो
> `backfill.py` को market close के बाद चलाए — हर रात आपका 1-min OHLCV भी
> consistent रहेगा।
> ```cron
> 30 16 * * 1-5  cd /home/ubuntu/Trading-TimescaleDB-Short-1 && .venv/bin/python backfill.py
> ```

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

## 🖥️ Same-Server Deployment (सब कुछ एक ही machine पर)

अगर **OpenAlgo + TimescaleDB + recorder** तीनों एक ही server पर चलाने हैं
(typical VPS setup), तो default `.env` values में **कुछ बदलने की ज़रूरत नहीं** —
सब कुछ `127.0.0.1` पर ही point करता है।

### Port map
| Service | Port | Bind |
|---|---|---|
| PostgreSQL / TimescaleDB | 5432 | localhost |
| OpenAlgo HTTP / REST | 5000 | localhost |
| OpenAlgo WebSocket | 8765 | localhost |
| `tick_recorder.py` | — (client only) | — |

> सुरक्षा के लिए OpenAlgo और Postgres को **localhost** पर ही रखें (firewall से
> public access block करें)। Loopback connections fastest भी हैं — कोई network
> latency नहीं।

### Resource estimate (same server)
| | RAM | Disk | CPU |
|---|---|---|---|
| OpenAlgo | ~400 MB | — | 1 core idle |
| TimescaleDB | ~1 GB | ~150 MB/day (compressed) | 1 core |
| Recorder | ~100 MB | — | 0.2 core |
| **Total (recommended)** | **2 vCPU / 4 GB / 50 GB SSD** बहुत है | | |

### Startup order (बहुत ज़रूरी)
```
PostgreSQL  →  OpenAlgo  →  tick_recorder.py
```
recorder पहले चालू होकर crash-loop में जाएगा अगर OpenAlgo अभी up नहीं है — इसलिए
systemd में dependency declare करना ज़रूरी है।

### Production में 24×7 चलाने के लिए — systemd

**1) OpenAlgo service** (`/etc/systemd/system/openalgo.service`):
```ini
[Unit]
Description=OpenAlgo Server
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/openalgo
ExecStart=/home/ubuntu/openalgo/.venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**2) Recorder service** (`/etc/systemd/system/tick-recorder.service`):
```ini
[Unit]
Description=OpenAlgo Tick Recorder
After=network.target postgresql.service openalgo.service
Requires=postgresql.service openalgo.service
PartOf=openalgo.service        # OpenAlgo बंद → recorder भी बंद (एक साथ)
BindsTo=openalgo.service       # OpenAlgo crash → recorder भी रुक जाए

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/Trading-TimescaleDB-Short-1
ExecStart=/home/ubuntu/Trading-TimescaleDB-Short-1/.venv/bin/python tick_recorder.py
ExecStartPost=/bin/bash -c 'sleep 30 && /home/ubuntu/Trading-TimescaleDB-Short-1/.venv/bin/python /home/ubuntu/Trading-TimescaleDB-Short-1/backfill.py || true'
Restart=always
RestartSec=15
KillSignal=SIGINT              # graceful shutdown — pending buffer DB में flush होगा
TimeoutStopSec=20

[Install]
WantedBy=multi-user.target
```

**3) (Optional) एक `target` से दोनों को एक साथ start/stop करें**
`/etc/systemd/system/tick-pipeline.target`:
```ini
[Unit]
Description=Tick Pipeline (OpenAlgo + Recorder)
Requires=openalgo.service tick-recorder.service
After=openalgo.service tick-recorder.service

[Install]
WantedBy=multi-user.target
```

**4) Enable + start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now postgresql openalgo tick-recorder tick-pipeline.target
sudo journalctl -u tick-recorder -f      # live logs

# एक command से दोनों बंद / चालू:
sudo systemctl stop  tick-pipeline.target
sudo systemctl start tick-pipeline.target
```

**ये क्या करते हैं?**

| Directive | असर |
|---|---|
| `Requires=` | reboot पर सही order में उठाता है |
| `PartOf=` | `systemctl stop openalgo` → recorder अपने आप रुकेगा |
| `BindsTo=` | OpenAlgo crash → recorder भी stop (कोई dead WS retry-storm नहीं) |
| `KillSignal=SIGINT` | recorder को graceful stop — pending buffer **DB में flush** होगा |
| `ExecStartPost` | recorder restart के 30s बाद **`backfill.py` अपने आप चलकर gap भर देगा** ⭐ |

Pre/post-market hours में recorder को रोकने के लिए **cron**:
```cron
15 9  * * 1-5  systemctl start tick-recorder
45 15 * * 1-5  systemctl stop  tick-recorder
```

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
