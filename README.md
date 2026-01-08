# Big Data - Interactive Advertising Analytics Project

## 🎯 Cel projektu

Stworzenie systemu do analizy pogody, ruchu drogowego i sentymentu w Warszawie w celu wykorzystania w reklamamach. Projekt powinen umożliwiać:

- Wykrywanie korków
- Analizę skuteczności pogody
- Analizę tweetów pod kątem negatywnego sentymentu
- **Decyzje Reklamowe (Ad Decision Engine)**: Automatyczne podejmowanie decyzji o wyświetleniu reklamy "na pocieszenie" (kampania eskapistyczna) w oparciu o złą pogodę, korki i negatywne nastroje społeczne.

---

## 🛠️ Stos technologiczny

| Technologia      | Wersja   | Zastosowanie                         | Port       |
| ---------------- | -------- | ------------------------------------ | ---------- |
| **Apache Kafka** | 7.4.0    | Buforowanie i streaming danych       | 9092       |
| **Kafka UI**     | latest   | Interfejs do monitorowania Kafka     | 8090       |
| **Apache NiFi**  | latest   | Pobieranie i preprocessing danych    | 8443       |
| **Apache Spark** | latest   | Przetwarzanie danych i analityka     | 8080, 7077 |
| **Hadoop HDFS**  | latest   | Długoterminowe przechowywanie danych | 9870, 9000 |
| **HBase**        | latest   | Szybki dostęp do danych              | 16010      |
| **Apache Hive**  | Embedded | Hurtownia danych (via Spark)         | -          |
| **Zookeeper**    | 7.4.0    | Koordynacja usług rozproszonych      | 2181       |

---

## � Konfiguracja Twitter API

Aby pobierać tweety z Warszawy, musisz skonfigurować klucz API Twittera.

### 1. Utwórz plik `.env`

W głównym katalogu projektu utwórz plik `.env` z kluczem API:

```env
TWITTER_API_KEY=twój_klucz_api_tutaj
```

### 2. Skąd wziąć klucz API?

Projekt używa zewnętrznego API Twitter (`api.twitterapi.io`). Aby uzyskać klucz:

1. Zarejestruj się na platformie dostawcy API
2. Wygeneruj klucz API
3. Skopiuj klucz do pliku `.env`

### 3. Weryfikacja konfiguracji

Po uruchomieniu systemu, NiFi automatycznie użyje klucza z pliku `.env` do uwierzytelniania żądań do Twitter API.

Możesz przetestować połączenie:

```powershell
python tests/twitter_api.py
```

> **Uwaga**: Bez prawidłowego klucza API, pobieranie tweetów nie będzie działać, ale pozostałe źródła danych (ZTM, pogoda, jakość powietrza) będą funkcjonować normalnie.

---

## �📦 Instalacja

### Wymagania systemowe

- **System operacyjny**: Windows 10/11 z WSL 2, Linux lub macOS
- **RAM**: 16GB
- **Dysk**: ~10GB wolnego miejsca
- **Docker Desktop**: najnowsza wersja

### Instalacja na Windows

#### 1. Instalacja WSL 2

Otwórz PowerShell jako **Administrator** i wykonaj:

```powershell
wsl --install
```

Po instalacji system poprosi o restart. Po restarcie:

- Ustaw login i hasło dla WSL (nie będzie to potem potrzebne, ale proponuję jakieś łatwe typu admin, password)

#### 2. Instalacja Docker Desktop

1. Pobierz Docker Desktop ze strony: https://www.docker.com/products/docker-desktop/
2. Uruchom instalator
3. **WAŻNE**: Podczas instalacji upewnij się, że zaznaczone jest:
   - ✅ **Use WSL 2 based engine**
4. Po instalacji uruchom Docker Desktop
5. Poczekaj aż Docker się w pełni uruchomi

#### 3. Uruchomienie projektu

Sklonuj repozytorium i przejdź do katalogu projektu:

```powershell
cd big-data-interactive-ads
```

Stwórz środowisko wirtualne i zaimportuj biblioteki python za pomocą uv:

```powershell
uv venv .venv
.venv\Scripts\activate
uv sync
```

Jeśli nie posiadasz uv:

```powershell
pip install uv
```

Uruchom wszystkie usługi na docker:

```powershell
docker-compose up -d
```

#### 4. Weryfikacja instalacji

Poczekaj 2-3 minuty na uruchomienie wszystkich usług, następnie sprawdź status:

```powershell
docker-compose ps
```

**Prawidłowy wynik powinien wyglądać tak:**

```
NAME           IMAGE                             STATUS
datanode       bde2020/hadoop-datanode:latest    Up
hbase          harisekhon/hbase:latest           Up
kafka          confluentinc/cp-kafka:7.4.0       Up
kafka-ui       provectuslabs/kafka-ui:latest     Up
namenode       bde2020/hadoop-namenode:latest    Up
nifi           apache/nifi:latest                Up
spark-master   apache/spark:latest               Up
spark-worker   apache/spark:latest               Up
zookeeper      confluentinc/cp-zookeeper:7.4.0   Up
```

Wszystkie kontenery powinny mieć status **"Up"**.

#### 5. Dostęp do interfejsów webowych

Po uruchomieniu, sprawdź czy wszystkie interfejsy są dostępne:

| Usługa              | URL                         | Opis                                                       |
| ------------------- | --------------------------- | ---------------------------------------------------------- |
| **Kafka UI**        | http://localhost:8090       | Monitor Kafka topics i messages                            |
| **NiFi**            | https://localhost:8443/nifi | Przepływy danych (login: `admin` / hasło: `adminadmin123`) |
| **Spark Master**    | http://localhost:8080       | Monitor Spark jobs                                         |
| **Hadoop NameNode** | http://localhost:9870       | HDFS filesystem                                            |
| **HBase Master**    | http://localhost:16010      | HBase tables                                               |

---

#### 6. Weryfikacja automatycznej konfiguracji

Po uruchomieniu `docker-compose up -d`, system **automatycznie** wykonuje pełną konfigurację:

- ✅ Czeka na gotowość wszystkich usług (Kafka, HBase, NiFi)
- ✅ Tworzy wszystkie tematy Kafka
- ✅ Tworzy wszystkie tabele HBase
- ✅ Wgrywa i instancjonuje szablon NiFi na canvas

Sprawdź logi automatycznej konfiguracji:

```powershell
docker-compose logs setup
```

Na końcu logów powinieneś zobaczyć:

```
✓ SETUP COMPLETED SUCCESSFULLY!
```

Jeśli zobaczysz błędy, uruchom ponownie:

```powershell
docker-compose restart setup
docker-compose logs -f setup
```

#### 7. Uruchomienie przepływów danych

> **Ważne**: Po automatycznej konfiguracji z poprzedniego kroku, musisz ręcznie uruchomić przepływy danych. Automatyczna konfiguracja tylko **przygotowuje** infrastrukturę (tematy, tabele, szablon), ale nie startuje pobierania i przetwarzania danych.

**7.1. Uruchom przepływy NiFi** (pobieranie danych z API):

#### macOS / Linux

```bash
./scripts/run_nifi_flows.sh
```

#### Windows (PowerShell)

```powershell
.\scripts\run_nifi_flows.ps1
```

To uruchomi wszystkie procesory NiFi, które będą pobierać dane z:

- ZTM API (autobusy i trolejbusy)
- Open-Meteo API (pogoda i jakość powietrza)
- Twitter API (tweety z Warszawy)

**7.2. Uruchom zadania Spark** (przetwarzanie danych):

#### macOS / Linux

```bash
./scripts/run_spark_jobs.sh
```

#### Windows (PowerShell)

```powershell
.\scripts\run_spark_jobs.ps1
```

To uruchomi:

1. **5 zadań Spark Streaming** (przetwarzanie danych z Kafka do HBase):
   - Buses
   - Trolleys
   - Weather
   - Air Quality
   - Twitter Sentiment
2. **Ad Campaign Manager** (niezależny proces Python podejmujący decyzje)
3. **Hive Archiver Scheduler** (automatyczny proces w tle)

Teraz sprawdź czy zadania na Spark'u się odpaliły: http://localhost:8080

Poczekaj 30 sek. Powinieneś zobaczyć **5 aktywnych aplikacji streamingowych** w sekcji "Running Applications".

> **Uwaga**: `ad_campaign_manager.py` oraz `archive_to_hive.py` (scheduler) działają jako procesy w tle i nie zawsze są widoczne na głównej liście aplikacji streamingowych w Spark UI (chyba że w momencie wykonywania batcha).

Jeśli nie zobaczysz zadań, zrestartuj:

#### macOS / Linux

```bash
./scripts/stop_spark_jobs.sh
./scripts/run_spark_jobs.sh
```

#### Windows (PowerShell)

```powershell
.\scripts\stop_spark_jobs.ps1
.\scripts\run_spark_jobs.ps1
```

**Zatrzymywanie przepływów danych:**

#### macOS / Linux

```bash
# Zatrzymaj NiFi procesory
./scripts/stop_nifi_flows.sh

# Zatrzymaj zadania Spark + Ad Manager
./scripts/stop_spark_jobs.sh
```

#### Windows (PowerShell)

```powershell
# Zatrzymaj NiFi procesory
.\scripts\stop_nifi_flows.ps1

# Zatrzymaj zadania Spark + Ad Manager
.\scripts\stop_spark_jobs.ps1
```

#### 8. Weryfikacja działania systemu

Poczekaj 2-3 minuty na zebranie pierwszych danych, następnie zweryfikuj:

**8.1. Sprawdź dane w HBase:**

```powershell
docker-compose exec hbase hbase shell
```

W HBase shell wykonaj:

```hbase
list
scan 'transport_events', {LIMIT => 1}
scan 'air_quality_forecast', {LIMIT => 1}
scan 'weather_forecast', {LIMIT => 1}
scan 'tweets', {LIMIT => 1}
scan 'ad_decisions', {LIMIT => 1}
exit
```

Jeśli zobaczysz dane w tabelach - system działa poprawnie! ✅

**8.2. Monitoruj dane w Kafka UI:**

Otwórz http://localhost:8090 i sprawdź tematy:

- `ztm-buses-raw` - powinny pojawiać się dane o autobusach
- `weather-forecast-raw` - dane pogodowe
- `air-quality-raw` - dane o jakości powietrza
- `tweets-warsaw-raw` - tweety z Warszawy
- `ad-decisions` - wyniki decyzji reklamowych

---

## 📊 Analityka i Archiwizacja (Hive)

System posiada dedykowaną warstwę analityczną opartą o **Apache Hive** (zintegrowany ze Spark SQL), która archiwizuje decyzje reklamowe na HDFS w formacie Parquet z partycjonowaniem Hive.

### 1. Architektura

- **Decyzje (Real-time)**: `ad_campaign_manager.py` (Docker Service) wysyła decyzje do **Kafka** (`ad-decisions`) i **HBase** co minutę.
- **Archivizacja (Batch)**: `archive_scheduler.py` (Docker Service) uruchamia co godzinę przenoszenie danych z HBase do tabeli Hive na HDFS w lokalizacji `/user/archive/ad_decisions` (format Parquet, partycjonowanie po dacie i godzinie).

### 2. Monitorowanie Archiwizacji

Archive Scheduler działa automatycznie jako Docker service. Sprawdź jego status:

#### macOS / Linux / Windows

```bash
# Sprawdź logi schedulera
docker logs -f archive-scheduler

# Lub sprawdź szczegółowe logi jobów
docker exec archive-scheduler tail -f /opt/spark-apps/scheduler.log
```

---

## 🔍 Jak Zapytać Wyniki?

System oferuje pięć sposobów dostępu do danych: **HBase** (real-time), **Hive/HDFS** (archiwum), **Jupyter Notebook** (analiza wizualna), **HDFS Browser** i **Kafka** (streaming).

### Metoda 1: Zapytania HBase (Real-Time Data)

HBase przechowuje dane z ostatnich 24 godzin - najlepsze dla zapytań real-time.

#### Przykład 1: Skanowanie ostatnich decyzji

```bash
# Uruchom HBase shell
docker exec -it hbase hbase shell

# W HBase shell:
# Sprawdź ostatnie 5 decyzji
scan 'ad_decisions', {LIMIT => 5}

# Sprawdź konkretną decyzję
get 'ad_decisions', '20260108_143000'

# Skanuj zakres czasowy (ostatnie 10 minut)
scan 'ad_decisions', {STARTROW => '20260108_143000', STOPROW => '20260108_144000'}

# Wyjdź
exit
```

#### Przykład 2: Sprawdzanie innych tabel

```bash
# Transport events
scan 'transport_events', {LIMIT => 2}

# Weather forecast
scan 'weather_forecast', {LIMIT => 2}

# Air quality
scan 'air_quality_forecast', {LIMIT => 2}

# Tweets
scan 'tweets', {LIMIT => 2}
```

---

### Metoda 2: Zapytania Hive/Spark SQL (Historical Data)

Dane archiwalne w formacie Parquet można odpytywać przez Spark SQL. 

> **Ważne**: Zapytania muszą być wykonywane przez kontener `archive-scheduler`, który ma skonfigurowany dostęp do Hive metastore.

```bash
# Uruchom Spark SQL shell
docker exec -it archive-scheduler /opt/spark/bin/spark-sql

# W Spark SQL:
# Pokaż wszystkie tabele
SHOW TABLES;

# Sprawdź schemat tabeli
DESCRIBE ad_decisions_archive;

# Sprawdź partycje
SHOW PARTITIONS ad_decisions_archive;

# Prosty SELECT
SELECT * FROM ad_decisions_archive LIMIT 10;

# Statystyki decyzji
SELECT 
    decision_result, 
    COUNT(*) as count,
    AVG(global_score) as avg_score
FROM ad_decisions_archive
GROUP BY decision_result;

# Decyzje z konkretnej daty
SELECT * FROM ad_decisions_archive 
WHERE dt = '20260108' 
LIMIT 20;

# Decyzje z konkretnej godziny
SELECT * FROM ad_decisions_archive 
WHERE dt = '20260108' AND hr = '14'
ORDER BY decision_id DESC;

# Top 10 najwyższych score'ów
SELECT decision_id, global_score, decision_result 
FROM ad_decisions_archive 
ORDER BY global_score DESC 
LIMIT 10;
```

---

### Metoda 3: Analiza w Jupyter Notebook

System zawiera gotowy Jupyter Notebook z wizualizacjami.

#### Uruchomienie Notebook

1. **Zainstaluj Jupyter** (jeśli nie masz):
   ```bash
   pip install jupyter pandas matplotlib seaborn happybase
   ```

2. **Uruchom Jupyter**:
   ```bash
   jupyter notebook
   ```

3. **Otwórz** `analyse/ad_decisions_analysis.ipynb`

#### Co znajdziesz w Notebook?

- 📈 **Wykres czasowy** global score
- 📊 **Rozkład decyzji** (SHOW_AD vs NO_AD)
- 🔥 **Heatmapa korelacji** między wskaźnikami
- 📉 **Statystyki opisowe** dla wszystkich score'ów
- 🕐 **Analiza wzorców czasowych** (godziny szczytu)

#### Przykład użycia Notebook:

```python
# Load last 24 hours of data from HBase
decisions = load_decisions_from_hbase(hours=24)
df = pd.DataFrame(decisions)

# Quick stats
print(df.describe())

# Decision distribution
df['decision'].value_counts().plot(kind='bar')

# Score correlation heatmap
sns.heatmap(df[['traffic_score', 'weather_score', 'sentiment_score', 'global_score']].corr(), 
            annot=True, cmap='coolwarm')
```

---

### Metoda 4: Przeglądanie HDFS przez Przeglądarkę

**URL**: http://localhost:9870/explorer.html#/user/archive/ad_decisions

Możesz:
- Przeglądać strukturę partycji (`dt=20260108/hr=14/`)
- Pobierać pliki Parquet
- Sprawdzać rozmiar danych
- Weryfikować uprawnienia

---

### Metoda 5: Monitoring Kafka (Real-Time Decisions)

Monitoruj decyzje w czasie rzeczywistym przez Kafka UI lub bezpośrednio z topicu.

#### Kafka UI

**URL**: http://localhost:8090

1. Przejdź do **Topics** → **ad-decisions**
2. Kliknij **Messages**
3. Zobacz ostatnie decyzje w czasie rzeczywistym

#### Konsumowanie przez CLI

```bash
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic ad-decisions \
    --from-beginning \
    --max-messages 10
```

---

## 🧪 Przykładowe Zapytania Analityczne

### 1. Skuteczność kampanii (conversion rate)

```sql
-- W Spark SQL
SELECT 
    dt,
    COUNT(*) as total,
    SUM(CASE WHEN decision_result = 'SHOW_AD' THEN 1 ELSE 0 END) as shown,
    ROUND(SUM(CASE WHEN decision_result = 'SHOW_AD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as show_rate_pct
FROM ad_decisions_archive
GROUP BY dt
ORDER BY dt DESC;
```

### 2. Średnie score'y w szczycie vs poza szczytem

```sql
SELECT 
    CASE 
        WHEN hr IN ('07','08','09','16','17','18') THEN 'Rush Hour'
        ELSE 'Off-Peak'
    END as period,
    AVG(traffic_score) as avg_traffic,
    AVG(weather_score) as avg_weather,
    AVG(global_score) as avg_global
FROM ad_decisions_archive
GROUP BY 
    CASE 
        WHEN hr IN ('07','08','09','16','17','18') THEN 'Rush Hour'
        ELSE 'Off-Peak'
    END;
```

### 3. Top 5 dni z najwyższym global score

```sql
SELECT 
    dt,
    AVG(global_score) as avg_score,
    MAX(global_score) as max_score,
    COUNT(*) as decisions
FROM ad_decisions_archive
GROUP BY dt
ORDER BY avg_score DESC
LIMIT 5;
```

---

## 📋 Szybkie Porównanie Metod Dostępu

| Metoda | Zakres Danych | Opóźnienie | Najlepsze Dla |
|--------|---------------|------------|---------------|
| **HBase Shell** | Ostatnie 24h | <10ms | Real-time monitoring, pojedyncze rekordy |
| **Spark SQL/Hive** | Pełne archiwum | ~5-10s | Agregacje, analizy historyczne, raporty |
| **Jupyter Notebook** | Ostatnie 24h (HBase) | <1s | Wizualizacje, eksploracja danych |
| **Kafka UI** | Streaming | Real-time | Monitoring decyzji na żywo |
| **HDFS Browser** | Pełne archiwum | - | Przeglądanie plików, weryfikacja partycji |

---

## 🔄 Podsumowanie workflow

```
1. docker-compose up -d          → Uruchamia wszystkie usługi + auto-konfiguracja
2. docker-compose logs setup     → Sprawdź czy konfiguracja się powiodła
3. .\scripts\run_nifi_flows.ps1  → Uruchom pobieranie danych
4. .\scripts\run_spark_jobs.ps1  → Uruchom przetwarzanie danych
5. Monitoruj w UI                → Kafka UI, NiFi, Spark Master, HBase
```

**Ponowne uruchomienie po zatrzymaniu:**

#### macOS / Linux

```bash
docker-compose down              # Zatrzymaj wszystko
docker-compose up -d             # Uruchom ponownie
./scripts/run_nifi_flows.sh      # Uruchom NiFi
./scripts/run_spark_jobs.sh      # Uruchom Spark
```

#### Windows (PowerShell)

```powershell
docker-compose down              # Zatrzymaj wszystko
docker-compose up -d             # Uruchom ponownie
.\scripts\run_nifi_flows.ps1     # Uruchom NiFi
.\scripts\run_spark_jobs.ps1     # Uruchom Spark
```

## ⚙️ Uruchamianie wybranych usług

Ze względu na ograniczenia pamięci RAM (16GB), możesz uruchamiać tylko wybrane usługi zamiast całego stosu.

### Przykład: Core Services (Kafka + Zookeeper)

Tylko podstawowe usługi do przesyłania danych:

```powershell
docker-compose up -d zookeeper kafka kafka-ui
```

### Zatrzymywanie usług

Zatrzymaj wszystkie uruchomione usługi:

```powershell
docker-compose down
```

Zatrzymaj wybrane usługi (np. tylko NiFi):

```powershell
docker-compose stop nifi
```

### Sprawdzanie użycia zasobów

Monitoruj użycie pamięci RAM i CPU przez kontenery:

```powershell
docker stats --no-stream
```
