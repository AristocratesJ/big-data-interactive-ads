# Big Data - Interactive Advertising Analytics Project

## 🎯 Cel projektu

Stworzenie systemu do analizy pogody, ruchu drogowego i sentymentu w Warszawie w celu wykorzystania w reklamamach. Projekt powinen umożliwiać:

- Wykrywanie korków
- Analizę skuteczności pogody
- Analizę tweetów pod kątem negatywnego sentymentu
- Całościowe wykorzystanie analiz w celu wybrania miejsca i czasu na wyświetlanie reklam

---

## 🛠️ Stos technologiczny

| Technologia      | Wersja | Zastosowanie                         | Port       |
| ---------------- | ------ | ------------------------------------ | ---------- |
| **Apache Kafka** | 7.4.0  | Buforowanie i streaming danych       | 9092       |
| **Kafka UI**     | latest | Interfejs do monitorowania Kafka     | 8090       |
| **Apache NiFi**  | latest | Pobieranie i preprocessing danych    | 8443       |
| **Apache Spark** | latest | Przetwarzanie danych i analityka     | 8080, 7077 |
| **Hadoop HDFS**  | latest | Długoterminowe przechowywanie danych | 9870, 9000 |
| **HBase**        | latest | Szybki dostęp do danych              | 16010      |
| **Zookeeper**    | 7.4.0  | Koordynacja usług rozproszonych      | 2181       |

---

## 📦 Instalacja

### Wymagania systemowe

- **System operacyjny**: Windows 10/11 z WSL 2 (lub Linux/macOS)
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
