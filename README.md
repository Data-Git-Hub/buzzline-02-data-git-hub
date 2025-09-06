# buzzline-02-data-git-hub

Streaming data is often too big for any one machine. Apache Kafka is a popular streaming platform that uses publish-subscribe patterns:

- **Producers** publish streaming data to topics
- **Consumers** subscribe to topics to process data in real-time

We'll write Python producers and consumers to work with Kafka topics.

Kafka needs space - it's big. 

It also comes from the Linux world. We'll use WSL on Windows machines.

## Copy This Example Project & Rename

1. Copy/fork this project into your GitHub account and create your own version of this project to run and experiment with.
2. Name it `buzzline-02-yourname` where yourname is something unique to you.

## Task 1. Install and Start Kafka (using WSL if Windows)

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> first.
Python 3.11 is required.

In this task, we will download, install, configure, and start a local Kafka service.

1. Install Windows Subsystem for Linux (Windows machines only)
2. Install Kafka Streaming Platform
3. Start the Kafka service (leave the terminal open).

For detailed instructions, see:

- [SETUP_KAFKA](SETUP_KAFKA.md)

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment
2. Activate the virtual environment
3. Upgrade pip
4. Install from requirements.txt

### Windows

Open PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

## Task 3. Start a Kafka Producer

Producers generate streaming data for our topics.

In VS Code, open a terminal.
Use the commands below to activate .venv, and start the producer.

Windows:

```shell
.venv\Scripts\activate
py -m producers.kafka_producer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m producers.kafka_producer_case
```

## Task 4. Start a Kafka Consumer

Consumers process data from topics or logs in real time.

In VS Code, open a NEW terminal in your root project folder.
Use the commands below to activate .venv, and start the consumer.

Windows:

```shell
.venv\Scripts\activate
py -m consumers.kafka_consumer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_case
```

# Task 5 P.2 Pokémon Battle Simulator

Stream structured Pokémon battle events through Apache Kafka.
A Python producer emits JSON battle events; a Python consumer validates moves, tracks HP/KO, and prints rolling analytics.

What You’ll Build

## Producer (Python): randomly picks a trainer & opponent, selects a legal move (from pokemon_attack.csv), looks up damage (from pokemon_damage.csv), and emits:

```shell
{ "battle_id": "...", "turn": 1, "trainer": "Snorlax", "opponent": "Charmander", "move": "Shadow Ball", "damage": 12, "ts": "2025-09-05T23:36:17.549414Z" }
```


## Consumer (Python): applies HP (from pokemon_hp.csv), logs the turn in natural language, detects KO, and prints minute-window metrics every 15 seconds.

### Immediate per-event logs (consumer)
```shell
Snorlax uses Shadow Ball that does 12 damage to Charmander. Charmander has 88 HP left!
```

### KO event (consumer)
```shell
Snorlax knocks out Charmander and wins the battle (f97f5ba8)!
```

### Alerts if something ever goes wrong (consumer)
```shell
[ALERT][ILLEGAL MOVE] Trainer=Pikachu used move='Water Gun' (not legal). Event={"battle_id":"...","turn":...}
```

### Staggered minute-window metrics (every 15s, covering last 60s)
```shell
[METRICS last 60s][KOs] total=3
[METRICS last 60s][Most Damage Given] Gengar with 124
[METRICS last 60s][Most Damage Received] Bulbasaur with 98
[METRICS last 60s][Avg Damage] 13.42 | [Legality] 100.0% legal
```

### Repo Layout
```shell
.
├─ data/
│  ├─ pokemon_attack.csv   # legal moves matrix (1 = legal)
│  ├─ pokemon_damage.csv   # damage[move][defender]
│  └─ pokemon_hp.csv       # base HP per Pokémon
├─ producers/
│  └─ kafka_producer_pokemon.py
├─ consumers/
│  └─ kafka_consumer_pokemon.py
├─ utils/
│  ├─ pokemon_data.py
│  ├─ utils_consumer.py
│  ├─ utils_producer.py
│  └─ utils_logger.py
├─ requirements.txt
└─ README.md
```

### Data files

### pokemon_attack.csv
- Columns: Pokémon names (header row).
- Rows: Move names; 1 indicates that Pokémon can use the move.

### pokemon_damage.csv
- Columns: Defender Pokémon names (header row).
- Rows: Move names; integer damage of that move against each defender.

### pokemon_hp.csv
- Row 1: Pokémon names.
- Row 2: Integer HP values.

The loader normalizes a few known typos (e.g., "Jigglyfuff" → "Jigglypuff") and validates consistency across files.

---

### Battle rules

    Producer picks a trainer and opponent at random.
    Trainer’s move is chosen only from legal moves for that Pokémon (from pokemon_attack.csv).
    Damage is looked up from pokemon_damage.csv for (move, opponent).
    The consumer tracks opponent HP (from pokemon_hp.csv) and decrements on each event.
    When opponent HP ≤ 0, the consumer logs a KO and win message and the producer starts a new battle.

### Consumer emits metrics every 15 seconds for the rolling last 60 seconds:

- Total KOs
- Most damage given (Pokémon)
- Most damage received (Pokémon)
- Rolling average damage
- Legality rate (should be 100%)

# *** Important instructions on setting up this project ***

Task 1. Install & Start Kafka (using WSL if Windows)

Kafka is Linux-native and large. We’ll run it in WSL (Ubuntu).

Prereqs

Windows 10/11 with WSL2 (Ubuntu) installed
Java 17+ in WSL (`java -version`)
Python 3.10+ on Windows

Do these in a WSL terminal (Ubuntu).

1) Start (or initialize) Kafka KRaft
```shell
cd ~/kafka
# If FIRST RUN ONLY, format storage (keep the UUID you get from random-uuid)
# uuid=$(bin/kafka-storage.sh random-uuid)
# bin/kafka-storage.sh format -t $uuid -c config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties
```

2) Verify ports
```shell
ss -ltnp | egrep ':9092|:9093'
```

You should see 9092 (broker) and 9093 (controller).
If you only see 9093, fix your `config/kraft/server.properties` so that:

```shell
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
```

Restart Kafka after changes.

3) Create the topic (reliable, do this in WSL)
```shell
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
  --create --topic pokemon_battles --partitions 1 --replication-factor 1

bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
# Expect: pokemon_battles
```
---

Task 2. Manage Local Project Virtual Environment

Open the project in VS Code and set up a venv.

Windows (PowerShell)
```shell
cd C:\Projects\buzzline-02-data-git-hub
py -3.11 -m venv .venv
. .\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get an execution policy error:

```shell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Mac / Linux
```bash
cd ~/path/to/buzzline-02-data-git-hub
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

Task 3. Configure Environment Variables
You can set these per session in PowerShell, or place them in a `.env` file at the project root.

PowerShell (recommended for clarity)
```shell
$env:KAFKA_BROKER_ADDRESS="127.0.0.1:9092"
$env:KAFKA_TOPIC="pokemon_battles"
$env:KAFKA_API_VERSION="3.5.0"      # avoids client/broker probing issues
$env:MESSAGE_INTERVAL_SECONDS="1"
$env:LOG_ROLE="producer"            # set 'consumer' in consumer terminal
```
Optional .env file
```shell
KAFKA_BROKER_ADDRESS=127.0.0.1:9092
KAFKA_TOPIC=pokemon_battles
KAFKA_API_VERSION=3.5.0
MESSAGE_INTERVAL_SECONDS=1
LOG_ROLE=app
```

---

Task 4. Start the Kafka Consumer (start this first)
Open a new PowerShell tab in your project root.

```shell
cd C:\Projects\buzzline-02-data-git-hub
. .\.venv\Scripts\Activate.ps1

# (if you did not set envs in this terminal)
$env:KAFKA_BROKER_ADDRESS="127.0.0.1:9092"   # or "$ip:9092"
$env:KAFKA_TOPIC="pokemon_battles"
$env:KAFKA_API_VERSION="3.5.0"
$env:LOG_ROLE="consumer"

py -m consumers.kafka_consumer_pokemon
```

You should see something like:
```shell
[CONSUMER] Kafka topic: pokemon_battles
[CONSUMER] Group ID: pokemon_group
[CONSUMER] Polling topic 'pokemon_battles'...
```

---

Task 5. Start the Kafka Producer
Open another PowerShell tab in your project root.

```shell
cd C:\Projects\buzzline-02-data-git-hub
. .\.venv\Scripts\Activate.ps1

# (if you did not set envs in this terminal)
$env:KAFKA_BROKER_ADDRESS="127.0.0.1:9092"   # or "$ip:9092"
$env:KAFKA_TOPIC="pokemon_battles"
$env:KAFKA_API_VERSION="3.5.0"
$env:MESSAGE_INTERVAL_SECONDS="1"
$env:LOG_ROLE="producer"

py -m producers.kafka_producer_pokemon
```

---

What You Should See
Immediate per-event logs (from the consumer)
```shell
Snorlax uses Shadow Ball that does 12 damage to Charmander. Charmander has 88 HP left!
Bulbasaur uses Vine Whip that does 18 damage to Squirtle. Squirtle has 0 HP left!
Bulbasaur knocks out Squirtle and wins the battle (f97f5ba8)!
```

Alerts if something ever goes wrong (should be rare)
```shell
[ALERT][ILLEGAL MOVE] Trainer=Pikachu used move='Water Gun' (not legal). Event={...}
```

Rolling analytics (every 15s, for the last 60s)
```shell
[METRICS last 60s][KOs] total=3
[METRICS last 60s][Most Damage Given] Gengar with 124
[METRICS last 60s][Most Damage Received] Bulbasaur with 98
[METRICS last 60s][Avg Damage] 13.42 | [Legality] 100.0% legal
```

---

Troubleshooting
Kafka in WSL shows only 9093 listening

Edit WSL file `~/kafka/config/kraft/server.properties`:
```shell
Troubleshooting
Kafka in WSL shows only 9093 listening

Edit WSL file ~/kafka/config/kraft/server.properties:
```

Restart Kafka:
```shell
# WSL
cd ~/kafka
# If already running, stop it (Ctrl+C in that terminal)
bin/kafka-server-start.sh config/kraft/server.properties
```

Verify:
```shell
ss -ltnp | egrep ':9092|:9093'
```

Windows can’t reach WSL Kafka on 127.0.0.1
Use the WSL IP from Windows:
```shell
$ip = (wsl.exe hostname -I).Split()[0]
Test-NetConnection -ComputerName $ip -Port 9092
$env:KAFKA_BROKER_ADDRESS="$ip:9092"
```

`UnrecognizedBrokerVersion` or `metadata timeout`
Pin the client:

```shell
$env:KAFKA_API_VERSION="3.5.0"
```

Create topic from WSL:
```shell
bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic pokemon_battles --partitions 1 --replication-factor 1
```

Log rotation `PermissionError` on Windows

If you see retention/rotation errors in `logs/`:
- Close editors/tailers that might lock files.
- Re-run PowerShell as Administrator if necessary.
- Temporarily disable retention in your logger or manually delete old `logs\*.log` while apps are closed.

---


### Later Work Sessions

When resuming work on this project:

1. Open the folder in VS Code.
2. Start the Kafka service.
3. Activate your local project virtual environment (.venv).

### Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

### License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.