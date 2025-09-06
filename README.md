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

### Producer picks a trainer and opponent at random.
### Trainer’s move is chosen only from legal moves for that Pokémon (from pokemon_attack.csv).
### Damage is looked up from pokemon_damage.csv for (move, opponent).
### The consumer tracks opponent HP (from pokemon_hp.csv) and decrements on each event.
### When opponent HP ≤ 0, the consumer logs a KO and win message and the producer starts a new battle.

### Consumer emits metrics every 15 seconds for the rolling last 60 seconds:
    - Total KOs
    - Most damage given (Pokémon)
    - Most damage received (Pokémon)
    - Rolling average damage
    - Legality rate (should be 100%)

*** Important instructions on setting up this project ***


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