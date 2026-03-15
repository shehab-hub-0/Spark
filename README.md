# PySpark 3.5.0 — VS Code Dev Container

Production-grade, single-node PySpark development environment.
No Jupyter. No extra services. Pure Spark/PySpark in VS Code.

---

## Stack

| Component       | Version / Detail                        |
|-----------------|-----------------------------------------|
| Spark           | 3.5.0 (apache/spark:3.5.0 base image)  |
| PySpark         | 3.5.0 (pinned — matches Spark exactly) |
| Python          | 3.11 (system)                           |
| Java            | OpenJDK 17                             |
| Mode            | Local (`local[*]`) — single node       |
| Resources       | 8 GB driver RAM, 6 cores               |
| IDE             | VS Code Dev Containers                 |

---

## Prerequisites

Install **all three** before continuing:

1. **Docker Desktop** ≥ 4.x — https://www.docker.com/products/docker-desktop
   - Settings → Resources → set Memory ≥ 10 GB, CPUs ≥ 6
2. **VS Code** — https://code.visualstudio.com
3. **Dev Containers extension** — Install from VS Code marketplace:
   `ms-vscode-remote.remote-containers`

---

## Directory Structure

```
pyspark-devcontainer/          ← your project root
├── .devcontainer/
│   └── devcontainer.json      ← Dev Container configuration
├── .vscode/
│   ├── launch.json            ← F5 run/debug configurations
│   └── settings.json          ← Workspace settings
├── conf/
│   ├── spark-defaults.conf    ← Spark tuning (12 GB / 6 CPU)
│   └── log4j2.properties      ← Clean logging config
├── src/
│   └── main.py                ← Verification script (run this first)
├── tests/
│   └── test_spark_basics.py   ← Example pytest tests
├── Dockerfile                 ← Container image definition
├── pytest.ini                 ← pytest configuration
└── .gitignore
```

---

## Launch Instructions

### Step 1 — Open the project in VS Code

```bash
# Clone or copy this folder, then:
code /path/to/pyspark-devcontainer
```

### Step 2 — Reopen in Container

VS Code will detect `.devcontainer/devcontainer.json` and show a popup:

> **"Folder contains a Dev Container configuration file. Reopen in Container?"**

Click **"Reopen in Container"**.

If the popup doesn't appear, open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`):
```
Dev Containers: Reopen in Container
```

### Step 3 — Wait for the build

The **first build** downloads `apache/spark:3.5.0` (~1.2 GB) and installs all
Python packages. This takes **3–8 minutes** depending on your internet speed.

Progress is visible in VS Code's Output panel → "Dev Containers".

Subsequent opens reuse the cached image and take **< 10 seconds**.

### Step 4 — Verify the environment

Once VS Code is connected to the container, open a terminal
(`Ctrl+` `` ` ``) and run:

```bash
python src/main.py
```

You should see all checks pass with ✅:

```
── 1. Python & System Environment ─────────────────────────
  ✅ Python version — Python 3.11.x
  ✅ Running inside container — Docker container detected
  ✅ SPARK_HOME set — /opt/spark
  ...
── 2. SparkSession Initialization ──────────────────────────
  ✅ SparkSession created — 12.4s
  ✅ Spark version — 3.5.0
  ✅ Master URL — local[*]
  ...
── Summary ──────────────────────────────────────────────────
  ✅ 25/25 checks passed
```

### Step 5 — (Optional) Open Spark UI

After running `main.py`, the Spark History UI is available at:
```
http://localhost:4040
```
VS Code auto-forwards port 4040. Click the notification or open it manually.

---

## Running & Debugging

### Run with F5
Open `src/main.py` → press **F5** → select **"▶ Run main.py (PySpark)"**.

### Run in terminal
```bash
python src/main.py
```

### Run tests
```bash
pytest tests/ -v
```

### Interactive PySpark shell
```bash
pyspark --master local[*]
```

---

## Spark Configuration Explained

The `conf/spark-defaults.conf` is tuned for your 12 GB / 6 CPU machine:

```
Total Docker RAM:           12 GB
  OS + Docker overhead:    - 2 GB  (reserved)
  Available to Spark:      10 GB
    spark.driver.memory:    8 GB   (JVM heap)
    JVM off-heap (20%):   + 1.6 GB (overhead factor)
    Total JVM footprint:  ≈ 9.6 GB ✓ (under 10 GB)
    Safety buffer:         ~0.4 GB

spark.driver.cores:         6      (all cores → driver in local mode)
spark.sql.shuffle.partitions: 12   (2× cores — avoids over-partitioning)
spark.sql.adaptive.enabled: true   (Spark 3.x AQE for auto-optimization)
```

**Why 8g and not 10g?**
The JVM always consumes more memory than `spark.driver.memory` alone.
The overhead factor (20%) adds ~1.6 GB on top. Setting the heap too high
causes the container to exceed Docker's memory limit → OOM kill → crash.
8g is the mathematically safe maximum for a 12 GB Docker budget.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Container exits immediately | Check Docker memory limit — must be ≥ 10 GB |
| `JAVA_HOME` not found | Verify base image is `apache/spark:3.5.0` (has JDK 17) |
| `pyspark` not found | Run `pip install pyspark==3.5.0` inside container |
| Port 4040 not opening | Spark UI only starts after a SparkSession is created |
| OOM / container killed | Reduce `spark.driver.memory` to `6g` if Docker has < 10 GB |
| Permission denied on `/workspace` | Run `sudo chown -R sparkdev:1000 /workspace` |
| Build fails on pip install | Check internet connectivity; retry the build |

---

## Starting Fresh

To rebuild the container from scratch (e.g., after changing `Dockerfile`):

**Command Palette** → `Dev Containers: Rebuild Container`

Or with Docker CLI:
```bash
docker ps -a | grep pyspark
docker rm -f <container_id>
# Then reopen VS Code in container
```
