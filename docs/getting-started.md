# Getting Started with gomigrate

This guide provides an exhaustive walkthrough for setting up and running `gomigrate` in various environments, from local development to production-grade VPS deployments.

---

## 1. Prerequisites

Before installing `gomigrate`, ensure your environment meets the following requirements:

### Hardware Recommendations

- **CPU**: 2+ cores recommended. `gomigrate` is highly concurrent; more cores directly improve performance.
- **RAM**: Minimum 2GB. For large migrations (millions of rows), 4GB+ is recommended to handle batch buffering.
- **Disk**: Space for the binary (~30MB) and, crucially, space for **Checkpoints** (BoltDB files) and **Local Storage** if you are not using S3/GCS.

### Software Requirements

- **Go**: Version 1.26 or later.
  - Check your version: `go version`
  - [Download Go](https://golang.org/dl/)
- **Make**: Required to run the build and test automation.
- **Docker**: Optional, but highly recommended for running local instances of PostgreSQL, MySQL, or MongoDB.
- **Operating System**: Linux (any modern distro), macOS, or Windows (via WSL2 recommended).

---

## 2. Local Installation & Build

### Step 1: Clone the Repository

```bash
git clone https://github.com/dinocodesx/gomigrate.git
cd gomigrate
```

### Step 2: Build the Binary

Run the following command to compile the source code:

```bash
make build
```

This command performs several actions:

1. Downloads Go dependencies (`go mod download`).
2. Runs code generation if necessary.
3. Compiles the binary into the root folder as `gomigrate`.

### Step 3: Verify the Build

```bash
./gomigrate --version
./gomigrate --help
```

### Common Build Issues & Fixes

| Issue                                 | Cause                      | Fix                                                                       |
| :------------------------------------ | :------------------------- | :------------------------------------------------------------------------ |
| `make: command not found`             | Build tools not installed. | Install `build-essential` (Ubuntu) or `Xcode Command Line Tools` (macOS). |
| `go: command not found`               | Go is not in your PATH.    | Add `/usr/local/go/bin` and `$HOME/go/bin` to your `.bashrc` or `.zshrc`. |
| `verifying module: checksum mismatch` | Local cache is corrupted.  | Run `go clean -modcache` and try again.                                   |

---

## 3. Running on a VPS / Virtual Machine (VM)

For production tasks, running on a dedicated VM (AWS EC2, DigitalOcean Droplet, GCP Compute Engine) is standard.

### Scenario A: Building on the VM (Recommended for Linux)

1. **Provision the VM**: Choose a Debian/Ubuntu or Amazon Linux image.
2. **Install Dependencies**:
   ```bash
   sudo apt update && sudo apt install git make golang-go -y
   ```
3. **Follow the Local Installation steps** above.

### Scenario B: Transferring a Pre-built Binary

If you don't want to install Go on the VM:

1. **Build locally for the target OS** (assuming Linux):
   ```bash
   GOOS=linux GOARCH=amd64 go build -o gomigrate ./cmd/gomigrate
   ```
2. **Transfer via SCP**:
   ```bash
   scp ./gomigrate user@your-vps-ip:/home/user/
   ```
3. **Set Permissions**:
   ```bash
   ssh user@your-vps-ip "chmod +x /home/user/gomigrate"
   ```

### Firewall & Networking

**Crucial**: `gomigrate` must be able to reach your database ports (e.g., 5432 for Postgres, 3306 for MySQL, 27017 for Mongo).

- Ensure your DB's `pg_hba.conf` or equivalent allows the VPS IP.
- Open outgoing ports on your VPS security group/firewall.

---

## 4. Deployment via Docker

If you prefer containerization:

### 1. Build the Image

```bash
docker build -t gomigrate:latest .
```

### 2. Run the Container

You must mount your configuration file and a directory for checkpoints.

```bash
docker run -v $(pwd)/configs:/app/configs \
           -v $(pwd)/checkpoints:/app/checkpoints \
           --env-file .env \
           gomigrate:latest migrate --config /app/configs/prod.yaml
```

---

## 5. Configuration & Environment

### The `.env` File

`gomigrate` looks for a `.env` file in the current directory for sensitive credentials:

```env
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=yyy
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Logging Levels

Control output verbosity via the `LOG_LEVEL` environment variable:

- `debug`: Detailed logs for every record batch (high overhead).
- `info`: Standard progress updates (recommended).
- `warn`/`error`: Only report issues.

---

## 6. Troubleshooting "Getting Started"

### "Database connection refused"

1. **Ping the DB**: Ensure the host is reachable.
2. **Check DSN**: Verify the username, password, and database name.
3. **SSL Mode**: If using managed DBs (RDS, Cloud SQL), you might need `sslmode=require`.

### "Permission denied" when running the binary

- Run `chmod +x gomigrate`.
- Ensure the user running the tool has write permissions to the checkpoint directory.

### "BoltDB: timeout"

- This happens if another instance of `gomigrate` is already running and locking the checkpoint file.
- **Fix**: Ensure only one process uses a specific checkpoint file at a time.
