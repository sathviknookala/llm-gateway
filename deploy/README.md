# Deploying to a single VM

The whole system runs as one Docker Compose project on one machine: Postgres, Redis, Flyway (once per start), the gateway, workers, the sweeper, the mock provider, Prometheus, Grafana, and Caddy for TLS. Only Caddy is reachable from outside (ports 80 and 443). Upstream is the mock provider, so every number from this deployment is **test traffic**.

## One-time setup

1. A VM with Docker Engine and the Compose plugin (v2.24 or later, for `!reset`). 2 vCPU and 4 GB RAM is enough for the stated load test.
2. A DNS A record for your domain pointing at the VM.
3. Firewall: allow 22, 80 and 443 only. Postgres, Redis, Prometheus and Grafana are never published publicly. (The overlay removes their port mappings; Prometheus and Grafana bind to 127.0.0.1.)
4. Clone the repository on the VM and create `.env` next to `docker-compose.yml`:

   ```bash
   git clone https://github.com/<you>/llm-gateway.git && cd llm-gateway
   cat > .env <<'EOF'
   LLMGW_DOMAIN=gateway.example.com
   LLMGW_PG_PASSWORD=<long random alphanumeric>
   LLMGW_GRAFANA_PASSWORD=<another one>
   EOF
   chmod 600 .env
   ```

   Set `LLMGW_PG_PASSWORD` before the first `up`: Postgres reads it only when the data volume is created. Keep it alphanumeric, because it is also embedded in a connection URL.
5. First start, then create an org and teams:

   ```bash
   docker compose -f docker-compose.yml -f deploy/docker-compose.deploy.yml --profile stack up -d --build
   docker compose -f docker-compose.yml -f deploy/docker-compose.deploy.yml --profile stack \
     exec gateway llmgw seed --org demo --teams 3 --fund 100000000 --grant 10000000
   ```

## Deploying a new version

From your machine, once `main` is pushed:

```bash
deploy/deploy.sh user@vm
```

It pulls `main` on the VM, rebuilds the image, restarts what changed (Flyway runs before the app services start) and waits for `https://$LLMGW_DOMAIN/readyz`. A gateway being replaced gets 20 s to finish streams, then closes their holds before it exits.

## Watching it

- Grafana and Prometheus: `ssh -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 user@vm`, then open `http://localhost:3000` (dashboard `llmgw`) and `http://localhost:9090/alerts`.
- Logs: `docker compose ... logs -f gateway worker sweeper` (one JSON object per line).
- Ledger: `docker compose ... exec gateway llmgw check-ledger` (exit 0 = consistent). The sweeper also runs it every 5 minutes and exports `llmgw_ledger_violations`.
- **Uptime:** point an external HTTP monitor (any free uptime service) at `https://$LLMGW_DOMAIN/readyz` every 1–5 minutes. `/readyz` answers 503 when Postgres is down. Redis being down doesn't fail it, because rate limiting fails open. Uptime claims come from that monitor's history.

## Backups

Not automated. A nightly dump from cron on the VM is enough at this scale:

```bash
docker compose ... exec -T postgres pg_dump -U llmgw -Fc llmgw > backups/llmgw-$(date -u +%F).dump
```
