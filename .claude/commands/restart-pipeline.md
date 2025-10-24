---
description: Rebuild and restart the Docker pipeline
---

Rebuild and restart the Dagster pipeline with latest code changes:

```bash
docker compose down && docker compose up --build -d
```

Then show startup logs:

```bash
docker compose logs worker --tail=30 -f
```

Monitor for successful startup and report any errors.
