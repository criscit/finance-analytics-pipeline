---
description: Check Docker pipeline status
---

Check the status of the Dagster pipeline running in Docker:

```bash
docker compose ps
```

Then show recent logs from the worker:

```bash
docker compose logs worker --tail=50
```

Summarize:
- Which services are running
- Any errors in recent logs
- Dagster UI availability (http://localhost:3000)
