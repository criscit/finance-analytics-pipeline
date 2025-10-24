---
description: Run linting and formatting checks
---

Run code quality checks:

```bash
poetry run ruff check . && poetry run black --check . && poetry run mypy src/
```

After running:
- Report any linting errors found
- Suggest fixes for common issues
- If all checks pass, confirm code quality is good
