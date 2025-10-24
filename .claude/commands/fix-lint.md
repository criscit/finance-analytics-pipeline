---
description: Auto-fix linting and formatting issues
---

Automatically fix code quality issues:

```bash
poetry run ruff check --fix . && poetry run black .
```

After running:
- Summarize what was fixed
- Run type checking: `poetry run mypy src/`
- Report any remaining issues that need manual fixes
