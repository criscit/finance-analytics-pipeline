# Auto-Approval Configuration for Claude Code

Auto-approvals let Claude Code automatically execute certain file operations and commands without asking for permission each time.

## Where to Configure

Auto-approvals are configured in your **VSCode User Settings**, not in project settings.

### Access User Settings

**Option 1: Settings UI**
1. Press `Ctrl+,` (or `Cmd+,` on Mac)
2. Search for "Claude Code"
3. Configure auto-approval patterns

**Option 2: Settings JSON**
1. Press `Ctrl+Shift+P` (or `Cmd+Shift+P`)
2. Type "Preferences: Open User Settings (JSON)"
3. Add Claude Code configuration

## Recommended Settings for This Project

Add to your User Settings JSON:

```json
{
  "claudeCode.autoApprove.read": [
    "**/*.py",
    "**/*.sql",
    "**/*.yaml",
    "**/*.yml",
    "**/*.md",
    "**/*.json",
    "**/*.toml",
    "pyproject.toml",
    "docker-compose.yml",
    "env.example"
  ],
  "claudeCode.autoApprove.write": [
    "**/*.py",
    "**/*.sql",
    "**/*.yaml",
    "tests/**/*"
  ],
  "claudeCode.autoApprove.bash": [
    "poetry run pytest*",
    "poetry run ruff*",
    "poetry run black*",
    "git status",
    "git diff*",
    "docker compose ps",
    "docker compose logs*",
    "ls*",
    "cat*"
  ]
}
```

## Safety Guidelines

### DO Auto-Approve
- Reading source code files
- Running tests and linters
- Viewing git status/logs
- Checking Docker status
- Reading config files

### DON'T Auto-Approve
- `.env` files (contain secrets)
- `data/credentials/**` (sensitive data)
- `rm -rf` commands (destructive)
- `git push` (remote changes)
- `docker compose up/down` (service management)
- Database files (data loss risk)

## Per-Session Approval

You can also approve actions just for the current chat:
- Click "Approve" when prompted
- Select "Always for this session"

This is useful for temporary operations without changing global settings.
