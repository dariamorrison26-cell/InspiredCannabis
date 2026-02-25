# Execution Scripts

Deterministic Python scripts that handle APIs, data processing, file operations, and storage.

## Rules
- Scripts must be **fast, testable, and repeatable**
- If something runs more than once, it belongs here
- Secrets and tokens live in `.env` (never hardcoded)
- All scripts should handle errors gracefully and return clear exit codes

## Naming Convention
- `action_target.py` — lowercase, underscores
- Example: `scrape_single_site.py`, `process_inventory.py`
