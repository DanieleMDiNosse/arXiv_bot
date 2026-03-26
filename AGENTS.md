# Repository Guidelines

## Project Structure & Module Organization
`arXiv_bot.py` is the application entrypoint and currently holds Telegram handlers, paper-source integrations, recap scheduling, and settings helpers. `bot_settings.json` is the local persistence file for user keywords, bookmarks, and recap preferences; treat it as runtime data, not documentation. `requirements.txt` contains the runtime dependencies. There is no `tests/` directory yet, so add one for any non-trivial new logic instead of growing the main script further.

## Build, Test, and Development Commands
Use the shared Python environment before running anything:

```bash
conda activate main
pip install -r requirements.txt
python -m py_compile arXiv_bot.py
export TELEGRAM_BOT_TOKEN=123456:ABC...
python arXiv_bot.py
```

`pip install -r requirements.txt` installs the Telegram bot and feed clients. `python -m py_compile arXiv_bot.py` is the fastest syntax check. Running `python arXiv_bot.py` starts long polling against the Telegram Bot API. Set optional variables such as `OPENALEX_MAILTO`, `MAX_RESULTS`, or `REPORT_FORWARD_CHAT_ID` only when your change needs them.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation, explicit helper functions, and type hints for new code. Match the existing naming style: `snake_case` for functions and variables, `UPPER_CASE` for module-level constants, and descriptive suffixes like `_cmd` and `_callback` for Telegram handlers. Keep persistence funneled through `load_settings()` and `save_settings()` rather than writing JSON ad hoc.

## Testing Guidelines
There is no formal automated suite yet. For reusable logic, add `pytest` tests under `tests/test_<feature>.py`. Before opening a PR, run `python -m py_compile arXiv_bot.py` and manually smoke-test the affected command flow with a non-production bot token. When changing fetchers or recap logic, test both normal results and empty/no-match cases.

## Commit & Pull Request Guidelines
Recent commits use short, imperative subjects such as `Fix pdf sending` and `add isee and ssrn`. Keep commit messages focused, concise, and behavior-oriented. PRs should explain the user-facing change, list any new environment variables, and mention manual verification steps. If a change touches `bot_settings.json`, explain whether the diff is intentional test data or should be excluded before merge.

## Security & Configuration Tips
Never commit real bot tokens, chat IDs, or personal runtime data. Prefer sanitized examples in docs and keep local configuration changes minimal and reviewable.
