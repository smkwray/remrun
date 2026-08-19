# Project-specific remrun hints

Use the global `remrun` command. No project wrapper script is required.

Examples:

```bash
remrun run --auto -- make test
remrun run macbox -- Rscript do/tmp/test_bootstrap.R --reps 1000
remrun run winbox -- powershell -File .\do\run_stata.ps1
```

Do not manually copy outputs from remote devices. Let remrun pull changed project files back into their normal paths.
