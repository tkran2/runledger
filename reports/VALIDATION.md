# Validation scope

All 15 Python unit/integration tests passed. These include actual concurrent
submissions/claims, stale-token fencing, a killed worker subprocess, lease
renewal during work, and real localhost HTTP requests. The separate four-worker
recovery scenario completed 40 jobs with 41 attempts and 40 committed results.

The inline browser JavaScript and optional UI smoke script passed Node syntax
checks. A browser end-to-end/visual check was **not completed**: the environment
had no Chromium binary and its browser download failed. Do not describe the
UI as browser-validated. The optional `tools/ui_smoke.cjs` starts an isolated
local app and checks submission, comparison, detail inspection and mobile
layout when Playwright/Chromium are available.

No production deployment or GitHub Actions run is claimed. See validation.json
and the recorded logs/outcome for the exact scope.
