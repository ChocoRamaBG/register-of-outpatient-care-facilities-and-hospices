# Pending hospital names after source updates

The authoritative dictionary is
`unconnected_datasets/standartized_medical_centres_and_types.csv` in the registry
repository. Every newly observed hospital/workplace spelling is appended as
`Raw Name,,`. The user supplies **Final Name** and **Final Type** manually. The
append step never fills those decisions, renames an existing row, deletes a name,
merges a hospital, changes doctor links, or rebuilds the website.

Existing bytes, including the BOM, headers, quoting, duplicate rows, row order,
approved decisions and pending decisions, are preserved. Repeated checks with no
new spelling produce no file change and no commit. Case, punctuation and internal
spaces remain separate spellings, so a previous manual decision is never guessed
for a new variant. Sources are read through explicit hospital/workplace columns
and structured workplace fields; doctor names and descriptions are excluded.

## Repository installation

Copy these files into the registry repository:

| Local source | Repository destination |
| --- | --- |
| `facility_dictionary_sync.py` | `.github/scripts/facility-dictionary/facility_dictionary_sync.py` |
| `sources.json` | `.github/scripts/facility-dictionary/sources.json` |
| `tools/maintenance/collect_facility_dictionary_names.py` from the master project | `.github/scripts/facility-dictionary/collect_facility_dictionary_names.py` |
| `facility-dictionary.yml` | `.github/workflows/facility-dictionary.yml` |

The hosted workflow runs after output-file pushes, after the named trusted scraper
workflows finish, and on manual **Run workflow**. Scraper completion also covers
partial commits from a scraper that later fails; commits made with `GITHUB_TOKEN`
otherwise do not start push workflows. No scheduled checks are installed. A new
scraper using `GITHUB_TOKEN` needs its workflow name added to `workflow_run`.

The workflow scans trusted current `main`, stages only the dictionary, and retries
a concurrent-main conflict up to three times by collecting against the latest
dictionary. Write access is limited to that job. It runs on a GitHub-hosted runner,
so dictionary updates do not depend on the website computer being online. Its
dictionary-only bot commit does not launch a website rebuild.

## Input inventory and new sources

`sources.json` is generated from the enabled GitHub `copy_rules` in the master
project's `config/sources.json`. It includes the current CSV and Excel outputs,
with explicit hospital-directory and hospital-sheet flags. Regenerate it when a
configured source changes:

```powershell
python integrations/facility-dictionary/facility_dictionary_sync.py --export-config config/sources.json --manifest integrations/facility-dictionary/sources.json
```

`discover_outputs` additionally finds future CSV/XLSX/XLSM files under directories
ending `_outputs`. These are scanned only for explicit hospital/workplace fields.
A generic `Name` or `Име` column is never treated as a hospital in an unknown
source. Add `facility_table: true` for a verified hospital CSV, or name its
`facility_sheets` for Excel. General company-registry outputs with no hospital
fields are skipped after inspecting their header; their company names are not
invented into hospital names. Required manifest files or Excel sheets that
disappear cause a visible error instead of silently losing source coverage.

## Shared local API

- `append_names(reference, names, expected_sha256=None, dry_run=False)` validates
  and appends; `ReferenceChangedError` means retry against the current manual file.
- `collect_manifest_names(root, manifest, cache_dir=None, allow_external=False)`
  returns `names`, input metrics, `cacheHits` and `scannedFiles`. Local callers may
  opt into absolute source paths. CSV inputs can set `encoding: "auto"` for legacy
  cp1251 and an explicit `delimiter`; Excel inputs set `sheets`/`facility_sheets`.
- `sync_manifest(root, manifest_path, reference=None, cache_dir=None, dry_run=False)`
  combines collection and append with an optimistic dictionary hash guard.

The optional cache stores names only, keyed by input bytes, input schema and the
collector's code hash. Changed sources are reparsed; unchanged sources reuse the
names. Pending names must still be compared to the latest dictionary every run,
so a manual correction never gets overwritten by a stale reference cache.

The local source-refresh coordinator maintains its own durable pending-name
outbox for local-only sources and upload retries. This hosted workflow reads no
local credentials, browser sessions, medical databases or website password.
