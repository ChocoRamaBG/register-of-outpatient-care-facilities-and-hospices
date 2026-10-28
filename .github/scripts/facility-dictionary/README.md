# Pending hospital names after source updates

The authoritative dictionary is
`unconnected_datasets/standartized_medical_centres_and_types.csv` in the registry
repository. Every newly observed hospital/workplace name is appended as
`Raw Name,,`. The user supplies **Final Name** and **Final Type** manually. The
update step never invents those decisions, merges a hospital, changes doctor
links, or rebuilds the website.

Raw names use the same exact key as the website: Unicode NFKC, case folding and
collapsed whitespace. These typographical variants share one review task. No
punctuation, legal-form, number, branch or fuzzy-name matching is used to remove
rows. Before appending, compatible duplicates consolidate into their earliest
row, filling missing B/C cells only from a unique already-recorded human value.
Completed duplicate decisions therefore cannot create another blank task.

Conflicting nonblank names or types keep all original rows and values for
explicit review. The report lists their original logical rows and physical CSV
lines. Unaffected row bytes, BOM, headers, quoting and relative row order stay
intact. Removing redundant rows can change later row numbers; consumers always
resolve row references from the current CSV. Repeating an already-clean update
with no new names changes no bytes and creates no commit. The same reconciliation
runs even when no names were added. Sources are read through explicit facility
columns and structured workplace fields; doctor names/descriptions are excluded.

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
  exact-key duplicates and appends missing names; `ReferenceChangedError` means
  retry against the current manual file. `changed` covers cleanup and additions;
  `addedRows` alone must not decide whether publication is needed.
- `cleanup_reference_bytes(raw_bytes)` returns `(proposed_bytes, report)` without
  writing. The report includes `removedDuplicateRows`, `filledNameCells`,
  `filledTypeCells`, `conflictingGroups` and complete conflict evidence.
- `dictionary_rows(raw_bytes)` returns one logical task per exact raw key, with
  reconciled `name`/`type`, `conflictFields`, all original `members`, logical
  `rows` and `physicalLines`. Conflicted fields have no selected value.
- `raw_name_key(value)` is the shared Unicode/case/whitespace key. It must stay
  aligned with `doctors_master.facility_names.exact_key` in the website pipeline.
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
