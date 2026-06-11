# nf-record-fusion-repro

Minimal, deterministic reproducer for a Nextflow bug: with
**`nextflow.enable.moduleBinaries = true`** on a **Fusion (S3)** executor, a typed
process consuming a **record field of type `Path`** does **not** stage that input.
The field stays a raw `S3Path` and is interpolated into `.command.sh` without the
`/fusion/s3` mount prefix, so the task fails:

```
cat: /my-bucket/.../s02_1.txt: No such file or directory
```

A **direct `Path` input** for the same file stages normally and works. Removing the
`nextflow.enable.moduleBinaries` flag makes every consumer pass.

## Evidence (getClass at script-render time)

```
CONSUME_A s02: r1class=class nextflow.cloud.aws.nio.S3Path  r1=/bucket/.../s02_1.txt  <- record field: UNSTAGED -> fails
VIA_PATH  s02: r1class=class nextflow.processor.TaskPath     r1=s02_1.txt              <- direct Path: staged -> works
```

## Pipeline

- `MAKE` writes two files and emits `record(id, r1: Path, r2: Path)`.
- `CONSUME_A` reads the files via the **record fields** `rec.r1` / `rec.r2`.
- `VIA_PATH` reads the **same** files via **direct `Path`** inputs (control).

## Trigger / scope

- Trigger: `nextflow.enable.moduleBinaries = true` (the flag alone; no module-binary
  process need be present or used).
- Only **record-field `Path`** inputs are affected; **direct `Path`** inputs are fine.
- Requires a remote/Fusion path provider — it does **not** reproduce on a local
  filesystem (local record fields stage as `TaskPath`).
- Observed on Nextflow 26.04.x.

## Mechanism (located)

- `TaskInputResolver.normalizePath` replaces a `Path` with a staged `TaskPath` only
  if `holders.containsKey(value)`, else returns the raw `Path`. With the flag on,
  the record field is not present in `holders`, so the raw `S3Path` survives.
- A raw `S3Path.toString()` is `/bucket/key`; it never passes through
  `FusionHelper.toContainerMount` (`/fusion/$scheme/$bucket$path`), so it is not
  mounted in the task container.

## Run

```bash
export NXF_SYNTAX_PARSER=v2
nextflow run robsyme/nf-record-fusion-repro -profile <fusion-batch-profile>
```

`CONSUME_A` tasks fail ("No such file"); `VIA_PATH` tasks succeed. Inspect a failed
`CONSUME_A` task's `.command.sh`: the `cat` argument is a bare `/bucket/...` path
with no `/fusion/s3` prefix.
