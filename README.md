# nf-record-fusion-repro

Minimal reproducer for a Nextflow bug: a **record `Path` field is not reliably
staged into a downstream task on a Fusion (S3) executor**.

## Confirmed trigger (deterministic)

The bug is triggered by **`nextflow.enable.moduleBinaries = true`** on a Fusion (S3)
executor. With the flag enabled, a **record-field `Path` input is not staged**: it
stays a raw `S3Path` and is interpolated into `.command.sh` without the `/fusion/s3`
mount prefix, so the task fails with "No such file". A **direct `Path` input** is
staged normally (`TaskPath`) and works. Observed in a single fresh run via the
`getClass()` probe:

```
CONSUME_A s01: r1class=class nextflow.cloud.aws.nio.S3Path  r1=/bucket/.../s01_1.txt   <- record field, UNSTAGED -> fails
VIA_PATH  s01: r1class=class nextflow.processor.TaskPath     r1=s01_1.txt               <- direct Path, staged -> works
```

Remove `nextflow.enable.moduleBinaries = true` and every consumer passes. (Nextflow
26.04.x.)

## Symptom

A typed process consumes a `Path` carried in a record field (`${rec.f}`). On a
Fusion-backed object-store executor, the field is intermittently interpolated into
`.command.sh` as a **raw object-store path** (`/bucket/key`) instead of being staged
and Fusion-mapped (`/fusion/s3/bucket/key`). The task then fails:

```
cat: /my-bucket/.../s07.txt: No such file or directory
```

Passing the **same file as a direct `Path` input** (not via a record field) always
works — so this isolates the defect to record-field file staging.

## Pipeline

- `MAKE` (×10) writes a file and emits `record(id, f: file(...))`.
- `VIA_RECORD` reads the file through the record field `rec.f`.
- `VIA_PATH` reads the **same** file through a direct `Path` input (control).

Fanned out over 10 samples to surface the intermittency. If buggy, some
`VIA_RECORD` tasks fail with "no such file" while **all** `VIA_PATH` tasks succeed.

## Mechanism (located in nextflow 26.04.x)

- `TaskInputResolver.normalizePath` replaces a `Path` with a staged `TaskPath` only
  if `holders.containsKey(value)`, else returns the raw `Path`.
- For a record field, the match into `holders` (built from the auto-generated
  `stageAs` file inputs in `ProcessDslV2`) intermittently misses, so the raw S3
  `Path` survives.
- A raw S3 `Path.toString()` is `/bucket/key`; it never passes through
  `FusionHelper.toContainerMount` (`/fusion/$scheme/$bucket$path`), so it is not
  mounted in the task container.

## Run

Requires Nextflow ≥ 26.04, the v2 parser, and a Fusion-enabled executor (e.g. AWS
Batch + Fusion + Wave):

```bash
export NXF_SYNTAX_PARSER=v2
nextflow run robsyme/nf-record-fusion-repro -profile <fusion-batch-profile>
```

To inspect a failed `VIA_RECORD` task, look at its `.command.sh`: the `cat`
argument will be a bare `/bucket/...` path with no `/fusion/s3` prefix.
