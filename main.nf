nextflow.enable.types = true

// Minimal reproducer: a record `Path` field is not reliably staged into a
// downstream task on a Fusion (S3) executor. The field is sometimes interpolated
// as a raw object-store path (/bucket/key) instead of being staged / Fusion-mapped
// (/fusion/s3/bucket/key), so the task fails with "no such file or directory".
//
// Mirrors the real failing case: a 2-Path-field record (like ReadPair {r1,r2}).
// A/B: VIA_RECORD consumes via record fields (rec.r1/rec.r2); VIA_PATH consumes the
// SAME files via direct Path inputs. getClass() is evaluated at script-render time,
// so it reveals whether each path was staged (TaskPath) or left raw (S3Path).
// Fanned out over many samples to surface the intermittency.

record Pair {
    id: String
    r1: Path
    r2: Path
}

process MAKE {
    tag "${id}"
    container 'ubuntu:24.04'

    input:
    id: String

    output:
    rec: Pair = record(id: id, r1: file("${id}_1.txt"), r2: file("${id}_2.txt"))

    script:
    """
    echo "${id} mate 1" > ${id}_1.txt
    echo "${id} mate 2" > ${id}_2.txt
    """
}

// Consume both files via RECORD FIELDS (rec.r1 / rec.r2).
process VIA_RECORD {
    tag "${rec.id}"
    container 'ubuntu:24.04'

    input:
    rec: Pair

    output:
    out: Path = file("${rec.id}.via_record.out")

    script:
    """
    echo "VIA_RECORD ${rec.id}: r1class=${rec.r1.getClass()} r1=${rec.r1} r2class=${rec.r2.getClass()} r2=${rec.r2}"
    cat ${rec.r1} ${rec.r2} > ${rec.id}.via_record.out
    """
}

// Consume the SAME files via DIRECT Path inputs (control).
process VIA_PATH {
    tag "${r1.baseName}"
    container 'ubuntu:24.04'

    input:
    r1: Path
    r2: Path

    output:
    out: Path = file("${r1.baseName}.via_path.out")

    script:
    """
    echo "VIA_PATH ${r1.baseName}: r1class=${r1.getClass()} r1=${r1} r2class=${r2.getClass()} r2=${r2}"
    cat ${r1} ${r2} > ${r1.baseName}.via_path.out
    """
}

params {
    // Sample ids. Parameterized so a resume can ADD new ids: old ones cache, new
    // ones run fresh -- the scenario under which the staging miss was observed.
    ids: List<String> = ['s01','s02','s03','s04','s05']
}

workflow {
    main:
    made = MAKE(channel.fromList(params.ids))

    VIA_RECORD(made)
    VIA_PATH(made.map { r -> r.r1 }, made.map { r -> r.r2 })
}
