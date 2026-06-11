nextflow.enable.types = true

// Minimal reproducer: a record `Path` field is not reliably staged into a
// downstream task on a Fusion (S3) executor. The field is sometimes interpolated
// as a raw object-store path (/bucket/key) instead of being staged / Fusion-mapped
// (/fusion/s3/bucket/key), so the task fails with "no such file or directory".
//
// A/B: VIA_RECORD consumes the file through a record field (rec.f); VIA_PATH
// consumes the SAME file through a direct Path input. Fanned out over many samples
// to surface the intermittency. Expectation if buggy: some VIA_RECORD tasks fail
// while all VIA_PATH tasks succeed.

record Sample {
    id: String
    f: Path
}

process MAKE {
    tag "${id}"
    container 'ubuntu:24.04'

    input:
    id: String

    output:
    rec: Sample = record(id: id, f: file("${id}.txt"))

    script:
    """
    echo "payload for ${id}" > ${id}.txt
    """
}

// Consume the file via the RECORD FIELD (rec.f).
process VIA_RECORD {
    tag "${rec.id}"
    container 'ubuntu:24.04'

    input:
    rec: Sample

    output:
    out: Path = file("${rec.id}.via_record.out")

    script:
    // getClass() is evaluated at script-render time (head job), so it reports
    // whether the record field was staged (TaskPath) or left raw (S3Path/UnixPath).
    """
    echo "VIA_RECORD ${rec.id}: class=${rec.f.getClass()} path=${rec.f}"
    cat ${rec.f} > ${rec.id}.via_record.out
    """
}

// Consume the SAME file via a DIRECT Path input (control).
process VIA_PATH {
    tag "${f.baseName}"
    container 'ubuntu:24.04'

    input:
    f: Path

    output:
    out: Path = file("${f.baseName}.via_path.out")

    script:
    """
    echo "VIA_PATH ${f.baseName}: class=${f.getClass()} path=${f}"
    cat ${f} > ${f.baseName}.via_path.out
    """
}

workflow {
    main:
    ids  = channel.of('s01','s02','s03','s04','s05','s06','s07','s08','s09','s10')
    made = MAKE(ids)

    VIA_RECORD(made)
    VIA_PATH(made.map { r -> r.f })
}
