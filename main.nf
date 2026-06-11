nextflow.enable.types = true
nextflow.enable.moduleBinaries = true

include { Pair } from './types.nf'
include { CONSUME_B } from './modules/consume_b/main.nf'

// Minimal reproducer for: a record `Path` field is not reliably staged into a
// downstream task on a Fusion (S3) executor. The field is sometimes interpolated as
// a raw object-store path (/bucket/key) instead of staged / Fusion-mapped
// (/fusion/s3/bucket/key), so the task fails with "no such file or directory".
//
// Mirrors the real failing pipeline:
//   - a 2-Path-field record (like ReadPair {r1,r2})
//   - record consumed by TWO processes (like READ_STRUCTURE + BARCODE_CHECK)
//   - one consumer is a MODULE-BINARY process mixing the record + a separate Path
//     input (like BARCODE_CHECK + whitelist) -- this is where the raw path appeared
// getClass() (script-render time) reports TaskPath (staged) vs raw S3Path.
// Parameterized ids so a resume can ADD samples (the scenario that failed).

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

// Consumer A: plain process, record only (like READ_STRUCTURE).
process CONSUME_A {
    tag "${rec.id}"
    container 'ubuntu:24.04'
    input:
    rec: Pair
    output:
    out: Path = file("${rec.id}.a.out")
    script:
    """
    echo "CONSUME_A ${rec.id}: r1class=${rec.r1.getClass()} r1=${rec.r1}"
    cat ${rec.r1} ${rec.r2} > ${rec.id}.a.out
    """
}

// Control: same files via direct Path inputs.
process VIA_PATH {
    tag "${r1.baseName}"
    container 'ubuntu:24.04'
    input:
    r1: Path
    r2: Path
    output:
    out: Path = file("${r1.baseName}.path.out")
    script:
    """
    echo "VIA_PATH ${r1.baseName}: r1class=${r1.getClass()} r1=${r1}"
    cat ${r1} ${r2} > ${r1.baseName}.path.out
    """
}

params {
    ids: List<String> = ['s01','s02','s03','s04','s05']
}

workflow {
    main:
    made  = MAKE(channel.fromList(params.ids))
    asset = file("${projectDir}/assets/asset.txt")

    CONSUME_A(made)
    CONSUME_B(made, asset)
    VIA_PATH(made.map { r -> r.r1 }, made.map { r -> r.r2 })
}
