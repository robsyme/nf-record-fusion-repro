nextflow.enable.types = true

// Minimal reproducer for: a record `Path` field is not reliably staged into a
// downstream task on a Fusion (S3) executor. The field is sometimes interpolated as
// a raw object-store path (/bucket/key) instead of staged / Fusion-mapped
// (/fusion/s3/bucket/key), so the task fails with "no such file or directory".
//
// Mirrors the real failing pipeline structure:
//   - a 2-Path-field record (like ReadPair {r1,r2})
//   - the record is consumed by TWO processes (like READ_STRUCTURE + BARCODE_CHECK)
//   - one consumer mixes the record with a separate Path input (like a whitelist)
// getClass() (evaluated at script-render time) reports TaskPath (staged) vs a raw
// S3Path. Parameterized ids so a resume can ADD samples (the scenario that failed).

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

// Consumer A: record only (like READ_STRUCTURE).
process CONSUME_A {
    tag "${rec.id}"
    container 'ubuntu:24.04'
    input:
    rec: Pair
    output:
    out: Path = file("${rec.id}.a.out")
    script:
    """
    echo "CONSUME_A ${rec.id}: r1class=${rec.r1.getClass()} r1=${rec.r1} r2class=${rec.r2.getClass()} r2=${rec.r2}"
    cat ${rec.r1} ${rec.r2} > ${rec.id}.a.out
    """
}

// Consumer B: record + a separate Path input (like BARCODE_CHECK + whitelist).
process CONSUME_B {
    tag "${rec.id}"
    container 'ubuntu:24.04'
    input:
    rec: Pair
    asset: Path
    output:
    out: Path = file("${rec.id}.b.out")
    script:
    """
    echo "CONSUME_B ${rec.id}: r1class=${rec.r1.getClass()} r1=${rec.r1} assetclass=${asset.getClass()} asset=${asset}"
    cat ${rec.r1} ${rec.r2} ${asset} > ${rec.id}.b.out
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
