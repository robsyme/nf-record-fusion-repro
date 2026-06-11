nextflow.enable.types = true

include { Pair } from '../../types.nf'

// Consumer B as a MODULE-BINARY process (resources/usr/bin), record + a separate
// Path input. In the real pipeline the module-binary consumer is where the raw
// (un-staged) S3 path showed up.
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
    consume_b.sh ${rec.r1} ${rec.r2} ${asset} > ${rec.id}.b.out
    """
}
