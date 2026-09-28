// The Hello pipeline in Nextflow (after the "Hello Nextflow" training, training.nextflow.io).
// Run from this folder: nextflow run main.nf

params {
    input: Path = '../greetings.csv'
}

process sayHello {
    input:
    val greeting

    output:
    path "${greeting}-output.txt"

    script:
    """
    echo '${greeting}' > '${greeting}-output.txt'
    """
}

process convertToUpper {
    input:
    path input_file

    output:
    path "UPPER-${input_file}"

    script:
    """
    tr '[a-z]' '[A-Z]' < ${input_file} > UPPER-${input_file}
    """
}

process collectGreetings {
    input:
    path input_files

    output:
    path 'COLLECTED-output.txt', emit: collected
    path 'report.txt', emit: report

    script:
    """
    cat ${input_files} > COLLECTED-output.txt
    echo 'There were ${input_files.size()} greetings in this batch.' > report.txt
    """
}

workflow {
    main:
    greetings = channel.fromPath(params.input).splitCsv().map { row -> row[0] }
    sayHello(greetings)
    convertToUpper(sayHello.out)
    collectGreetings(convertToUpper.out.collect())

    publish:
    collected = collectGreetings.out.collected
    report = collectGreetings.out.report
}

output {
    collected {
        path '.'
    }
    report {
        path '.'
    }
}
