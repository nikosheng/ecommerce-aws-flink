package flink.aws.generator.util;

import org.kohsuke.args4j.Option;

public class RandomArgs {
    @Option(name = "-file", required = true, usage = "input file name")
    private String file;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }
}
