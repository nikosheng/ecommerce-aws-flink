package flink.aws.generator.util;

import org.kohsuke.args4j.Option;

public class Args {
    @Option(name = "-file", required = true, usage = "input file name")
    private String file;
    @Option(name = "-topic", required = true, usage = "kafka topic")
    private String topic;
    @Option(name = "-broker", required = true, usage = "kafka broker list")
    private String broker;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }
}
