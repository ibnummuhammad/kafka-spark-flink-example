package org.davidcampos.kafka.consumer;

public class JavaRecord implements java.io.Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}