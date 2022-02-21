package testutils;

public class DummyPojo {
    public byte[] bytesField;
    public String stringField;
    public String id;
    public long counterField;

    public byte[] getBytesField() {
        return bytesField;
    }

    public void setBytesField(byte[] bytesField) {
        this.bytesField = bytesField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCounterField() {
        return counterField;
    }

    public void setCounterField(long counterField) {
        this.counterField = counterField;
    }
}
