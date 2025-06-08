package ioobject;

import com.rapidminer.operator.ResultObjectAdapter;

public class ConfigObjectIOObject extends ResultObjectAdapter {

    private static final long serialVersionUID = 1725159059797569345L;

    private String data;

    public ConfigObjectIOObject(String data) {
        this.data = data;
    }

    public String getDemoData() {
        return data;
    }

    @Override
    public String toString() {
        return "Custom data specific string";
    }
}