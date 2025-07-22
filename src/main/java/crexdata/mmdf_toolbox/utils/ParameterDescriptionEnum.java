package crexdata.mmdf_toolbox.utils;

public enum ParameterDescriptionEnum {
    INPUT_TOPIC_NAME("The topic name. Topic must exist in kafka cluster connected (Text)."),
    OUTPUT_NAME("The output stream name. Used from future nodes (Text)."),
    LONGITUDE("The name of the field that contains LONGITUDE information (expects Text)."),
    LATITUDE("The name of the field that contains LATITUDE information (expects Text)."),
    TIME("The name of the field that contains TIME information (expects Text)."),
    ID("The name of the field that contains IDENTIFIER (expects Text)."),
    H3_RESOLUTION("The h3 resolution that contributes the spatial part of the record key. The H3 resolution controls the default spatial resolution for stream (expects integer 1-15)."),
    EXPIRE_MS("Number of milliseconds that the value is available in the stream (expects Integer)."),
    DT_MS("Maximum time interval in milliseconds that two values are considered relevant for processing."),
    DISTANCE_METERS("Maximum distance in meters that two values are considered relevant for processing."),
    PEEK("Boolean value that exposes stream values into the log."),
    TOPIC("Boolean value that converts internal stream into kafka topic using (using the output_name).")
    ;




    private final String label;

    // Constructor
    ParameterDescriptionEnum(String label) {
        this.label = label;
    }

    // Getter
    public String getLabel() {
        return label;
    }

    // Optional: override toString
    @Override
    public String toString() {
        return label;
    }
}
