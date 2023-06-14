package org.streamprocessor.core.utils;

public class CustomExceptionsUtils {

    public static class MissingMetadataException extends Exception {
        public MissingMetadataException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class MalformedEventException extends Exception {
        public MalformedEventException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class UnknownPorviderException extends Exception {
        public UnknownPorviderException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class MissingIdentifierException extends Exception {
        public MissingIdentifierException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class NoSchemaException extends Exception {

        public NoSchemaException(String errorMessage) {
            super(errorMessage);
        }
    }
}
