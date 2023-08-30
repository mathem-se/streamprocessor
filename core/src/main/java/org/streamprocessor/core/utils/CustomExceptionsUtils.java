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

    public static class UnknownProviderException extends Exception {
        public UnknownProviderException(String errorMessage) {
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

    public static class InactiveDataContractException extends Exception {

        public InactiveDataContractException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class InvalidEntityException extends Exception {

        public InvalidEntityException(String errorMessage) {
            super(errorMessage);
        }
    }
}
