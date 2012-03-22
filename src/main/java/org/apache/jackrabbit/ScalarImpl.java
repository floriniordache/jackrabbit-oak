package org.apache.jackrabbit;

import org.apache.jackrabbit.oak.api.Scalar;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Callable;

public abstract class ScalarImpl implements Scalar {
    private final int type;

    public static Scalar createNumber(String value) {
        // todo improve
        try {
            return createLong(Long.parseLong(value));
        }
        catch (NumberFormatException e) {
            return createDouble(Double.parseDouble(value));
        }
    }

    public static Scalar createBoolean(final boolean value) {
        return value ? TRUE_SCALAR : FALSE_SCALAR;
    }
    
    public static Scalar createLong(final long value) {
        return new LongScalar(value);
    }
    
    public static Scalar createDouble(final double value) {
        return new DoubleScalar(value);
    }
    
    public static Scalar createString(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new StringScalar(value);
    }
    
    public static Scalar createBinary(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new SmallBinaryScalar(value);
    }
    
    public static Scalar createBinary(final Callable<InputStream> valueProvider) {
        if (valueProvider == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new BinaryScalar(valueProvider);
    }

    protected ScalarImpl(int type) {
        this.type = type;
    }

    @Override
    public int getType() {
        return type;   
    }

    @Override
    public boolean getBoolean() {
        return Boolean.valueOf(getString());
    }

    @Override
    public long getLong() {
        return Long.parseLong(getString());
    }

    @Override
    public double getDouble() {
        return Double.parseDouble(getString());
    }

    @Override
    public InputStream getInputStream() {
        try {
            return new ByteArrayInputStream(getString().getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            // todo handle UnsupportedEncodingException
            return null;
        }
    }

    @Override
    public String toString() {
        return getString() + ": " + Scalar.typeNames[type];
    }

    //------------------------------------------------------------< private >---

    private static final BooleanScalar TRUE_SCALAR = new BooleanScalar(true);
    private static final BooleanScalar FALSE_SCALAR = new BooleanScalar(false);

    private static final class BooleanScalar extends ScalarImpl {
        private final boolean value;

        public BooleanScalar(boolean value) {
            super(Scalar.BOOLEAN);
            this.value = value;
        }

        @Override
        public boolean getBoolean() {
            return value;
        }

        @Override
        public String getString() {
            return Boolean.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            return value == ((BooleanScalar) other).value;
        }

        @Override
        public int hashCode() {
            return (value ? 1 : 0);
        }
    }

    private static final class LongScalar extends ScalarImpl {
        private final long value;

        public LongScalar(long value) {
            super(Scalar.LONG);
            this.value = value;
        }

        @Override
        public long getLong() {
            return value;
        }

        @Override
        public String getString() {
            return Long.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            return value == ((LongScalar) other).value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }

    private static final class DoubleScalar extends ScalarImpl {
        private final double value;

        public DoubleScalar(double value) {
            super(Scalar.DOUBLE);
            this.value = value;
        }

        @Override
        public double getDouble() {
            return value;
        }

        @Override
        public String getString() {
            return Double.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            return Double.compare(((DoubleScalar) other).value, value) == 0;

        }

        @Override
        public int hashCode() {
            long h = value != 0.0d ? Double.doubleToLongBits(value) : 0L;
            return (int) (h ^ (h >>> 32));
        }
    }

    private static final class StringScalar extends ScalarImpl {
        private final String value;

        public StringScalar(String value) {
            super(Scalar.STRING);
            this.value = value;
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return value.equals(((StringScalar) o).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final class SmallBinaryScalar extends ScalarImpl {
        private final String value;

        public SmallBinaryScalar(String value) {
            super(Scalar.BINARY);
            this.value = value;
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            return value.equals(((SmallBinaryScalar) other).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static class BinaryScalar extends ScalarImpl {
        private final Callable<InputStream> valueProvider;

        public BinaryScalar(Callable<InputStream> valueProvider) {
            super(Scalar.BINARY);
            this.valueProvider = valueProvider;
        }

        @Override
        public InputStream getInputStream() {
            try {
                return valueProvider.call();
            }
            catch (Exception e) {
                // todo handle Exception
                return null;
            }
        }

        @Override
        public String getString() {
            return ""; // todo implement getString
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            return getString().equals(((BinaryScalar) other).getString());
        }

        @Override
        public int hashCode() {
            return getString().hashCode();
        }
    }
}
