package io.github.keemgdeok.kafk8s;

public final class Failure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public final String reasonCode;

    public Failure(String reasonCode) {
        super(reasonCode);
        this.reasonCode = reasonCode;
    }
}
