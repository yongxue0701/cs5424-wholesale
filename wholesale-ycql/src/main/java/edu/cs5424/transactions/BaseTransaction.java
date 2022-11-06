package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public abstract class BaseTransaction {
    private final CqlSession session;
    private final String[] params;

    public BaseTransaction(final CqlSession session, final String[] params) {
        this.session = session;
        this.params = params;
    }

    public abstract void execute();
}
