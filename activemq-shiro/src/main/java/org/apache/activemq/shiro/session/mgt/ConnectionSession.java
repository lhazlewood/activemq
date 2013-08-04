package org.apache.activemq.shiro.session.mgt;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.shiro.session.mgt.SimpleSession;

/**
 * @since 5.9.0
 */
public class ConnectionSession extends SimpleSession {

    private final ConnectionReference connection;

    public ConnectionSession(ConnectionReference connection) {
        super();
        if (connection == null) {
            throw new IllegalArgumentException("ConnectionReference argument cannot be null.");
        }
        this.connection = connection;
    }

    public ConnectionReference getConnection() {
        return connection;
    }
}
