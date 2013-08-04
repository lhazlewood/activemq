package org.apache.activemq.shiro.session.mgt;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.shiro.session.mgt.DefaultSessionContext;

/**
 * @since 5.9.0
 */
public class ConnectionSessionContext extends DefaultSessionContext {

    private final ConnectionReference connection;

    public ConnectionSessionContext(ConnectionReference connection) {
        super();
        if (connection == null) {
            throw new IllegalArgumentException("ConnectionReference argument cannot be null.");
        }
        this.connection = connection;
        setHost(connection.getConnectionInfo().getClientIp());
    }

    public ConnectionReference getConnection() {
        return connection;
    }
}
