package org.apache.activemq.shiro.session.mgt;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.SubjectSecurityContext;
import org.apache.shiro.session.mgt.SessionKey;

import java.io.Serializable;

/**
 * @since 0.8
 */
public class ConnectionSessionKey implements SessionKey {

    private final ConnectionReference connection;

    public ConnectionSessionKey(ConnectionReference connection) {
        if (connection == null) {
            throw new IllegalArgumentException("ConnectionReference argument cannot be null.");
        }
        this.connection = connection;
    }

    @Override
    public Serializable getSessionId() {
        return ((SubjectSecurityContext) connection.getConnectionContext().getSecurityContext()).getSession().getId();
    }

    public ConnectionReference getConnection() {
        return connection;
    }
}
