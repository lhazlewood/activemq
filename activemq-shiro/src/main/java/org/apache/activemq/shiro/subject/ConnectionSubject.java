package org.apache.activemq.shiro.subject;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.session.mgt.ConnectionSessionContext;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.subject.support.DelegatingSubject;
import org.apache.shiro.util.StringUtils;

/**
 * @since 5.9.0
 */
public class ConnectionSubject extends DelegatingSubject {

    private final ConnectionReference connection;

    public ConnectionSubject(SecurityManager securityManager, ConnectionReference connection) {
        super(null, false, connection.getConnectionInfo().getClientIp(), null, true, securityManager);
        this.connection = connection;
    }

    @Override
    protected SessionContext createSessionContext() {
        ConnectionSessionContext context = new ConnectionSessionContext(this.connection);
        String host = getHost();
        if (StringUtils.hasText(host)) {
            context.setHost(host);
        }
        return context;
    }
}
