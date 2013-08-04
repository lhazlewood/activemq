package org.apache.activemq.shiro.session.mgt;

import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.SubjectSecurityContext;
import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.SessionException;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.DelegatingSession;
import org.apache.shiro.session.mgt.NativeSessionManager;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.session.mgt.SessionKey;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

/**
 * @since 0.8
 */
public class ConnectionSessionManager implements NativeSessionManager {

    @Override
    public Session start(SessionContext context) {
        if (!(context instanceof ConnectionSessionContext)) {
            throw new IllegalArgumentException("SessionContext must be an instance of " +
                    ConnectionSessionContext.class.getName());
        }

        ConnectionSessionContext ctx = (ConnectionSessionContext)context;

        ConnectionReference conn = ctx.getConnection();

        SubjectSecurityContext ssc = ensureSubjectSecurityContext(conn);
        ConnectionSession session = ssc.getSession();
        if (session != null) {
            return session;
        }


        session = newSessionInstance(conn);
        //Connection Sessions never timeout - they are tied to the actual connection.  When the connection is
        //terminated, the session is terminated
        session.setTimeout(-1l);


        ssc.setSession(session);

        return new DelegatingSession(this, new ConnectionSessionKey(conn));
    }

    protected final SubjectSecurityContext ensureSubjectSecurityContext(ConnectionReference connection) {
        SecurityContext ctx = connection.getConnectionContext().getSecurityContext();
        if (!(ctx instanceof SubjectSecurityContext)) {
            String msg = "Connection's SecurityContext must be an instance of " + SubjectSecurityContext.class.getName();
            throw new IllegalStateException(msg);
        }
        return (SubjectSecurityContext)ctx;
    }
    protected final ConnectionSessionKey ensureConnectionSessionKey(SessionKey key) {
        if (!(key instanceof ConnectionSessionKey)) {
            String msg = "SessionKey must be an instance of " + ConnectionSessionKey.class.getName();
            throw new IllegalArgumentException(msg);
        }
        return (ConnectionSessionKey)key;
    }

    protected ConnectionSession getConnectionSession(SessionKey key) {
        ConnectionSessionKey csk = ensureConnectionSessionKey(key);
        SubjectSecurityContext ssc = ensureSubjectSecurityContext(csk.getConnection());
        ConnectionSession session = ssc.getSession();
        if (session == null) {
            throw new UnknownSessionException("There is no session for the specified SessionKey");
        }
        return session;
    }

    protected ConnectionSession newSessionInstance(ConnectionReference connection) {
        return new ConnectionSession(connection);
    }

    @Override
    public Session getSession(SessionKey key) throws SessionException {
        ConnectionSessionKey csk = ensureConnectionSessionKey(key);
        return new DelegatingSession(this, csk);
    }

    @Override
    public Date getStartTimestamp(SessionKey key) {
        return getConnectionSession(key).getStartTimestamp();
    }

    @Override
    public Date getLastAccessTime(SessionKey key) {
        return getConnectionSession(key).getLastAccessTime();
    }

    @Override
    public boolean isValid(SessionKey key) {
        getConnectionSession(key); //validate the session exists.
        return true; //always because sessions are tied to connections
    }

    @Override
    public void checkValid(SessionKey key) throws InvalidSessionException {
        getConnectionSession(key); //validate the session exists.
    }

    @Override
    public long getTimeout(SessionKey key) throws InvalidSessionException {
        return getConnectionSession(key).getTimeout();
    }

    @Override
    public void setTimeout(SessionKey key, long maxIdleTimeInMillis) throws InvalidSessionException {
        String msg = "Cannot set Session timeout: sessions are bound to the ActiveMQ connection lifecycle.";
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public void touch(SessionKey key) throws InvalidSessionException {
        getConnectionSession(key).touch();
    }

    @Override
    public String getHost(SessionKey key) {
        return getConnectionSession(key).getHost();
    }

    @Override
    public void stop(SessionKey key) throws InvalidSessionException {
        String msg = "Cannot stop Sessions: sessions are bound to the ActiveMQ connection lifecycle.";
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Collection<Object> getAttributeKeys(SessionKey key) {
        Collection<Object> attrKeys = getConnectionSession(key).getAttributeKeys();
        return Collections.unmodifiableCollection(attrKeys);
    }

    @Override
    public Object getAttribute(SessionKey sessionKey, Object attributeKey) throws InvalidSessionException {
        return getConnectionSession(sessionKey).getAttribute(attributeKey);
    }

    @Override
    public void setAttribute(SessionKey sessionKey, Object attributeKey, Object value) throws InvalidSessionException {
        getConnectionSession(sessionKey).setAttribute(attributeKey, value);
    }

    @Override
    public Object removeAttribute(SessionKey sessionKey, Object attributeKey) throws InvalidSessionException {
        return getConnectionSession(sessionKey).removeAttribute(attributeKey);
    }
}
