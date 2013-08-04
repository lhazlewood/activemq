package org.apache.activemq.shiro.session.mgt;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.SessionException;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.session.mgt.SessionKey;
import org.apache.shiro.session.mgt.SessionManager;

/**
 * @since 0.8
 */
public class DisabledSessionManager implements SessionManager {

    @Override
    public Session start(SessionContext context) {
        String msg = "Sessions are disabled.";
        throw new UnsupportedOperationException(msg);
    }

    @Override
    public Session getSession(SessionKey key) throws SessionException {
        return null;
    }
}
