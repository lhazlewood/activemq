package org.apache.activemq.shiro.mgt;

import org.apache.activemq.shiro.session.mgt.DisabledSessionManager;
import org.apache.activemq.shiro.subject.mgt.ConnectionSubjectFactory;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;

/**
 * @since 0.8
 */
public class DefaultActiveMqSecurityManager extends DefaultSecurityManager {

    public DefaultActiveMqSecurityManager() {
        super();
        DefaultSubjectDAO subjectDAO = (DefaultSubjectDAO) getSubjectDAO();
        DefaultSessionStorageEvaluator evaluator = (DefaultSessionStorageEvaluator) subjectDAO.getSessionStorageEvaluator();
        evaluator.setSessionStorageEnabled(false);
        setSubjectFactory(new ConnectionSubjectFactory());
        setSessionManager(new DisabledSessionManager());
    }
}
