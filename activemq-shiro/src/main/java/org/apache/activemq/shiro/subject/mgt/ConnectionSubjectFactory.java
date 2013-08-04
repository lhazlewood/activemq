package org.apache.activemq.shiro.subject.mgt;

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.subject.ConnectionSubject;
import org.apache.shiro.mgt.DefaultSubjectFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;

/**
 * @since 5.9.0
 */
public class ConnectionSubjectFactory extends DefaultSubjectFactory {

    public static final String CONNECTION_REFERENCE = ConnectionSubjectFactory.class.getName() + ".CONNECTION_REFERENCE";

    @Override
    public Subject createSubject(SubjectContext context) {
        ConnectionReference connection = (ConnectionReference) context.get(CONNECTION_REFERENCE);
        if (connection == null) {
            return super.createSubject(context);
        }

        SecurityManager sm = context.resolveSecurityManager();

        return new ConnectionSubject(sm, connection);
    }
}
