package org.apache.activemq.shiro.session.mgt;

import org.apache.shiro.session.mgt.DefaultSessionKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since 0.8
 */
public class DisabledSessionManagerTest {

    private DisabledSessionManager mgr;

    @Before
    public void setUp() {
        this.mgr = new DisabledSessionManager();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStart() {
        mgr.start(null);
    }

    @Test
    public void testGetSession() {
        assertNull(mgr.getSession(null));
        assertNull(mgr.getSession(new DefaultSessionKey("foo")));
    }
}
