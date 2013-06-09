/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.shiro;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since 5.9.0
 */
public class ActiveMQWildcardPermissionTest {

    @Test
    public void testNotWildcardPermission() {
        ActiveMQWildcardPermission perm = new ActiveMQWildcardPermission("topic:TEST:*");
        Permission dummy = new Permission() {
            @Override
            public boolean implies(Permission p) {
                return false;
            }
        };
       assertFalse(perm.implies(dummy));
    }

    @Test
    public void testIntrapartWildcard() {
        ActiveMQWildcardPermission superset = new ActiveMQWildcardPermission("topic:ActiveMQ.Advisory.*:read");
        ActiveMQWildcardPermission subset = new ActiveMQWildcardPermission("topic:ActiveMQ.Advisory.Topic:read");

        assertTrue(superset.implies(subset));
        assertFalse(subset.implies(superset));
    }

    @Test
    public void testMatches() {
        assertTrue(matches("x", "x"));
        assertFalse(matches("x", "y"));
        assertTrue(matches("*", "x"));
        assertTrue(matches("*", "x:x"));
        assertTrue(matches("*", "x:x:x"));
        assertTrue(matches("foo?armat*", "foobarmatches"));
        assertTrue(matches("f*", "f"));
        assertTrue(matches("t*k?ou", "thankyou"));
        assertTrue(matches("*:ActiveMQ.Advisory", "foo:ActiveMQ.Advisory"));
        assertFalse(matches("*:ActiveMQ.Advisory", "foo:ActiveMQ.Advisory."));
        assertTrue(matches("*:ActiveMQ.Advisory*", "foo:ActiveMQ.Advisory"));
        assertTrue(matches("*:ActiveMQ.Advisory*", "foo:ActiveMQ.Advisory."));
        assertTrue(matches("*:ActiveMQ.Advisory.*", "foo:ActiveMQ.Advisory.Connection"));
        assertTrue(matches("*:ActiveMQ.Advisory*:read", "foo:ActiveMQ.Advisory.Connection:read"));
        assertFalse(matches("*:ActiveMQ.Advisory*:read", "foo:ActiveMQ.Advisory.Connection:write"));
        assertTrue(matches("*:ActiveMQ.Advisory*:*", "foo:ActiveMQ.Advisory.Connection:read"));
        assertTrue(matches("*:ActiveMQ.Advisory*:*", "foo:ActiveMQ.Advisory."));
        assertTrue(matches("topic", "topic:TEST:*"));
        assertFalse(matches("*:ActiveMQ*", "topic:TEST:*"));
        assertTrue(matches("topic:ActiveMQ.Advisory*", "topic:ActiveMQ.Advisory.Connection:create"));
        assertTrue(matches("foo?ar", "foobar"));
    }

    protected boolean matches(String pattern, String value) {
        ActiveMQWildcardPermission patternPerm = new ActiveMQWildcardPermission(pattern);
        WildcardPermission valuePerm = new WildcardPermission(value, true);
        return patternPerm.implies(valuePerm);
    }

}
