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

import org.apache.activemq.ConfigurationException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.shiro.env.Environment;
import org.apache.shiro.mgt.SecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 5.9.0
 */
public class ShiroPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroPlugin.class);

    private volatile boolean enabled = true;

    private Broker broker; //the downstream broker after any/all Shiro-specific broker filters

    private Environment environment;
    private SecurityManager securityManager;

    //Subject Filter and its components:
    private SubjectFilter subjectFilter;

    //AuthenticationFilter and its components:
    private AuthenticationFilter authenticationFilter;
    private volatile boolean authenticationEnabled;

    public ShiroPlugin() {
        authenticationEnabled = true;

        // we want to share one AuthenticationPolicy instance across both the AuthenticationFilter and the
        // ConnectionSubjectFactory:
        AuthenticationPolicy authcPolicy = new DefaultAuthenticationPolicy();

        authenticationFilter = new AuthenticationFilter();
        authenticationFilter.setAuthenticationPolicy(authcPolicy);

        subjectFilter = new SubjectFilter();
        subjectFilter.setNext(authenticationFilter);
        DefaultConnectionSubjectFactory subjectFactory = new DefaultConnectionSubjectFactory();
        subjectFactory.setAuthenticationPolicy(authcPolicy);
        subjectFilter.setConnectionSubjectFactory(subjectFactory);
    }

    public SubjectFilter getSubjectFilter() {
        return subjectFilter;
    }

    public void setSubjectFilter(SubjectFilter subjectFilter) {
        this.subjectFilter = subjectFilter;
        this.subjectFilter.setNext(this.authenticationFilter);
    }

    public AuthenticationFilter getAuthenticationFilter() {
        return authenticationFilter;
    }

    public void setAuthenticationFilter(AuthenticationFilter authenticationFilter) {
        this.authenticationFilter = authenticationFilter;
        this.subjectFilter.setNext(authenticationFilter);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (isInstalled()) {
            //we're running, so apply the changes now:
            applyEnabled(enabled);
        }
    }

    public boolean isEnabled() {
        if (isInstalled()) {
            return getNext() == this.subjectFilter;
        }
        return enabled;
    }

    private void applyEnabled(boolean enabled) {
        if (enabled) {
            //ensure the SubjectFilter and downstream filters are used:
            super.setNext(this.subjectFilter);
        } else {
            //Shiro is not enabled, restore the original downstream broker:
            super.setNext(this.broker);
        }
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public SecurityManager getSecurityManager() {
        return securityManager;
    }

    public void setSecurityManager(SecurityManager securityManager) {
        this.securityManager = securityManager;
    }

    // ===============================================================
    // Authentication Configuration
    // ===============================================================
    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
        if (isInstalled()) {
            this.authenticationFilter.setEnabled(authenticationEnabled);
        }
    }

    public boolean isAuthenticationEnabled() {
        if (isInstalled()) {
            return this.authenticationFilter.isEnabled();
        }
        return authenticationEnabled;
    }

    public AuthenticationPolicy getAuthenticationPolicy() {
        return authenticationFilter.getAuthenticationPolicy();
    }

    public void setAuthenticationPolicy(AuthenticationPolicy authenticationPolicy) {
        authenticationFilter.setAuthenticationPolicy(authenticationPolicy);
        //also set it on the ConnectionSubjectFactory:
        ConnectionSubjectFactory factory = subjectFilter.getConnectionSubjectFactory();
        if (factory instanceof DefaultConnectionSubjectFactory) {
            ((DefaultConnectionSubjectFactory) factory).setAuthenticationPolicy(authenticationPolicy);
        }
    }

    private Environment ensureEnvironment() throws ConfigurationException {
        if (this.environment != null) {
            return this.environment;
        }

        //this.environment is null - set it:
        final SecurityManager securityManager = this.securityManager;
        if (securityManager != null) {
            this.environment = new Environment() {
                @Override
                public SecurityManager getSecurityManager() {
                    return securityManager;
                }
            };
            return this.environment;
        }

        String msg = "Configuration error.  Ensure you have configured a Shiro Environment or SecurityManager " +
                "instance.";
        throw new ConfigurationException(msg);
    }

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        this.broker = broker;
        this.authenticationFilter.setNext(broker);

        long start = System.nanoTime();
        Environment environment = ensureEnvironment();
        long end = System.nanoTime();
        LOG.info("Shiro Environment initialized in " + ((end - start) / 1000) + " milliseconds");

        this.authenticationFilter.setEnvironment(environment);
        this.subjectFilter.setEnvironment(environment);

        Broker next = this.subjectFilter;
        if (!this.enabled) {
            //not enabled at startup - default to the original broker:
            next = broker;
        }

        setNext(next);
        return this;
    }

    private boolean isInstalled() {
        return getNext() != null;
    }
}
