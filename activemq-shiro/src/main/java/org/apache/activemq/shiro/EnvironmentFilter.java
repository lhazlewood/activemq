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

import org.apache.shiro.env.Environment;

/**
 * An abstract {@code BrokerFilter} that makes the Shiro {@link Environment} available to subclasses.
 *
 * @since 5.9.0
 */
public abstract class EnvironmentFilter extends SecurityFilter {

    private Environment environment;

    public EnvironmentFilter() {
    }

    public Environment getEnvironment() {
        if (this.environment == null) {
            String msg = "Environment has not yet been set.  This should be done before this broker filter is used.";
            throw new IllegalStateException(msg);
        }
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
