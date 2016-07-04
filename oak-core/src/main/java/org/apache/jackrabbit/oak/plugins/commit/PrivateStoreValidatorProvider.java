/*
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

package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.felix.scr.annotations.*;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link Validator} which detects change commits to the read only mounts.
 */
@Component(label = "Apache Jackrabbit Oak PrivateStoreValidatorProvider")
public class PrivateStoreValidatorProvider extends ValidatorProvider {
    private static final String ROOT_PATH = "/";

    @Property(
        boolValue = false,
        label = "Fail when detecting commits to the read-only stores",
        description = "Commits will fail if set to true when detecting changes to any read-only store. If set to false the commit information is only logged."
    )
    public static final String PROP_FAIL_ON_DETECTION = "failOnDetection";
    private boolean failOnDetection;

    @Reference
    private MountInfoProvider mountInfoProvider;

    private ServiceRegistration serviceRegistration;

    @Nonnull
    public Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        return new PrivateStoreValidator(ROOT_PATH);
    }

    @Activate
    @Modified
    protected void activate(BundleContext bundleContext, Map<String, ?> config) {
        failOnDetection = PropertiesUtil.toBoolean(config.get(PROP_FAIL_ON_DETECTION), false);

        if (mountInfoProvider != null
                && mountInfoProvider.hasNonDefaultMounts()) {
            if (serviceRegistration == null) {
                serviceRegistration = bundleContext.registerService(PrivateStoreValidatorProvider.class.getName(), this, null);
            }
        } else {
            unregisterValidatorProvider();
        }
    }

    @Deactivate
    private void deactivate() {
        unregisterValidatorProvider();
    }

    private void unregisterValidatorProvider() {
        if (serviceRegistration != null) {
            serviceRegistration.unregister();
            serviceRegistration = null;
        }
    }

    private class PrivateStoreValidator extends DefaultValidator {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private String path;

        public PrivateStoreValidator(String path) {
            this.path = path;
        }

        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        private Validator checkPrivateStoreCommit(String commitPath) throws CommitFailedException {
            if (mountInfoProvider != null) {
                Mount mountInfo = mountInfoProvider.getMountByPath(commitPath);
                if (mountInfo.isReadOnly()) {
                    Throwable throwable = new Throwable("Commit path: " + commitPath);
                    logger.error("Detected commit to a read-only store! ", throwable);

                    if (failOnDetection) {
                        throw new CommitFailedException(CommitFailedException.UNSUPPORTED, 0,
                                "Unsupported commit to a read-only store!", throwable);
                    }
                }
            }

            return new PrivateStoreValidator(commitPath);
        }

        private String getCommitPath(String changeNodeName) {
            String parentNodePath = path;

            String commitPath = ROOT_PATH.equals(parentNodePath)
                    ? parentNodePath + changeNodeName
                    : parentNodePath + "/" + changeNodeName;

            return commitPath;
        }
    }
}
