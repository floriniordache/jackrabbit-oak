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

package org.apache.jackrabbit.oak.plugins.multiplex;

import org.apache.felix.scr.annotations.*;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.*;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link Validator} which detects commits containing references across shared and private repositories
 */

@Component(label = "Apache Jackrabbit Oak CrossStoreReferencesValidatorProvider")
public class CrossStoreReferencesValidatorProvider extends ValidatorProvider {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String ROOT_PATH = "/";

    @Property(
        boolValue = true,
        label = "Fail when detecting cross store references",
        description = "Commits will fail if set to true when cross store property references are detected. If set to false the commit information is only logged."
    )
    public static final String PROP_FAIL_ON_DETECTION = "failOnDetection";
    private boolean failOnDetection;

    @Reference
    private MountInfoProvider mountInfoProvider;

    private ServiceRegistration serviceRegistration;

    @Nonnull
    public Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        MemoryNodeStore store = new MemoryNodeStore(after);
        Root systemRoot = RootFactory.createSystemRoot(store, EmptyHook.INSTANCE, null,
                null, new QueryEngineSettings(),
                new CompositeQueryIndexProvider(new PropertyIndexProvider().with(mountInfoProvider),
                        new NodeTypeIndexProvider(), new ReferenceIndexProvider().with(mountInfoProvider)));
        IdentifierManager identifierManager = new IdentifierManager(systemRoot);

        return new CrossStoreReferencesValidator(ROOT_PATH, identifierManager);
    }

    @Activate
    protected void activate(BundleContext bundleContext, Map<String, ?> config) {
        failOnDetection = PropertiesUtil.toBoolean(config.get(PROP_FAIL_ON_DETECTION), true);

        if (mountInfoProvider.hasNonDefaultMounts()
            && serviceRegistration == null) {
            serviceRegistration = bundleContext.registerService(EditorProvider.class.getName(), this, null);
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

    private class CrossStoreReferencesValidator extends DefaultValidator {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        private String path;
        private IdentifierManager identifierManager;

        public CrossStoreReferencesValidator(String path, IdentifierManager identifierManager) {
            this.path = path;
            this.identifierManager = identifierManager;
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return getNewValidator(name);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return getNewValidator(name);
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
            validateCrossStoreReference(after);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after)
                throws CommitFailedException {
            validateCrossStoreReference(after);
        }

        private Validator getNewValidator(String name) {
            return new CrossStoreReferencesValidator(getNodePath(name), identifierManager);
        }

        private void validateCrossStoreReference(PropertyState propertyState) throws CommitFailedException {
            Type<?> propertyType = propertyState.getType();
            String propertyName = propertyState.getName();
            if (Type.REFERENCE.equals(propertyType)
                    || Type.WEAKREFERENCE.equals(propertyType)) {
                checkReference(propertyName, (String) propertyState.getValue(propertyType));
            } else if (Type.REFERENCES.equals(propertyType)
                        || Type.WEAKREFERENCES.equals(propertyType)) {
                Iterable<String> references = propertyState.getValue(Type.REFERENCES);
                for (String reference : references) {
                    checkReference(propertyName, reference);
                }
            }
        }

        private void checkReference(String propertyName, String refUUID) throws CommitFailedException {
            String targetPath = uuidToPath(refUUID);

            if (targetPath == null
                    || path == null) {
                return;
            }

            Mount targetMount = mountInfoProvider.getMountByPath(targetPath);
            Mount sourceMount = mountInfoProvider.getMountByPath(path);

            if (!sourceMount.equals(targetMount)) {
                Throwable throwable = new Throwable("Commit stacktrace: ");
                logger.error("Property "
                        + path + "/" + propertyName
                        + " references "
                        + targetPath
                        + " in a different repository!", throwable);

                if (failOnDetection) {
                    throw new CommitFailedException(CommitFailedException.UNSUPPORTED, 0,
                            "Cross store references are not supported!", throwable);
                }
            }
        }

        /**
         * Get a {@link String} path from a and a node name and the current path of the validator
         *
         * @param nodeName - the {@link String} name of the node
         * @return - the {@link String} path of the given node
         */
        private String getNodePath(String nodeName) {
            return PathUtils.concat(path, nodeName);
        }

        /**
         * Resolves a node path from a given uuid
         *
         * @param uuid - the UUID {@link String}
         *
         * @return - a {@link String} path of the node having the given uuid or {@code null} otherwise
         */
        private String uuidToPath(String uuid) {
            try {
                return identifierManager.getPath(uuid);
            } catch (Exception e) {
                logger.error("Can't determine path of node with UUID " + uuid, e);
            }

            return null;
        }
    }
}