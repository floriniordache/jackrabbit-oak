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
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A validator provider designed to work only on systems where non-default mounts are available
 */

@Component
public abstract class NonDefaultMountsValidatorProvider extends ValidatorProvider {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Reference(policy = ReferencePolicy.DYNAMIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.OPTIONAL_UNARY)
    private volatile MountInfoProvider mountInfoProvider;

    protected void activate(BundleContext bundleContext, Map<String, ?> config) {
        detectMounts(bundleContext);
    }

    public MountInfoProvider getMountInfoProvider() {
        return mountInfoProvider;
    }

    protected void bindMountInfoProvider(ServiceReference serviceReference) {
        BundleContext bundleContext = serviceReference.getBundle().getBundleContext();
        this.mountInfoProvider = (MountInfoProvider) bundleContext.getService(serviceReference);
        detectMounts(bundleContext);
    }

    protected void unbindMountInfoProvider(ServiceReference serviceReference) {
        BundleContext bundleContext = serviceReference.getBundle().getBundleContext();
        MountInfoProvider mountInfoProvider = (MountInfoProvider) bundleContext.getService(serviceReference);
        if (this.mountInfoProvider == mountInfoProvider) {
            this.mountInfoProvider = null;
            detectMounts(bundleContext);
        }
    }

    private void detectMounts(BundleContext bundleContext) {
        if (hasNonDefaultMounts()) {
            logger.debug("Detected non-default mounts.");
            nonDefaultMountsDetected(bundleContext);
        } else {
            logger.debug("Detected default mounts.");
            defaultMountsDetected(bundleContext);
        }
    }

    private boolean hasNonDefaultMounts() {
        if (mountInfoProvider != null
                && mountInfoProvider.hasNonDefaultMounts()) {
            return true;
        }

        return false;
    }

    public abstract void nonDefaultMountsDetected(BundleContext bundleContext);

    public abstract void defaultMountsDetected(BundleContext bundleContext);
}
