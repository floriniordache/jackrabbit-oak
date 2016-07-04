package org.apache.jackrabbit.oak.plugins.commit;


import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the {@link PrivateStoreValidatorProvider}
 */
public class PrivateStoreValidatorProviderTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private ContentRepository repository;

    private ServiceRegistration mipServiceRegistration;

    private PrivateStoreValidatorProvider privateStoreValidatorProvider;

    private void setUp(String... readOnlyPaths) {
        registerMountInfoProvider(readOnlyPaths);

        privateStoreValidatorProvider = new PrivateStoreValidatorProvider();
        registerValidatorProvider(privateStoreValidatorProvider, true);

        repository = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(privateStoreValidatorProvider)
                .createContentRepository();
    }

    @After
    public void tearDown() {
        repository = null;
    }

    @Test
    public void testDefaultMount() throws Exception {
        setUp();

        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("test");
        t.addChild("node1").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
        t.addChild("node3").setProperty("jcr:primaryType", "nt:base");
        r.commit();

        t.getChild("node1").removeProperty("jcr:primaryType");
        r.commit();

        t.getChild("node1").remove();
        r.commit();
    }

    @Test
    public void testReadOnlyMounts() throws Exception {
        // default mount info provider to setup some content
        setUp();
        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("content");
        t.addChild("node1").setProperty("jcr:primaryType", "nt:base");

        Tree readonlyRoot = t.addChild("readonly");
        readonlyRoot.setProperty("jcr:primaryType", "nt:base");
        readonlyRoot.addChild("readonlyChild").setProperty("jcr:primaryType", "nt:base");

        r.commit();

        // register a different mount info provider
        registerMountInfoProvider("/content/readonly");

        // commits under /content/readonly should now fail
        s = repository.login(null, null);

        // changes that are not under the read-only mount should work
        r = s.getLatestRoot();
        t = r.getTree("/").addChild("content");
        t.addChild("node2").setProperty("jcr:primaryType", "nt:base");
        r.commit();

        // changes under the read-only mount should fail
        readonlyRoot = t.getChild("readonly");
        readonlyRoot.setProperty("testProp", "test");
        try {
            r.commit();
            Assert.fail("Commit to read-only mount should fail!");
        } catch (Exception e) {
        }

        r.refresh();
        readonlyRoot = t.getChild("readonly");
        readonlyRoot.getChild("readonlyChild").remove();
        try {
            r.commit();
            Assert.fail("Commit to read-only mount should fail!");
        } catch (Exception e) {
        }
    }

    @Test
    public void testValidatorServiceRegistered() {
        // test service registration, there should be a service for the PrivateStoreValidatorProvider
        setUp("/content/readonly");

        Object validator = getValidatorService(PrivateStoreValidatorProvider.class);
        Assert.assertNotNull("No PrivateStoreValidatorProvider available!", validator);
    }

    @Test
    public void testValidatorServiceNotRegistered() {
        // test service registration, for default mount there should be no service for the validator provider
        setUp();

        Object validator = getValidatorService(PrivateStoreValidatorProvider.class);
        Assert.assertNull("No PrivateStoreValidatorProvider should be registered for default mounts!", validator);
    }

    private Object getValidatorService(Class tClass) {
        try {
            ServiceReference[] services = context.bundleContext().getServiceReferences(EditorProvider.class.getName(), null);
            for (ServiceReference serviceRef : services) {
                Object service = context.bundleContext().getService(serviceRef);

                if (service.getClass() == tClass) {
                    return service;
                }
            }
        } catch (Exception e) {}

        return null;
    }

    private void registerValidatorProvider(PrivateStoreValidatorProvider validatorProvider, boolean failOnDetection) {
        Map<String, Object> propMap = new HashMap<>();
        propMap.put("failOnDetection", failOnDetection);

        MockOsgi.injectServices(validatorProvider, context.bundleContext());
        MockOsgi.activate(validatorProvider, context.bundleContext(), propMap);
    }

    /**
     * Register a {@link MountInfoProvider} service
     * If the given path array is empty, the {@code MountInfoProvider.DEFAULT} will be registered.
     *
     * @param readOnlyPaths - contains the string paths mounted on a read-only store
     */
    private void registerMountInfoProvider(String... readOnlyPaths) {
        if (mipServiceRegistration != null) {
            // un-register first any existing MountInfoProvider
            mipServiceRegistration.unregister();
            mipServiceRegistration = null;
        }

        MountInfoProvider mountInfoProvider = MountInfoProvider.DEFAULT;
        if (readOnlyPaths != null && readOnlyPaths.length > 0) {
            mountInfoProvider = SimpleMountInfoProvider.newBuilder().readOnlyMount("readOnly", readOnlyPaths).build();
        }

        mipServiceRegistration = context.bundleContext().registerService(MountInfoProvider.class.getName(), mountInfoProvider, null);

        if (privateStoreValidatorProvider != null) {
            Whitebox.setInternalState(privateStoreValidatorProvider, "mountInfoProvider", mountInfoProvider);
            registerValidatorProvider(privateStoreValidatorProvider, true);
        }
    }
}
