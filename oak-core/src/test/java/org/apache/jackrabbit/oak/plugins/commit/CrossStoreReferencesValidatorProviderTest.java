package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.*;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the {@link CrossStoreReferencesValidatorProvider}
 */
public class CrossStoreReferencesValidatorProviderTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private ContentRepository repository;

    private ServiceRegistration mipServiceRegistration;

    private CrossStoreReferencesValidatorProvider crossStoreReferencesValidatorProvider;

    private String testUUID = "12345678-1234-1234-1234-123456789012";
    private String replaceUUID = "12345678-1234-1234-1234-123456789013";

    private String[] defaultReadonlyPaths = new String[] {"/content/readonly"};

    @Before
    public void setUp() throws Exception {
        registerMountInfoProvider(defaultReadonlyPaths);

        crossStoreReferencesValidatorProvider = new CrossStoreReferencesValidatorProvider();
        registerValidatorProvider(crossStoreReferencesValidatorProvider, true);

        repository = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(crossStoreReferencesValidatorProvider)
                .createContentRepository();

        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree rootTree = r.getTree("/");
        Tree target = rootTree.addChild("content").addChild("target");
        // setup a target node with a given uuid
        target.setProperty("jcr:primaryType", "nt:base");
        target.setProperty("jcr:mixinTypes", "mix:referenceable");
        target.setProperty("jcr:uuid", testUUID);

        // also set up some content
        rootTree.addChild("defaultContent");

        // set up the readonly path content
        for (String readonlyPath : defaultReadonlyPaths) {

            Tree currentTree = rootTree;
            for (String nodeName : readonlyPath.split("/")) {
                if (StringUtils.isNotEmpty(nodeName)) {
                    currentTree  = currentTree.addChild(nodeName);
                }
            }
        }
        r.commit();
    }

    @After
    public void tearDown() {
        repository = null;
    }

    @Test
    public void testValidatorServiceRegistered() throws Exception {
        // test service registration, there should be a service for the CrossStoreReferencesValidatorProvider
        ServiceReference serviceRef = context.bundleContext().getServiceReference(CrossStoreReferencesValidatorProvider.class.getName());
        Assert.assertNotNull("No PrivateStoreValidatorProvider available!", serviceRef);
    }

    @Test
    public void testValidatorServiceNotRegistered() throws Exception {
        // test service registration, for default mount there should be no service for the validator provider
        registerMountInfoProvider();

        ServiceReference serviceRef = context.bundleContext().getServiceReference(CrossStoreReferencesValidatorProvider.class.getName());
        Assert.assertNull("No PrivateStoreValidatorProvider should be registered for default mounts!", serviceRef);
    }

    @Test
    public void testAddReferenceSameStore() throws Exception {
        testSetReferenceProperty("/defaultContent", "testRef", testUUID, Type.REFERENCE, false);
    }

    @Test
    public void testAddReferenceDifferentStore() throws Exception {
        testSetReferenceProperty("/content/readonly", "testRef", testUUID, Type.REFERENCE, true);
    }

    @Test
    public void testUpdateReferenceSameStore() throws Exception {
        testSetReferenceProperty("/defaultContent", "testUpdateRef", replaceUUID, Type.REFERENCE, false);

        // update the property should work
        testSetReferenceProperty("/defaultContent", "testUpdateRef", testUUID, Type.REFERENCE, false);
    }

    @Test
    public void testUpdateReferenceDifferentStore() throws Exception {
        testSetReferenceProperty("/content/readonly", "testUpdateRef", replaceUUID, Type.REFERENCE, false);

        // update the property should fail if it points to another store
        testSetReferenceProperty("/content/readonly", "testUpdateRef", testUUID, Type.REFERENCE, true);
    }

    public void testSetReferenceProperty(String path, String propName, String value, Type type, boolean shouldFail) throws Exception {
        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        r.getTree(path).setProperty(propName, value, type);

        try {
            r.commit();
            if (shouldFail) {
                Assert.fail("Adding a Reference property pointing to a different mount should fail!");
            }
        } catch (Exception e) {
            if (!shouldFail) {
                Assert.fail("Adding a Reference property should work!");
            }
        }
    }

    private void registerValidatorProvider(CrossStoreReferencesValidatorProvider provider, boolean failOnDetection) {
        MockOsgi.injectServices(provider, context.bundleContext());

        Map<String, Object> propMap = new HashMap<>();
        propMap.put("failOnDetection", failOnDetection);

        MockOsgi.activate(provider, context.bundleContext(), propMap);
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

        MountInfoProvider mip = MountInfoProvider.DEFAULT;
        if (readOnlyPaths != null && readOnlyPaths.length > 0) {
            mip = SimpleMountInfoProvider.newBuilder().readOnlyMount("readOnly", readOnlyPaths).build();
        }

        mipServiceRegistration = context.bundleContext().registerService(MountInfoProvider.class.getName(), mip, null);
    }
}
