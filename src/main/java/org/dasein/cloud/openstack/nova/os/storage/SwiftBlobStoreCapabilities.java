package org.dasein.cloud.openstack.nova.os.storage;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.storage.BlobStoreCapabilities;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.uom.storage.*;
import org.dasein.util.uom.storage.Byte;

import javax.annotation.Nonnull;
import java.util.Locale;

public class SwiftBlobStoreCapabilities extends AbstractCapabilities<NovaOpenStack> implements BlobStoreCapabilities {
    static final  int                                       MAX_BUCKETS         = 100;
    static final  int                                       MAX_OBJECTS         = -1;
    static final Storage<org.dasein.util.uom.storage.Byte> MAX_OBJECT_SIZE     = new Storage<org.dasein.util.uom.storage.Byte>(5000000000L, Storage.BYTE);

    public SwiftBlobStoreCapabilities(NovaOpenStack provider) {
        super(provider);
    }

    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return true;
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return MAX_BUCKETS;
    }

    @Nonnull
    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return MAX_OBJECT_SIZE;
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return MAX_OBJECTS;
    }

    @Nonnull
    @Override
    public NamingConstraints getBucketNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(new char[]{'-', '.'});
    }

    @Nonnull
    @Override
    public NamingConstraints getObjectNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(new char[]{'-', '.', ',', '#', '+'});
    }

    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        return "bucket";
    }

    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return "object";
    }
}
