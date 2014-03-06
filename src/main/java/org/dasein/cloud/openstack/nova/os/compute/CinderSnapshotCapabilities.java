package org.dasein.cloud.openstack.nova.os.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.SnapshotCapabilities;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Describes the capabilities of Openstack with respect to Dasein snapshot operations.
 * User: daniellemayne
 * Date: 06/03/2014
 * Time: 09:49
 */
public class CinderSnapshotCapabilities extends AbstractCapabilities<NovaOpenStack> implements SnapshotCapabilities{
    public CinderSnapshotCapabilities(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForSnapshot(@Nonnull Locale locale) {
        return "snapshot";
    }

    @Nonnull
    @Override
    public Requirement identifyAttachmentRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean supportsSnapshotCopying() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsSnapshotCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean supportsSnapshotSharing() throws InternalException, CloudException {
        return false;
    }

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return false;
    }
}
