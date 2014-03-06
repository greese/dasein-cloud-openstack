package org.dasein.cloud.openstack.nova.os.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VolumeCapabilities;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Openstack with respect to Dasein volume operations.
 * User: daniellemayne
 * Date: 06/03/2014
 * Time: 10:15
 */
public class CinderVolumeCapabilities extends AbstractCapabilities<NovaOpenStack> implements VolumeCapabilities{
    public CinderVolumeCapabilities(@Nonnull NovaOpenStack provider) {
        super(provider);
    }

    @Override
    public boolean canAttach(VmState vmState) throws InternalException, CloudException {
        return true;
    }

    @Override
    public boolean canDetach(VmState vmState) throws InternalException, CloudException {
        return true;
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Nullable
    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1024, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        if( (getProvider()).isRackspace() ) {
            return new Storage<Gigabyte>(100, Storage.GIGABYTE);
        }
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "volume";
    }

    @Nonnull
    @Override
    public Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return ((getProvider()).isHP() ? Requirement.NONE : Requirement.OPTIONAL);
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        ArrayList<String> list = new ArrayList<String>();

        if( platform.isWindows() ) {
            if ((getProvider()).isHP()) {
                list.add("/dev/vdf");
                list.add("/dev/vdg");
                list.add("/dev/vdh");
                list.add("/dev/vdi");
                list.add("/dev/vdj");
            }
            else {
                list.add("/dev/xvdf");
                list.add("/dev/xvdg");
                list.add("/dev/xvdh");
                list.add("/dev/xvdi");
                list.add("/dev/xvdj");
            }
        }
        else {
            list.add("/dev/vdf");
            list.add("/dev/vdg");
            list.add("/dev/vdh");
            list.add("/dev/vdi");
            list.add("/dev/vdj");
        }
        return list;
    }

    @Nonnull
    @Override
    public Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Nonnull
    @Override
    public Requirement requiresVMOnCreate() throws InternalException, CloudException {
        return Requirement.NONE;
    }
}
