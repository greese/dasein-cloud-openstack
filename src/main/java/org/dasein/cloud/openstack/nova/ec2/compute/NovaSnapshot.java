/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.openstack.nova.ec2.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.compute.Snapshot;
import org.dasein.cloud.compute.SnapshotState;
import org.dasein.cloud.compute.SnapshotSupport;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.ec2.NovaEC2;
import org.dasein.cloud.openstack.nova.ec2.NovaException;
import org.dasein.cloud.openstack.nova.ec2.NovaMethod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;

public class NovaSnapshot implements SnapshotSupport {
	static public final String CREATE_SNAPSHOT             = "CreateSnapshot";
	static public final String DELETE_SNAPSHOT             = "DeleteSnapshot";
	static public final String DESCRIBE_SNAPSHOTS          = "DescribeSnapshots";
	static public final String DESCRIBE_SNAPSHOT_ATTRIBUTE = "DescribeSnapshotAttribute";
	static public final String MODIFY_SNAPSHOT_ATTRIBUTE   = "ModifySnapshotAttribute";
	
	private NovaEC2 provider = null;
	
	NovaSnapshot(NovaEC2 provider) {
		this.provider = provider;
	}
	
	@Override
	public String create(String fromVolumeId, String description) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), CREATE_SNAPSHOT);
		NovaMethod method;
        NodeList blocks;
		Document doc;

		parameters.put("VolumeId", fromVolumeId);
		parameters.put("Description", description == null ? ("From " + fromVolumeId) : description);
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaSnapshot.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("snapshotId");
        if( blocks.getLength() > 0 ) {
        	return blocks.item(0).getFirstChild().getNodeValue().trim();
        }
        return null;
	}

	@Override
	public void remove(String snapshotId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DELETE_SNAPSHOT);
		NovaMethod method;
        NodeList blocks;
		Document doc;

		parameters.put("SnapshotId", snapshotId);
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
            String code = e.getCode();
            
            if( code != null ) {
                if( code.equals("InvalidSnapshot.NotFound") ) {
                    return;
                }
            }
        	NovaEC2.getLogger(NovaSnapshot.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
        	if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
        		throw new CloudException("Deletion of snapshot denied.");
        	}
        }
	}

	@Override
	public String getProviderTermForSnapshot(Locale locale) {
		return "snapshot";
	}

	@Override
	public Iterable<String> listShares(String forSnapshotId) throws InternalException, CloudException {
	    return Collections.emptyList();
	}

	@Override
	public Snapshot getSnapshot(String snapshotId) throws InternalException, CloudException {
	    for( Snapshot snapshot : listSnapshots() ) {
	        if( snapshot.getProviderSnapshotId().equals(snapshotId) ) {
	            return snapshot;
	        }
	    }
	    return null;
	}

	@Override
	public boolean isPublic(String snapshotId) throws InternalException, CloudException {
	    return false;
	}


    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        return true;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

	@Override
	public boolean supportsSnapshotSharing() throws InternalException, CloudException {
		return false;
	}

    @Override
    public boolean supportsSnapshotSharingWithPublic() throws InternalException, CloudException {
        return false;
    }
    
	@Override
	public Iterable<Snapshot> listSnapshots() throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_SNAPSHOTS);
		ArrayList<Snapshot> list = new ArrayList<Snapshot>();
		NovaMethod method;
        NodeList blocks;
		Document doc;

		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	NovaEC2.getLogger(NovaSnapshot.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("snapshotSet");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList items = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<items.getLength(); j++ ) {
            	Node item = items.item(j);
            	
            	if( item.getNodeName().equals("item") ) {
            		Snapshot snapshot = toSnapshot(item);
            		
            		if( snapshot != null ) {
            			list.add(snapshot);
            		}
            	}
            }
        }
        return list;
	}

	@Override
	public void shareSnapshot(String snapshotId, String withAccountId, boolean affirmative) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), MODIFY_SNAPSHOT_ATTRIBUTE);
		NovaMethod method;
        NodeList blocks;
		Document doc;

		parameters.put("SnapshotId", snapshotId);
		if( withAccountId == null ) {
			parameters.put("UserGroup.1", "all");
		}
		else {
			parameters.put("UserId.1", withAccountId);
		}
		parameters.put("Attribute", "createVolumePermission");
		parameters.put("OperationType", affirmative ? "add" : "remove");
		method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidSnapshot.NotFound") ) {
        		return;
        	}
        	NovaEC2.getLogger(NovaSnapshot.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        };
        blocks = doc.getElementsByTagName("return");
        if( blocks.getLength() > 0 ) {
        	if( !blocks.item(0).getFirstChild().getNodeValue().equalsIgnoreCase("true") ) {
        		throw new CloudException("Deletion of snapshot denied.");
        	}
        }
	}
	
	private Snapshot toSnapshot(Node node) throws CloudException {
		NodeList attrs = node.getChildNodes();
		Snapshot snapshot = new Snapshot();
		
		snapshot.setOwner(provider.getContext().getAccountNumber());
		for( int i=0; i<attrs.getLength(); i++ ) {
			Node attr = attrs.item(i);
			String name;
			
			name = attr.getNodeName();
			if( name.equals("snapshotId") ) {
				snapshot.setProviderSnapshotId(attr.getFirstChild().getNodeValue().trim());
				snapshot.setName(snapshot.getProviderSnapshotId());
			}
			else if( name.equals("volumeId") ) {
				NodeList children = attr.getChildNodes();
				
				if( children != null && children.getLength() > 0 ) {
					String vol = children.item(0).getNodeValue();
					
					if( vol != null ) {
						vol = vol.trim();
						if( vol.length() > 0 ) {
							snapshot.setVolumeId(vol);
						}
					}
				}
			}
			else if( name.equals("status") ) {
				String s = attr.getFirstChild().getNodeValue().trim();
				SnapshotState state;
				
		        if( s.equals("completed") ) {
		            state = SnapshotState.AVAILABLE;
		        }
		        else if( s.equals("deleting") || s.equals("deleted") ) {
		            state = SnapshotState.DELETED;
		        }
		        else {
		            state = SnapshotState.PENDING;
		        }
		        snapshot.setCurrentState(state);
			}
			else if( name.equals("startTime") ) {
				NodeList children = attr.getChildNodes();
				long ts = 0L;
				
				if( children != null && children.getLength() > 0 ) {
					String t = children.item(0).getNodeValue();
					
					if( t != null ) {
					    ts = provider.parseTime(t);
					}
				}
				snapshot.setSnapshotTimestamp(ts);
			}
			else if( name.equals("progress") ) {
				NodeList children = attr.getChildNodes();
				String progress = "100%";
				
				if( children != null && children.getLength() > 0 ) {
					String p = children.item(0).getNodeValue();
					
					if( p != null ) {
						p = p.trim();
						if( p.length() > 0 ) {
							progress = p;
						}
					}
				}
				snapshot.setProgress(progress);
			}
			else if( name.equals("ownerId") ) {
				snapshot.setOwner(attr.getFirstChild().getNodeValue().trim());
			}
			else if( name.equals("volumeSize") ) {
			    String val = attr.getFirstChild().getNodeValue().trim();
			    
			    if( val == null || val.equals("n/a") ) {
			        snapshot.setSizeInGb(0);
			    }
			    else {
			        snapshot.setSizeInGb(Integer.parseInt(val));
			    }
			}
			else if( name.equals("description") ) {
				NodeList children = attr.getChildNodes();
				String description = null;
				
				if( children != null && children.getLength() > 0 ) {
					String d = children.item(0).getNodeValue();
					
					if( d != null ) {
						description = d.trim();
					}
				}
				snapshot.setDescription(description);
			}
		}
		if( snapshot.getDescription() == null ) {
		    snapshot.setDescription(snapshot.getName() + " [" + snapshot.getSizeInGb() + " GB]");
		}
		snapshot.setRegionId(provider.getContext().getRegionId());
		if( snapshot.getSizeInGb() < 1 ) {
		    try {
		        Volume volume = provider.getComputeServices().getVolumeSupport().getVolume(snapshot.getProviderSnapshotId());
	            
	            if( volume != null ) {
	                snapshot.setSizeInGb(volume.getSizeInGigabytes());
	            }
            }
            catch( InternalException ignore ) {
                // ignore
            }
		}
		return snapshot;
	}
}
