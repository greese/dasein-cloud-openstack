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

package org.dasein.cloud.openstack.nova.ec2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class NovaLocation implements DataCenterServices {
	static public final String DESCRIBE_AVAILABILITY_ZONES = "DescribeAvailabilityZones";
	static public final String DESCRIBE_REGIONS            = "DescribeRegions";
	
	private NovaEC2 provider = null;
	
	NovaLocation(NovaEC2 provider) {
		this.provider = provider;
	}

	@Override
	public DataCenter getDataCenter(String zoneId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_AVAILABILITY_ZONES);
		NovaMethod method;
        NodeList blocks;
		Document doc;
        
		parameters.put("ZoneName", zoneId);
        method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
        	String code = e.getCode();
        	
        	if( code != null && code.startsWith("InvalidZone") ) {
        		return null;
        	}
            if( code != null && code.equals("InvalidParameterValue") ) {
                String message = e.getMessage();
                
                if( message != null && message.startsWith("Invalid availability") ) {
                    return null;
                }
            }        	
        	NovaEC2.getLogger(NovaLocation.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("availabilityZoneInfo");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList zones = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<zones.getLength(); j++ ) {
            	Node region = zones.item(j);
            	
            	if( region.getNodeName().equals("item") ) {
            		DataCenter dc = toDataCenter(null, zones.item(j));
            		
            		if( dc != null && dc.getProviderDataCenterId().equals(zoneId) ) {
                        if( dc.getRegionId() == null ) {
                            for( Region r : listRegions() ) {
                                for( DataCenter d : listDataCenters(r.getProviderRegionId()) ) {
                                    if( d.getProviderDataCenterId().equals(dc.getProviderDataCenterId()) ) {
                                        dc.setRegionId(r.getProviderRegionId());
                                        break;
                                    }
                                }
                                if( dc.getRegionId() != null ) {
                                    break;
                                }
                            }
                        }
            			return dc;
            		}
            	}
            }
        }
        return null;
	}

	@Override
	public String getProviderTermForDataCenter(Locale locale) {
		return "availability zone";
	}

	@Override
	public String getProviderTermForRegion(Locale locale) {
		return "region";
	}

	@Override
	public Region getRegion(String regionId) throws InternalException, CloudException {
	    for( Region region : listRegions() ) {
	        if( region.getProviderRegionId().equals(regionId) ) {
	            return region;
	        }
	    }
	    return null;
	}

	@Override
	public Collection<DataCenter> listDataCenters(String regionId) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_AVAILABILITY_ZONES);
		NovaMethod method = new NovaMethod(provider, provider.getEc2Url(), parameters);
		ArrayList<DataCenter> list = new ArrayList<DataCenter>();
        NodeList blocks;
		Document doc;
        
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
            NovaEC2.getLogger(NovaLocation.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("availabilityZoneInfo");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	NodeList zones = blocks.item(i).getChildNodes();
        	
            for( int j=0; j<zones.getLength(); j++ ) {
            	Node region = zones.item(j);
            	
            	if( region.getNodeName().equals("item") ) {
            		list.add(toDataCenter(regionId, zones.item(j)));
            	}
            }
        }
        return list;
	}

	@Override
	public Collection<Region> listRegions() throws InternalException, CloudException {
        ArrayList<Region> list = new ArrayList<Region>();
        
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_REGIONS);
		NovaMethod method = new NovaMethod(provider, provider.getEc2Url(), parameters);
        NodeList blocks, regions;
		Document doc;
        
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
            NovaEC2.getLogger(NovaLocation.class, "std").error(e.getSummary());
            e.printStackTrace();
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("regionInfo");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	regions = blocks.item(i).getChildNodes();
            for( int j=0; j<regions.getLength(); j++ ) {
            	Node region = regions.item(j);
            	
            	if( region.getNodeName().equals("item") ) {
            	    Region r = toRegion(regions.item(j));

            	    if( r != null ) {
            	        list.add(r);
            	    }
            	}
            }
        }

        return list;
	}

	Map<String,String> mapRegions(String url) throws InternalException, CloudException {
		Map<String,String> parameters = provider.getStandardParameters(provider.getContext(), DESCRIBE_REGIONS);
		HashMap<String,String> results = new HashMap<String,String>();
		NovaMethod method = new NovaMethod(provider, url, parameters);
        NodeList blocks, regions;
		Document doc;
       
        try {
        	doc = method.invoke();
        }
        catch( NovaException e ) {
            NovaEC2.getLogger(NovaLocation.class, "std").error(e.getSummary());
        	throw new CloudException(e);
        }
        blocks = doc.getElementsByTagName("regionInfo");
        for( int i=0; i<blocks.getLength(); i++ ) {
        	regions = blocks.item(i).getChildNodes();
            for( int j=0; j<regions.getLength(); j++ ) {
            	Node region = regions.item(j);
            	
            	if( region.getNodeName().equals("item") ) {
                	NodeList data = region.getChildNodes();
                	String name = null, endpoint = null;
                	
	            	for( int k=0; k<data.getLength(); k++ ) {
	            		Node item = data.item(k);
	            		
	            		if( item.getNodeName().equals("regionName") ) {
	            			name = item.getFirstChild().getNodeValue();
	            		}
	            		else if( item.getNodeName().equals("regionEndpoint") ) {
	            			endpoint = item.getFirstChild().getNodeValue();
	            		}
	            	}
	            	if( name != null && endpoint != null ) {
	            	    NovaEC2.getLogger(NovaLocation.class, "std").debug(name + "=" + endpoint);
	            		results.put(name, endpoint);
	            	}
            	}
            }
        }
		return results;
	}
	
	private DataCenter toDataCenter(String regionId, Node zone) throws CloudException {
		NodeList data = zone.getChildNodes();
		DataCenter dc = new DataCenter();
		
		dc.setActive(true);
		dc.setAvailable(true);
		dc.setRegionId(regionId);
		for( int i=0; i<data.getLength(); i++ ) {
			Node item = data.item(i);
			String name = item.getNodeName();
			
			if( name.equals("zoneName") ) {
				String value = item.getFirstChild().getNodeValue().trim();
				
				dc.setName(value);
				dc.setProviderDataCenterId(value);
			}
			else if( name.equals("regionName") ) {
			    if( item.hasChildNodes() ) {
			        String value = item.getFirstChild().getNodeValue();
				
			        if( value != null ) {
			            dc.setRegionId(value.trim());
			        }
				}
			}
		}
		if( dc.getName() == null ) {
			throw new CloudException("Availability zone info is incomplete for " + dc.getProviderDataCenterId() + ".");
		}
		return dc;
	}
	
	private Region toRegion(Node region) throws CloudException {
		String name = null, endpoint = null;
		NodeList data;
            	
		data = region.getChildNodes();
		for( int i=0; i<data.getLength(); i++ ) {
			Node item = data.item(i);
			
			if( item.getNodeName().equals("regionName") ) {
				name = item.getFirstChild().getNodeValue();
			}
			else if( item.getNodeName().equals("regionEndpoint") ) {
				endpoint = item.getFirstChild().getNodeValue();
			}
		}
		if( name == null || endpoint == null ) {
			throw new CloudException("Invalid region data.");
		}
		Region r = new Region();
		r.setActive(true);
		r.setAvailable(true);
		r.setName(name);
		r.setProviderRegionId(name);
		return r;
	}
}
