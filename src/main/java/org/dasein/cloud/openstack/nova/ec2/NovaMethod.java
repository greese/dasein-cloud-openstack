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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class NovaMethod {
	private int                attempts    = 0;
	private Map<String,String> parameters  = null;
	private NovaEC2         provider    = null;
	private String             url         = null;
	
	public NovaMethod(NovaEC2 provider, String url, Map<String,String> parameters) throws InternalException {
		this.url = url;
		this.parameters = parameters;
		this.provider = provider;
		parameters.put(NovaEC2.P_SIGNATURE, provider.signEc2(provider.getContext().getAccessPrivate(), url, parameters));
	}
	
    protected HttpClient getClient() {
        String proxyHost = provider.getContext().getCustomProperties().getProperty("proxyHost");
        String proxyPort = provider.getContext().getCustomProperties().getProperty("proxyPort");
        HttpClient client = new HttpClient();
        
        if( proxyHost != null ) {
            int port = 0;
            
            if( proxyPort != null && proxyPort.length() > 0 ) {
                port = Integer.parseInt(proxyPort);
            }
            client.getHostConfiguration().setProxy(proxyHost, port);
        }
        return client;
    }
    
	public Document invoke() throws NovaException, CloudException, InternalException {
        Logger logger = NovaEC2.getLogger(NovaMethod.class, "std");
        Logger wire = NovaEC2.getLogger(NovaMethod.class, "wire");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + NovaMethod.class.getName() + ".invoke()");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------------> " + url);
            wire.debug("");
        }
        try {
            StringBuilder paramString = new StringBuilder();
            TreeSet<String> keys = new TreeSet<String>();
            
            keys.addAll(parameters.keySet());
            for( String key : keys ) {
                if( !key.equalsIgnoreCase("signature") ) {
                    if( paramString.length() > 0 ) {
                        paramString.append("&");
                    }
                    paramString.append(key);
                    paramString.append("=");
                    try {
                        paramString.append(URLEncoder.encode(parameters.get(key), "utf-8"));
                    }
                    catch( UnsupportedEncodingException e ) {
                        throw new InternalException(e);
                    }
                }
            }
            paramString.append("&Signature=");
            try {
                paramString.append(URLEncoder.encode(parameters.get("Signature"), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
            GetMethod get = new GetMethod(url + "/?" + paramString.toString());
            HttpClient client = getClient();
            
            get.addRequestHeader("User-agent", "Dasein Cloud");
            get.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
    		try {
        		int status;
        
        		attempts++;
                if( wire.isDebugEnabled() ) {
                    wire.debug("GET " + get.getPath() + "?" + get.getQueryString());
                    for( Header header : get.getRequestHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
        		try {
        			status =  client.executeMethod(get);
        		} 
        		catch( HttpException e ) {
        		    logger.error("invoke(): HTTP error executing query: " + e.getMessage());
        		    if( logger.isDebugEnabled() ) {
        		        e.printStackTrace();
        		    }
        			throw new CloudException(e);
        		} 
        		catch( IOException e ) {
                    logger.error("invoke(): I/O error executing query: " + e.getMessage());
                    if( logger.isDebugEnabled() ) {
                        e.printStackTrace();
                    }
        			throw new InternalException(e);
        		}
                if( logger.isDebugEnabled() ) {
                    logger.debug("invoke(): HTTP Status " + status);
                }
                Header[] headers = get.getResponseHeaders();
                
                if( wire.isDebugEnabled() ) {
                    wire.debug(get.getStatusLine().toString());
                    for( Header h : headers ) {
                        if( h.getValue() != null ) {
                            wire.debug(h.getName() + ": " + h.getValue().trim());
                        }
                        else {
                            wire.debug(h.getName() + ":");
                        }
                    }
                    wire.debug("");
                }
        		if( status != HttpStatus.SC_OK ) {
                    String response = null;
                    InputStream input;
                    
                    try {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response = get.getResponseBodyAsString());
                        }
                        input = get.getResponseBodyAsStream();
                    }
                    catch( IOException e ) {
                        logger.error("invoke(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( logger.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    try {
                        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(input);
                    }
                    catch( SAXException e ) {
                        throw new CloudException(response);
                    }
                    catch( IOException e ) {
                        throw new InternalException(e);
                    }
                    catch( ParserConfigurationException e ) {
                        throw new InternalException(e);
                    }
        		}
        		else {
        		    InputStream input;
                    
                    try {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(get.getResponseBodyAsString());
                        }
                        input = get.getResponseBodyAsStream();
                    }
                    catch( IOException e ) {
                        logger.error("invoke(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( logger.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    try {
                        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(input);
                    }
                    catch( SAXException e ) {
                        throw new CloudException("Invalid XML: " + e.getMessage());
                    }
                    catch( IOException e ) {
                        throw new InternalException(e);
                    }
                    catch( ParserConfigurationException e ) {
                        throw new InternalException(e);
                    }
        		}
    		}
    		finally {
    		    get.releaseConnection();
    		}
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NovaMethod.class.getName() + ".invoke()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------------> " + url);
            } 
        }
	}
}
