/**
 * 
 */
package com.kafka.adls;
/**
 * @author JN043533
 *
 */
class ADLSSinkConnectorConstants {
	
	private ADLSSinkConnectorConstants(){
		throw new IllegalStateException("Constants Class");
	}
	
	static final String ACCOUNTFQDN = "accountFQDN";
	static final String CLIENTID = "clientId";
	static final String AUTHTOKENENDPOINT = "authTokenEndpoint";
	static final String CLIENTKEY = "clientKey";
	static final String DIRECTORY = "directory";
	static final String FILENAME = "fileName";
	static final String TOPICNAME = "topics";
	static final String SCHEMAFILELOC = "avroschemafile";
	static final String COLUMNSEPERATOR = "columnseperator";
	static final String NEWLINE = "\n";
	static final String CARRIAGERETURN = "\r";
	static final String EMPTYSPACE = " ";
	static final String DIRSEPERATOR = "/"; 
	static final String FILEPERM = "777";
	static final String AVROFORMAT = "avro";
	static final String DELIMITEDFORMAT = "delimited";
	static final String FREETEXT = "freetext";
	static final String FROMFORMAT = "sourceformat";
	static final String TOFORMAT = "targetformat";
	static final long FILESIZE = 1073741824;
	static final String ERROR_STRING = "Error Processing Data from topic ";
}
