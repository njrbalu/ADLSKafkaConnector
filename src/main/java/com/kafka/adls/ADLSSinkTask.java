package com.kafka.adls;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.Logger;

import com.cerner.ADLSFileSeekableInput;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

public class ADLSSinkTask extends SinkTask {

	final Logger logger = Logger.getLogger(ADLSSinkTask.class);
	private PrintStream out;
	private OutputStream stream;
	private String directory;
	private String topicName;
	private String columnSeperator;
	private String fromFormat;
	private String toFormat;
	private String schemaLoc;
	private ADLStoreClient client;
	private String currFile;
	private String accountFQDN;
	private String clientId;
	private String authTokenEndpoint;
	private String clientKey;

	public String version() {
		return null;
	}

	@Override
	public void start(Map<String, String> props) {
		logger.debug("Entering ADLSSinkTask::start");

		try {
			accountFQDN = props.get(ADLSSinkConnectorConstants.ACCOUNTFQDN);
			clientId = props.get(ADLSSinkConnectorConstants.CLIENTID);
			authTokenEndpoint = props.get(ADLSSinkConnectorConstants.AUTHTOKENENDPOINT);
			clientKey = props.get(ADLSSinkConnectorConstants.CLIENTKEY);
			directory = props.get(ADLSSinkConnectorConstants.DIRECTORY);
			topicName = props.get(ADLSSinkConnectorConstants.TOPICNAME);
			columnSeperator = props.get(ADLSSinkConnectorConstants.COLUMNSEPERATOR);
			schemaLoc = props.get(ADLSSinkConnectorConstants.SCHEMAFILELOC);
			fromFormat = props.get(ADLSSinkConnectorConstants.FROMFORMAT);
			toFormat = props.get(ADLSSinkConnectorConstants.TOFORMAT);

			// Checking all the required properties
			checkIfEmpty(ADLSSinkConnectorConstants.ACCOUNTFQDN, accountFQDN);
			checkIfEmpty(ADLSSinkConnectorConstants.CLIENTID, clientId);
			checkIfEmpty(ADLSSinkConnectorConstants.AUTHTOKENENDPOINT, authTokenEndpoint);
			checkIfEmpty(ADLSSinkConnectorConstants.CLIENTKEY, clientKey);
			checkIfEmpty(ADLSSinkConnectorConstants.DIRECTORY, directory);
			checkIfEmpty(ADLSSinkConnectorConstants.TOPICNAME, topicName);
			checkIfEmpty(ADLSSinkConnectorConstants.FROMFORMAT, fromFormat);
			checkIfEmpty(ADLSSinkConnectorConstants.TOFORMAT, toFormat);

			// Avro schema and column seperator are required when pushing data
			// from avro to delimited.
			if ((fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT)
					&& toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.DELIMITEDFORMAT))) {
				if (schemaLoc.isEmpty()) {
					throw new PropertyEmptyException(
							"Avro Schema is required when converting data from Avro to Delimited format");
				} else if (columnSeperator.isEmpty()) {
					throw new PropertyEmptyException(
							"Column Seperator is required while converting data from Avro to Delimited");
				}
			}

			// Avro schema is required for writing data in avro format
			if ((fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT)
					&& toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT) && schemaLoc.isEmpty())) {
				throw new PropertyEmptyException(
						"Avro Schema is required when converting data from Avro to Avro format");
			}

			// Cannot convert to Avro format from other formats
			if ((toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT))
					&& (!fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT))) {
				throw new PropertyEmptyException("Cannot convert " + fromFormat + " Format to Avro Format");
			}

			// Data can be moved in delimited format from delimited or avro
			if ((toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.DELIMITEDFORMAT))
					&& (!(fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT)
							|| fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.DELIMITEDFORMAT)))) {
				throw new PropertyEmptyException("Cannot convert " + fromFormat + " Format to Delimited Format");
			}

		} catch (PropertyEmptyException e) {
			logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
			throw new ConnectException("Couldn't start Connector due to configuration error", e);
		}
		logger.debug(topicName + " Exiting ADLSSinkTask::start");
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		logger.debug(topicName + " Entering ADLSSinkTask::put()");

		if (!records.isEmpty()) {
			logger.info("Processing " + records.size() + " records from " + topicName);

			try {
				if ((fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT)
						&& toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT))) {
					addAvroData(records);
				} else if ((fromFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.AVROFORMAT)
						&& toFormat.equalsIgnoreCase(ADLSSinkConnectorConstants.DELIMITEDFORMAT))) {
					addConvertedAvroToDelimitedData(records);
				} else {
					addRawData(records);
				}

			} catch (Exception e) {
				logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
				throw new ConnectException(e);
			}
		}

		logger.debug(topicName + " Exiting ADLSSinkTask::put()");
	}
	
	private void addRawData(Collection<SinkRecord> records) throws IOException {
		logger.debug(topicName + " Entering ADLSSinkTask::addRawData");
		try{
			createFile(".dat");
			for (SinkRecord record : records) {
				out.println(record.value());
			}
			if (out != null)
				out.close();
			if (stream != null)
				stream.close();
		} catch (IOException e) {
			logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
			throw new ConnectException(e);
		}
		
		logger.debug(topicName + " Exiting ADLSSinkTask::addRawData");
	}

	public void addAvroData(Collection<SinkRecord> records) throws IOException {
		logger.debug(topicName + " Entering ADLSSinkTask::AddAvroData");
		createFile(".avro");
		Schema schema = new Schema.Parser().parse(new File(schemaLoc));
		DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		ADLSFileSeekableInput seekableInput = new ADLSFileSeekableInput(currFile, client);
		DataFileWriter<GenericRecord> dw = dataFileWriter.appendTo(seekableInput, stream);
		for (SinkRecord record : records) {
			GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
			byte[] rawdata = (byte[]) record.value();
			Decoder decoder = DecoderFactory.get().binaryDecoder(rawdata, null);
			GenericRecord genericRecord = reader.read(null, decoder);
			dw.append(genericRecord);
		}
		seekableInput.close();
		dw.close();
		stream.close();
	}

	public void addConvertedAvroToDelimitedData(Collection<SinkRecord> records) throws IOException {
		logger.debug(topicName + " Entering ADLSSinkTask::addConvertedAvroToDelimitedData()");

		Schema schema = new Schema.Parser().parse(new File(schemaLoc));
		int cols = schema.getFields().size();
		createFile(".dat");
		for (SinkRecord record : records) {
			GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
			byte[] rawdata = (byte[]) record.value();
			Decoder decoder = null;
			GenericRecord genericRecord = null;
			StringBuilder data = new StringBuilder();

			try {
				decoder = DecoderFactory.get().binaryDecoder(rawdata, null);
				genericRecord = reader.read(null, decoder);
			} catch (UnsupportedEncodingException e) {
				logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
				throw new ConnectException(e);
			}

			for (int cntr = 0; cntr < cols; cntr++) {
				String value = (null != genericRecord.get(cntr)) ? genericRecord.get(cntr).toString() : "";
				if (value.contains(ADLSSinkConnectorConstants.NEWLINE)
						|| value.contains(ADLSSinkConnectorConstants.CARRIAGERETURN)) {
					value = value.replace(ADLSSinkConnectorConstants.NEWLINE, ADLSSinkConnectorConstants.EMPTYSPACE);
					value = value.replace(ADLSSinkConnectorConstants.CARRIAGERETURN,
							ADLSSinkConnectorConstants.EMPTYSPACE);
				}
				data.append(value).append(columnSeperator);
			}
			out.println(data);
		}

		try {
			if (out != null)
				out.close();
			if (stream != null)
				stream.close();
		} catch (IOException e) {
			logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
			throw new ConnectException(e);
		}
		logger.debug(topicName + " Exiting ADLSSinkTask::addConvertedAvroToDelimitedData");
	}

	/**
	 * @throws Exception
	 * 			@throws IOException @throws IOException @throws
	 * 
	 */
	public void createFile(String extension) {

		logger.debug(topicName + " Entering ADLSSinkTask::createFile");

		try {
			AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(authTokenEndpoint, clientId, clientKey);
			client = ADLStoreClient.createClient(accountFQDN, token);
			client.createDirectory(directory);

			Calendar c = Calendar.getInstance();
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH) + 1;
			int day = c.get(Calendar.DAY_OF_MONTH);
			int hour = c.get(Calendar.HOUR_OF_DAY);
			int minute = c.get(Calendar.MINUTE);
			int second = c.get(Calendar.SECOND);

			String file = directory + ADLSSinkConnectorConstants.DIRSEPERATOR + topicName + ADLSSinkConnectorConstants.DIRSEPERATOR + (Integer.toString(year))
					+ ADLSSinkConnectorConstants.DIRSEPERATOR + (formatString(Integer.toString(month), 2))
					+ ADLSSinkConnectorConstants.DIRSEPERATOR + topicName + Integer.toString(year)
					+ formatString(Integer.toString(month), 2) + formatString(Integer.toString(day), 2);

			currFile = file.concat(".").concat("000000").concat(extension);
			String newFile = file.concat(".").concat(formatString(Integer.toString(hour), 2)).concat(formatString(Integer.toString(minute), 2)).concat(formatString(Integer.toString(second), 2));
			
			
			if(extension.equals(".avro")){
				stream = handleAvroFiles(currFile,newFile);
			}else{
				out = handleNormalFiles(currFile,newFile);
			}
			
		} catch (Exception e) {
			logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
			throw new ConnectException(e);
		}

		logger.debug(topicName + " Exiting ADLSSinkTask::createFile");
	}
	
	public PrintStream handleNormalFiles(String currFile,String newFile) throws IOException{
		if(client.checkExists(currFile)){
			long fileSize = client.getContentSummary(currFile).spaceConsumed;
			if (fileSize < ADLSSinkConnectorConstants.FILESIZE) {
				stream = client.getAppendStream(currFile);
			}else{
				client.rename(currFile,newFile.concat(".dat"));
				stream = client.createFile(currFile, IfExists.OVERWRITE);
			}
		}else{
			stream = client.createFile(currFile, IfExists.OVERWRITE);
		}
		PrintStream returnStream = new PrintStream(stream);
		return returnStream;
	}

	public OutputStream handleAvroFiles(String currFile,String newFile) throws IOException {
		OutputStream returnStream = null;
		Schema schema = new Schema.Parser().parse(new File(schemaLoc));
		if (client.checkExists(currFile)){
			long fileSize = client.getContentSummary(currFile).spaceConsumed;
			if (fileSize < ADLSSinkConnectorConstants.FILESIZE) {
				DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
				ADLSFileSeekableInput seekableInput = new ADLSFileSeekableInput(currFile, client);
				DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(seekableInput, datumReader);			
				if (!schema.equals(dataFileReader.getSchema())) {
					client.rename(currFile,newFile.concat(".avro"));
					createAvroFile(currFile,schema);							
				}
				dataFileReader.close();
				seekableInput.close();				
			} else {
				client.rename(currFile,newFile.concat(".avro"));
				createAvroFile(currFile,schema);
			}
		}else{
			createAvroFile(currFile,schema);
		}
		returnStream = client.getAppendStream(currFile);	
		return returnStream;
	}

	public void createAvroFile(String currFile,Schema schema) throws IOException{
		stream = client.createFile(currFile, IfExists.OVERWRITE);
		DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		DataFileWriter<GenericRecord> dw = dataFileWriter.create(schema, stream);
		dw.close();
		stream.close();
		dataFileWriter.close();
	}

	public String formatString(String s, int length) {
		String formatStr = "%" + length + "s";
		return String.format(formatStr, s).replace(' ', '0');
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		logger.debug(topicName + " Entering ADLSSinkTask::flush");

		logger.debug(topicName + " Exiting ADLSSinkTask::flush");
	}

	@Override
	public void stop() {
		logger.debug(topicName + " Entering ADLSSinkTask::stop");
		try {
			if (out != null)
				out.close();
			if (stream != null)
				stream.close();
		} catch (IOException e) {
			logger.error(ADLSSinkConnectorConstants.ERROR_STRING + topicName, e);
			throw new ConnectException(e);
		}

		logger.debug(topicName + " Exiting ADLSSinkTask::stop");

	}

	public boolean checkIfEmpty(String prop, String value) throws PropertyEmptyException {
		if (value.isEmpty()) {
			throw new PropertyEmptyException(prop + "cannot be empty");
		} else
			return true;
	}

}
