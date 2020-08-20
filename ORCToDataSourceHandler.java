package com.alation.lambda.s3.schemas;

import static com.alation.lambda.s3.util.ColumnUtils.getColumnType;
import static com.alation.lambda.s3.util.ColumnUtils.getFileNameFromKey;
import static com.alation.lambda.s3.util.ColumnUtils.getSchemaNameFromBucket;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.json.simple.JSONObject;

import com.alation.lambda.s3.Constants;
import com.alation.lambda.s3.StorageManager;
import com.alation.lambda.s3.model.AlationConfig;
import com.amazonaws.services.s3.model.S3Object;

public class ORCToDataSourceHandler {

	private String dataSourceId;
	private StorageManager jobStorageManager;
	public static Logger logger = Logger.getLogger(ORCToDataSourceHandler.class);

	/**
	 * Method to convert ORC data to Alation data source
	 * 
	 * @param schema
	 * @param object
	 * @param alationConfig
	 * @param schemaName
	 * @return
	 */
	public String convertToDataSource(TypeDescription schema, S3Object object, AlationConfig alationConfig,
			String schemaName) {
		String buffer = "";
		try {
			dataSourceId = alationConfig.getDataSourceId();
			String key = object.getKey();
			// Populate the schema name only if is not provided
			if (schemaName == null) {
				schemaName = getSchemaNameFromBucket(object.getBucketName(), key,alationConfig.getSchemaFolderPath(),true);
			}
			buffer = buildTable(schema, schemaName, getFileNameFromKey(object.getKey(),alationConfig.getSchemaFolderPath(),true,object.getBucketName()), object,alationConfig);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);

		}
		return buffer;
	}

	/**
	 * Build schema and table objects that is used to post the data to Alation
	 *
	 * @param schema
	 * @param schemaName
	 * @param tableName
	 * @param fileName
	 * @return
	 */
	private String buildTable(TypeDescription schema, String schemaName, String tableName,  S3Object object,AlationConfig alationConfig) {
		StringBuffer buff = new StringBuffer();
		SimpleDateFormat SDF = new SimpleDateFormat("YYYY-MM-dd H:m:s");
		try {
			String	fileName=object.getKey();
			
			
			
			if((!StringUtils.isEmpty(alationConfig.getSchemaFolderPath())) && ((object.getBucketName()+"/"+fileName).contains(alationConfig.getSchemaFolderPath()))) {
				
			 
	        String currentFilelastModified=SDF.format(object.getObjectMetadata().getLastModified());
	      
	        if(alationConfig.getTableVslastModifiedDateMap().get(tableName)==null) {
		    alationConfig.getTableVslastModifiedDateMap().put(tableName,currentFilelastModified );
		   
		   
	        }
	        else
	        {
	        	String existingFileLastModified=alationConfig.getTableVslastModifiedDateMap().get(tableName);
	        	 Date currentFileDate = SDF.parse(currentFilelastModified);
	             Date existingFileDate = SDF.parse(existingFileLastModified);
	            		         
	             if(currentFileDate.compareTo(existingFileDate)<=0) {

	            	 return buff.toString();
	             }
	             else {
	            	 alationConfig.getTableVslastModifiedDateMap().put(tableName,currentFilelastModified );

	             }
	 
	        }
	       

	      
			}
			JSONObject jsonObject = new JSONObject();

			// Create the Schema
			jsonObject.put("key", dataSourceId + ".\"" + schemaName + "\"");
			jsonObject.put(Constants.TABLE_TYPE, Constants.SCHEMA);
			buff.append(jsonObject.toJSONString() + System.lineSeparator()); // Schema level

			// Create the TABLE
			jsonObject.put("key", dataSourceId + ".\"" + schemaName + "\".\"" + tableName + "\"");
			jsonObject.put(Constants.TABLE_TYPE, Constants.TABLE);
			jsonObject.put(Constants.DESCRIPTION, fileName);

			buff.append(jsonObject.toJSONString() + System.lineSeparator()); // Table level
			buff.append(buildColumns(schema, schemaName, tableName)); // Fields
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return buff.toString();
	}

	/**
	 * Method to build columns from schema in key and data type format
	 *
	 * @param schema
	 * @param schemaName
	 * @param tableName
	 * @return
	 */
	private String buildColumns(TypeDescription schema, String schemaName, String tableName) {
		StringBuffer stringBuffer = new StringBuffer();
		List<TypeDescription> parquetColumns = schema.getChildren();
		List<String> fieldNames = schema.getFieldNames();

		for (int i = 0; i < fieldNames.size(); i++) {
			JSONObject jsonObject = new JSONObject();
			String columnName = fieldNames.get(i);
			String dataType = parquetColumns.get(i).getCategory().getName();
			String dataLength = "";
			if (dataType.equalsIgnoreCase("int96")) {
				dataType = "datetime";
			} else if (dataType.equalsIgnoreCase("binary")) {
				dataType = "string";
			}
			jsonObject.put("key", dataSourceId + ".\"" + schemaName + "\".\"" + tableName + "\"" + "." + columnName);
			jsonObject.put(Constants.COLUMN_TYPE, getColumnType(dataType, dataLength));
			stringBuffer.append(jsonObject.toJSONString());
			stringBuffer.append(System.lineSeparator());
		}
		return stringBuffer.toString();
	}

	public String getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public StorageManager getJobStorageManager() {
		return jobStorageManager;
	}

	public void setJobStorageManager(StorageManager jobStorageManager) {
		this.jobStorageManager = jobStorageManager;
	}

}
