import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import javax.management.monitor.Monitor;

import org.apache.kafka.clients.producer.Producer;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;


import java.util.Iterator;
 
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;




public class KafkaP implements FileAlterationListener {

	static String topicName = "";
	static Properties props ;

	public static void main(String[] args) throws Exception{
	   
	      topicName = "realtime";
	      
	      // create instance for properties to access producer configs   
	      props = new Properties();
	      
	      //Assign localhost id
	      props.put("bootstrap.servers", "localhost:9092");
	      
	      //Set acknowledgements for producer requests.      
	      props.put("acks", "all");
	      
	      //If the request fails, the producer can automatically retry,
	      props.put("retries", 0);
	      
	      //Specify buffer size in config
	      props.put("batch.size", 16384);
	      
	      //Reduce the no of requests less than 0   
	      props.put("linger.ms", 1);
	      
	      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	      props.put("buffer.memory", 33554432);
	      
	      props.put("key.serializer", 
	         "org.apache.kafka.common.serialization.StringSerializer");
	         
	      props.put("value.serializer", 
	         "org.apache.kafka.common.serialization.StringSerializer");
	      
	   
	      
	      
	      //Path myDir = Paths.get("C:/Self/Softwares/Elastic/Data/cars");
	      
	      final File directory = new File("C:/Self/Blog/RealTime_Processing_ELK/input");
	        FileAlterationObserver fao = new FileAlterationObserver(directory);
	        fao.addListener(new KafkaP());
	        final FileAlterationMonitor monitor = new FileAlterationMonitor();
	        monitor.addObserver(fao);
	        System.out.println("Starting monitor. CTRL+C to stop.");
	        monitor.start();
	      
	     
	  
	   }

	@Override
	public void onDirectoryChange(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryCreate(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileChange(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileCreate(File arg0) {
		// TODO Auto-generated method stub
		
		 File folder = new File("C:/Self/Blog/RealTime_Processing_ELK/input");
	      File[] listOfFiles = folder.listFiles();
	      
	      
	          BufferedReader br = null;
	          String line = "";
	          String cvsSplitBy = ",";
	          String excelLineString = "";
	          String excelLineString1 = "";
	          String excelLineString2 = null;
	          String excelLineString3 = null;
	          Boolean excelLineBoolean = null;
	          Integer excelLineInt = null;
	          Workbook workbook = null;
	        File moveFile = null; 
	      //Parser parser = new AutoDetectParser();
	     
                     for (int i = 0; i < listOfFiles.length; i++) {
           	          if (listOfFiles[i].isFile()) {
           	        	  
           	        	   Producer<String, String> producer = new KafkaProducer
           	        		         <String, String>(props);
           	        	  
           	        	  String fileNameWithPath = folder+"/"+listOfFiles[i].getName();
           	        	 
           	        	
           	        	  
           	        	  
           	        	String[] ext=fileNameWithPath.split("\\.");
           	        	
           	        if(ext[1].equalsIgnoreCase("csv")) 
           			  {
           	        	    
           	        	 /* long start = System.currentTimeMillis();
           	    		  BodyContentHandler handler = new BodyContentHandler(10000000);
           	    		  Metadata metadata = new Metadata();
           	    		  	      
           	    		  FileInputStream content = null;*/
         	    		  
           	    		try {
							br = new BufferedReader(new FileReader(fileNameWithPath));
						} catch (FileNotFoundException e3) {
							// TODO Auto-generated catch block
							e3.printStackTrace();
						}
           	            try {
							while ((line = br.readLine()) != null) {

								System.out.println(line);
							    	  producer.send(new ProducerRecord<String, String>(topicName, 
							    			  line.toString()));
							    	  

							    	}
								    
						} catch (IOException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
           	         
           	            
           	            
						/*try {
							content = new FileInputStream(folder+"/"+listOfFiles[i].getName());
						} catch (FileNotFoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
           	    		  try {
							parser.parse(content, handler, metadata, new ParseContext());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (SAXException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TikaException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
           	    		  
           	    		  //Passing Metadata to Consumer
           	    		  for (String name : metadata.names()) {
           	    		        System.out.println(name + ":\t" + metadata.get(name));
           	    		        producer.send(new ProducerRecord<String, String>(topicName, 
           	    		        		metadata.get(name), metadata.get(name)));
           	    		  }
           	    		  
           	    		  //Passing FileContent to Consumer
           	    		  producer.send(new ProducerRecord<String, String>(topicName, 
           	    				  handler.toString(), handler.toString())); */
           	    		  
           	    		  
           	    		  producer.close(); 
           	    		  
           	    		
           	    		//moveFile = folder+"/"+listOfFiles[i];
           	    		
           	    		System.out.println("garg");
           	    		
           	    		
           	    		listOfFiles[i].delete();
           	    		
           	    		//moveFile.renameTo(new File("C:/Self/Softwares/Elastic/Data/archive" + "/" + listOfFiles[i].getName()));
           	    		
           			  }
           	        
           	        else if (ext[1].equalsIgnoreCase("xls") || ext[1].equalsIgnoreCase("xlsx") || ext[1].equalsIgnoreCase("xlsm"))
           	        {
           	        	excelLineString1 = ",";
           	        	FileInputStream inputStream = null;
           	        	Workbook wb = null;
           	        	try {
							inputStream = new FileInputStream(new File(fileNameWithPath.toString()));
						} catch (FileNotFoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
           	        	try {
							 wb = new XSSFWorkbook(inputStream);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
           	        	DataFormatter formatter = new DataFormatter();
           	        	Sheet sheet = wb.getSheetAt(0);
           	        	
           	        	//for (Sheet sheet : wb.geta) {
           	        	    for (Row row : sheet) {
           	        	        boolean firstCell = true;
           	        	        for (Cell cell : row) {
           	        	            if ( ! firstCell ) excelLineString = excelLineString + excelLineString1;
           	        	         excelLineString  = excelLineString + cell;
           	        	            firstCell = false;
           	        	        }
           	        	     excelLineString = excelLineString + ",";
           	        	    }
           	        	 
           	        	 System.out.println(excelLineString);
       	        	     producer.send(new ProducerRecord<String, String>(topicName, 
            	            		excelLineString.toString(), excelLineString.toString()));
           	        	//}
           	        	
           	        	
           	        	/*System.out.println("1");
           	        	FileInputStream inputStream = null;
						try {
							inputStream = new FileInputStream(new File(fileNameWithPath.toString()));
						} catch (FileNotFoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
           	         
						System.out.println("2");
						try {
							workbook = new XSSFWorkbook(inputStream);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
           	        	Sheet firstSheet = workbook.getSheetAt(0);
           	        	Iterator<Row> iterator = firstSheet.iterator();
           	        	System.out.println("3");
           	        	
           	        	
           	        	while (iterator.hasNext()) {
           	        		
           	        		
           	             Row nextRow = iterator.next();
           	             Iterator<Cell> cellIterator = nextRow.cellIterator();
           	          System.out.println("4");  
           	             while (cellIterator.hasNext()) {
           	                 Cell cell = cellIterator.next();
           	              System.out.println("5");
           	                 switch (cell.getCellType()) {
           	                     case Cell.CELL_TYPE_STRING:
           	                    	excelLineString = cell.getStringCellValue();
           	                    	System.out.println("6");
           	                         break;
           	                     case Cell.CELL_TYPE_BOOLEAN:
           	                    	excelLineString = Boolean.toString(cell.getBooleanCellValue());
           	                    	System.out.println("7");
           	                         break;
           	                     case Cell.CELL_TYPE_NUMERIC:
           	                    	excelLineString = String.valueOf(cell.getNumericCellValue());
           	                    	System.out.println("8");
           	                         break;
           	                         
           	                 }
           	             // System.out.println(excelLineString);
           	              
           	           excelLineString = excelLineString + excelLineString1 + excelLineString2 + excelLineString3 + ",";
           	              producer.send(new ProducerRecord<String, String>(topicName, 
           	            		excelLineString.toString(), excelLineString.toString()));
           	             }*/
           	             
           	       //System.out.println(excelLineString);
           	    //producer.send(new ProducerRecord<String, String>(topicName, 
 	            		//excelLineString, excelLineString));
           	             
           	           
            	     
           	         }
           	          
           	         
           	        	
           	        	
           	        	
           	        	
           	        
           	    		           
           	          } 
           	          System.out.println("Message sent successfully");
           	        }
                 
           	     
		
	}

	@Override
	public void onFileDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStart(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStop(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}

}
	
