package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        
        
        if (args.length != 2) { 
            System.out.println("Usage: hadoop jar <jar_file> <hdfs_file_path> <content_to_write>");
            System.out.println("Example: hadoop jar WriteHDFS.jar /user/root/test.txt 'text'");
            return;
        }

      
        String hdfsFilePath = args[0];
        String contentToWrite = args[1];

        
        Configuration conf = new Configuration(); 
        FileSystem fs = null;
        FSDataOutputStream outStream = null;

        try {
            fs = FileSystem.get(conf);
            Path nomcomplet = new Path(hdfsFilePath);

            
            if (!fs.exists(nomcomplet)) { 
                
               
                outStream = fs.create(nomcomplet);
                
            
                outStream.writeUTF("Bonjour tout le monde ! "); 
                
               
                outStream.writeUTF(contentToWrite); 
                
                System.out.println("\n File created successfully and data written to HDFS at: " + hdfsFilePath);
                
            } else {
                 System.out.println("\n File already exists at: " + hdfsFilePath + ". Skipping creation.");
            }

        } catch (IOException e) {
            System.err.println(" An error occurred during HDFS operation: " + e.getMessage());
            e.printStackTrace();
        } finally {
           
            if (outStream != null) {
                outStream.close(); 
            }
            if (fs != null) {
                fs.close();
            }
        }
    }
}