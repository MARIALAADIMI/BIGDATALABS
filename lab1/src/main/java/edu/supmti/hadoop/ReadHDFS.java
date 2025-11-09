package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        
        if (args.length != 1) {
            System.out.println("Usage: hadoop jar <jar_file> <hdfs_file_path>");
            System.out.println("Example: hadoop jar ReadHDFS.jar /user/root/test_write.txt");
            return;
        }

        String hdfsFilePath = args[0];
        Configuration conf = new Configuration(); 
        FileSystem fs = null;
        FSDataInputStream inStream = null;
        
        try {
            fs = FileSystem.get(conf);
            Path nomcomplet = new Path(hdfsFilePath); 

            
            if (!fs.exists(nomcomplet)) {
                System.out.println("File " + nomcomplet.toString() + " does not exist on HDFS.");
                return;
            }

            inStream = fs.open(nomcomplet); 
            
            System.out.println("--- Content of file: " + hdfsFilePath + " ---");

     
            
           
            System.out.println("Contenu 1: " + inStream.readUTF()); 
            
      
            System.out.println("Contenu 2: " + inStream.readUTF()); 
            
            System.out.println("--- End of file ---");

        } catch (IOException e) {
            
            System.err.println(" Error reading data (Check if writeUTF was used): " + e.getMessage());
        } finally {
            try {
                if (inStream != null) inStream.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                 e.printStackTrace();
            }
        }
    }
}