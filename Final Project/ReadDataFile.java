import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.*;
import java.io.IOException;
import java.io.File;

public class ReadDataFile {

    private static int batchSize=10;
    private static int fileLength=300561;
    private static File file;
    public static void main(String args[]) throws IOException {
        File file  = new File("file.txt");
        BufferedReader br = new BufferedReader(new FileReader("dockless-vehicles-9.csv"));
        for (int j= 0; j < fileLength/batchSize; j++) {
            readBatch(br,batchSize, file);  
            try{
            Thread.sleep(10000);
            }catch(Exception e){
            e.printStackTrace();
            }
        }
    }

    public static void readBatch(BufferedReader reader, int batchSize, File file) throws IOException {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            String line = reader.readLine();
            if (line != null) {
                System.out.println(line);
                result.add(line);
            }
        }


        FileWriter fw = new FileWriter(file,false);
        BufferedWriter bw = new BufferedWriter(fw);
        for (String line : result) {
            bw.write(line+"\n");
            
        }
        bw.close();
    }
}