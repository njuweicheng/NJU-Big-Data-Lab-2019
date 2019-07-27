package task5_qy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class LpaDriver {

    //Driver of LPA
    public static int mincount = 15; //minest time

    public static Map<String,String> getLastResult(String filePath) throws Exception{
        Map<String,String> keyLabel = new HashMap<String,String>();
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(filePath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length;i++){
            if(!fileList[i].isDirectory())
            {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
                while((line = in.readLine())!=null){
                    String []temp = line.split("\t");
                    keyLabel.put(temp[0],temp[1]);//end
                }
            }
        }
        in.close();
        fsi.close();
        return keyLabel;
    }

    public static boolean isEqual(Map<String,String> a,Map<String,String> b){
        for(String temp: a.keySet())
        {
           if(a.get(temp).equals(b.get(temp)) == false){
               return false;
           }
        }
        return true;
    }

    public static void main(String[] args) throws Exception { //firtst arg is Graph , second is folder
        String GraphLocation = args[0]; //will not change
        String targetFolder = args[1];  //target-root
        //initialize
        String initialize_name = targetFolder + "/" + 0;
        String[] args_init = {GraphLocation, initialize_name};
        LabelBuild.main(args_init);

        //non - iter
        Map<String, String> CUR = new HashMap<String, String>();  //last result
        Map<String, String> newRes = new HashMap<String, String>();// new Result
        boolean flag = false;
        int count = 0;

        while(!flag){
            String [] args_iteration = {"","",""}; //input , last result /output
            args_iteration[0] = GraphLocation;
            args_iteration[1] = targetFolder+"/"+count;
            args_iteration[2] = targetFolder+"/"+String.valueOf(count+1);
            LpaIter.main(args_iteration);
            //determine
            CUR = getLastResult(args_iteration[1]);
            newRes = getLastResult(args_iteration[2]);
            flag = isEqual(CUR,newRes);
            count++;
            if(count >= 15){
                break;
            }
        }
        //end

//iteration
//        for(int i = 0; i < mincount; i++){
//            String [] args_iteration = {"","",""}; //input , last result /output
//            args_iteration[0] = GraphLocation;
//            args_iteration[1] = targetFolder+"/"+i;
//            args_iteration[2] = targetFolder+"/"+String.valueOf(i+1);
//            LpaIter.main(args_iteration);
//        }
    }

}