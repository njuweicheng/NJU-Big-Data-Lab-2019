package task4;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class PageRank {
    private static FileSystem fileSystem = null;

    public static List<String> getNameRank(String filePath) throws Exception{
        List<String> ret = new ArrayList<String>();

        Map<String, Float> map = new HashMap<String, Float>();
        BufferedReader bf = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(filePath))));
        String line;
        while (null!=(line = bf.readLine())){
            String[] lineInfo = line.split("\t");
            float val = Float.parseFloat(lineInfo[1].split("#")[0]);
            map.put(lineInfo[0], val);
        }

        List<Map.Entry<String,Float>> list = new ArrayList<Map.Entry<String,Float>>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
            public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                int mainSort = o1.getValue().compareTo(o2.getValue());
                if (mainSort!=0){
                    return mainSort;
                }
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        for (Map.Entry<String,Float> token : list){
            ret.add(token.getKey());
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("\u001b[31mError parameters!\u001b[0m");
            System.exit(-1);
        }

        GraphBuilder.main(args[0], args[1]);


        String inPath = args[1];
        String outPath = args[2];
        String temp = null;

        fileSystem = FileSystem.get(new Configuration());

        List<String> preList = getNameRank(inPath+"/part-r-00000");
        List<String> nowList = null;

        int iterCount = 0;
        while (true){
            if (fileSystem.exists(new Path(outPath))){
                fileSystem.delete(new Path(outPath), true);
            }

            PageRankIter.main(inPath, outPath);
            iterCount ++;

            nowList = getNameRank(outPath+"/part-r-00000");
            if(nowList.equals(preList)){
                break;  // rank remains
            }
            preList.clear();
            preList = nowList;

            // swap input path with output path
            temp = inPath;
            inPath = outPath;
            outPath = temp;
        }
        System.out.println("\u001b[31mPageRank Iter: "+iterCount+"\u001b[0m");

        /*
        final int iterNum = 5;
        for(int i=0;i<iterNum;i++){
            if (fileSystem.exists(new Path(outPath))){
                fileSystem.delete(new Path(outPath), true);
            }

            PageRankIter.main(inPath, outPath);

            // swap input path with output path
            temp = inPath;
            inPath = outPath;
            outPath = temp;
        }
        System.out.println("\u001b[31mPageRank Iter: "+iterNum+"\u001b[0m");

         */

        outPath = args[3];
        RankViewer.main(inPath, outPath);
    }
}
