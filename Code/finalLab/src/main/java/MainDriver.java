import task1.*;
import task2.*;
import task3.*;
import task4.*;
import task5_dks.*;
import task5_qy.*;
import task6_dks.*;
import task6_qy.*;

public class MainDriver {
    public static void main(String[] args) throws Exception{
        if(args.length!=12){
            System.out.println("\u001b[31mError parameters!\u001b[0m");
            System.exit(-1);
        }

        // Task 1
        String[] task1_para = {args[0], args[1], args[2]};
        Preprocessing.main(task1_para);

        // Task 2
        String[] task2_para = {args[2], args[3]};
        FinalLab_task2.main(task2_para);

        // Task 3
        String[] task3_para = {args[3], args[4]};
        BuildGraph.main(task3_para);

        // Task 4
        String[] task4_para = {args[4], args[5], args[6], args[7]};
        PageRank.main(task4_para);

        // Task 5 dks
        String[] task5_dks_para = {args[4], args[8]};
        LabelDriver.main(task5_dks_para);

        // Task 6 dks
        String[] task6_dks_para = {args[8]+ "/data10", args[9]};
        LabelAnalysis.main(task6_dks_para);

        // Task 5 qy
        String[] task5_qy_para = {args[4], args[10]};
        LpaDriver.main(task5_qy_para);

        // Task 6 qy
        String[] task6_qy_para = {args[10]+ "/15", args[11]};
        qy_analysis.main(task6_qy_para);
    }
}
