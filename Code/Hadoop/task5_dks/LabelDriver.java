package task5_dks;


import task6_dks.LabelAnalysis;

public class LabelDriver {

    private static int times = 10;

    public static void main(String[] args) throws Exception{

        String[] labelBuilderArgs = {args[0], args[1] + "/data0"};
        LabelBuilder.main(labelBuilderArgs);

        String[] iterArgs = {args[0], "", ""};

        for(int i = 0; i < times; i++){
            System.out.println("Iterating round " + (i + 1) + "...");
            iterArgs[1] = args[1] + "/data" + i;
            iterArgs[2] = args[1] + "/data" + (i + 1);
            LabelIter.main(iterArgs);
        }

//        String[] analysisArgs = {args[1] + "/data" + times, "statistics"};
//        LabelAnalysis.main(analysisArgs);


    }
}
