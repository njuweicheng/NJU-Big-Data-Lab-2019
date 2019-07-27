### jar包运行方式

本项目将所有task实现组合在一起，用以下命令执行jar包：

```
hadoop jar finalLab-1.0-SNAPSHOT-jar-with-dependencies.jar /data/task2/people_name_list.txt /data/task2/novels task1_output task2_output task3_output task4_output1 task4_output2 task4_output task5_dks_output task6_dks_output task5_qy_output task6_qy_output
```

上述output目录运行前均需不存在，每个task的输出分别存储在以下目录：

- task1\_output
- task2\_output
- task3\_output
- task4\_output
- task5\_dks\_output/data10、task5\_qy\_output/15
- task6\_dks\_output、task6\_qy\_output