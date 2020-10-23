# Flink

- Flink on Yarn

  - Task任务执行的时候才会消耗资源，执行任务完毕，自动释放资源
  - 命令
    - yarn-session -tm 800 -jm 800 -s 2 -d

- Yarn-cluster

  - 命令

    - flink run -m yarn-cluster -ytm 800 -yjm 800 -ys 2 *.jar

    

- 优化
  - 分区自动感知，增加分区，自动感知增加的分区

