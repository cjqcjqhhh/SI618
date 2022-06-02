```bash
# connect to server
ssh junqich@cavium-thunderx.arc-ts.umich.edu
```



```shell
# upload
scp -r D:\CScode\UMCode\si618\project\data\* junqich@cavium-thunderx.arc-ts.umich.edu:/home/junqich/si618_project
```

```shell
# run the code
spark-submit --master yarn --queue umsi618f21 --num-executors 16 --executor-memory 1g --executor-cores 2 junqich_si618_lab6.py
```

```shell
# cat result
hadoop fs -getmerge junqich_si618_project_q1 junqich_si618_project_q1.csv
hadoop fs -getmerge junqich_si618_project_q1-1 junqich_si618_project_q1-1.csv
hadoop fs -getmerge junqich_si618_project_q1-2 junqich_si618_project_q1-2.csv
hadoop fs -getmerge junqich_si618_project_q2 junqich_si618_project_q2.csv
hadoop fs -getmerge junqich_si618_project_q2-1 junqich_si618_project_q2-1.csv
hadoop fs -getmerge junqich_si618_project_q2-2 junqich_si618_project_q2-2.csv
hadoop fs -getmerge junqich_si618_project_q3-1 junqich_si618_project_q3-1.csv
hadoop fs -getmerge junqich_si618_project_q3-2 junqich_si618_project_q3-2.csv
```

```shell
# download
scp junqich@cavium-thunderx.arc-ts.umich.edu:/home/junqich/si618_project/result/* D:\CScode\UMCode\si618\project\result\
```



```bash
# upload file to hadoop
hadoop fs -put covid.csv covid.csv
hadoop fs -put country_vaccinations.csv country_vaccinations.csv
hadoop fs -put country_vaccinations_by_manufacturer.csv country_vaccinations_by_manufacturer.csv
hadoop fs -put population_by_country_2020.csv population_by_country_2020.csv
```

```bash
# download file from hadoop
# DO NOT USE THIS!!!!!
hadoop fs -get junqich_si618_project_q1-1.csv junqich_si618_project_q1-1.csv
```

