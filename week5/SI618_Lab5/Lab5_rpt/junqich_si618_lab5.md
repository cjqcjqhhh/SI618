

# SI618: Lab5

Name: Junqi Chen

Uniqname: junqich

Date: Oct. 3rd, 2021

---

+ **Question 1**

  ![9115b8358eb963f49315c380ea39a79](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\9115b8358eb963f49315c380ea39a79.png)

  Two files called `ngram-job.py` and `spark-run.sh` are in the directory.

+ **Question 2**

  ![48b54647416c35f0f73eee1a252c6e9](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\48b54647416c35f0f73eee1a252c6e9.png)

  The name of the last file in the listing for HFS folder `/var/umsi618f21/lab5/ngrams/data/` is `googlebooks-eng-all-1gram-20120701-z`.

+ **Question 3**

  ![a778b6cfbd0a246fe704a0ab8a1be16](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\a778b6cfbd0a246fe704a0ab8a1be16.png)

  It was in the year **1505** that “information” first mentioned (as a noun) in Google Books data.

+ **Question 4**

  ![6d58484710e5199e1c2337cb98561e1](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\6d58484710e5199e1c2337cb98561e1.png)
  
  The top two files are `_SUCCESS` and `part-00000` as shown above.
  
+ **Question 5**

  ![9d3abc123fcc97e08c45ffa21611c9a](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\9d3abc123fcc97e08c45ffa21611c9a.png)

  The average word lengths of words starting with x observed in books from the years 1810, 1946, and 2002 are **5.388058732511713, 4.911973636754396, 4.959304448361536.**









Useful Commands and results:

```
# upload
scp -r D:\CScode\UMCode\si618\week5\SI618_Lab5\si618CaviumSetup junqich@cavium-thunderx.arc-ts.umich.edu:/home/junqich
```

```
./spark-run.sh ngram-job.py /var/umsi618f21/lab5/ngrams/data/googlebooks-eng-all-1gram-20120701-x ./ngrams-out
```

![04f0b3d4e9e2ec93fbb2da963cdf95a](D:\CScode\UMCode\si618\week5\SI618_Lab5\Lab5_rpt\04f0b3d4e9e2ec93fbb2da963cdf95a.png)

```
# download
scp uniqname@cavium-thunderx.arc-ts.umich.edu:remotefile localfile
```

