import pandas as pd
import numpy as np

# read in batch result
df_raw = pd.read_csv("junqich_zhaoss_si618_hw7_batch_result.csv")
df = df_raw[["Input.comment_text", "Answer.target1.yes", "Answer.target2.no", "Answer.target3.na", "Answer.toxic1.yes", "Answer.toxic2.no"]]
df.head()

# compute the fraction of comments that were labeled as toxic
df_group = df.groupby("Input.comment_text").sum()
toxic_1 = len(df_group[df_group["Answer.toxic1.yes"] >= 1]) / len(df_group) # at least once
toxic_2 = len(df_group[df_group["Answer.toxic1.yes"] >= 2]) / len(df_group) # at least 2
toxic_3 = len(df_group[df_group["Answer.toxic1.yes"] >= 3]) / len(df_group) # at least 3

# compute the fraction of toxic comments that are also labeled as targeted at least once
df_toxic = df_group[df_group["Answer.toxic1.yes"] >= 2]
targeted = len(df_toxic[df_toxic["Answer.target1.yes"] >= 1]) / len(df_toxic)

# save to txt file
with open("junqich_zhaoss_si618_hw7_compute.txt", "w") as file:
    file.write(f"toxic_1\t{toxic_1}\n")
    file.write(f"toxic_2\t{toxic_2}\n")
    file.write(f"toxic_3\t{toxic_3}\n")
    file.write(f"targeted\t{targeted}\n")