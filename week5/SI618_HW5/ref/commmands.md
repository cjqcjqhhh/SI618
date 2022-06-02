```bash
# connect to server
ssh junqich@cavium-thunderx.arc-ts.umich.edu
```

```python
# random text file
["I am a boy", "hello world", "I love China", "How are you"]
```

```
# upload
scp -r D:\CScode\UMCode\si618\week5\SI618_HW5\spark_total_reviews_per_category.py junqich@cavium-thunderx.arc-ts.umich.edu:/home/junqich/si618_HW5
```

```
# download
scp junqich@cavium-thunderx.arc-ts.umich.edu:/home/junqich/si618_HW5/total_reviews_per_category_output.tsv D:\CScode\UMCode\si618\week5\SI618_HW5
```



```
# format
Atlanta # city
Event Planning & Services # category

944 # business, 1 for each
3.643008475 # stars
241 # "WheelchairAccessible"
344 # Ture for parking
```



