# Hadoop Streaming assignment 3: Stop Words
Improve the previous program to calculate how many stop words are in the input dataset. Stop words list is in ‘/datasets/stop_words_en.txt’ file. Use Hadoop counter to count the number of stop words and total words in the dataset. The result is the percentage of stop words in the entire dataset (without percent symbol).

The result on the sample dataset:
```
41.603
```

**Hint.** As you can see in the Hadoop Streaming userguide you will need to use "-file" option to tell the framework to pack your executable files as a part of job submission.". In general you can attach to the job not only executable files and then access them within your mappers and reducers as if were located in the same directory.

For example if you've attached such files to the job:
```
...
-files mapper.py,reducer.py,/dir1/file1.txt,/dir2/file2 \
...
```

you can works with attached files using relative paths:
```
# mapper.py

with open("file1.txt") as f1, open("file2") as f2:
 ...
```


Please pay attention that the following code:
```
# mapper.py

with open("/dir1/file1.txt") as f1, open("/dir2/file2") as f2:
 ...
``` 

will work within Jupyter or Docker container because it has a single node which is simultaneously client node, datanode and namenode. However the code with absolute paths will fail on a real multi-node Hadoop cluster because "/dir1" and "/dir2" doesn't exist on the datanodes.

