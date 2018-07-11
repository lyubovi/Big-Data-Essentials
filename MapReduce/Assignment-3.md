# Hadoop Streaming assignment 3: Name Count
Make WordCount program for all the names in the dataset. Name is a word with the following properties:

- The first character is not a digit (other characters can be digits).   
- The first character is uppercase, all the other characters that are letters are lowercase.  
- There are less than 0.5% occurrences of this word, when this word regardless to its case appears in the dataset and the condition (2) is not met. 
- Order by quantity, most popular first, output format:
```
name <tab> count
```
The result is the 5th line in the output.

The result on the sample dataset:
```
french 5742
```
