# Hadoop Streaming assignment 4: Word Groups
Calculate statistics for groups of words which are equal up to permutations of letters. For example, ‘emit’, ‘item’ and ‘time’ are the same words up to a permutation of letters. Determine such groups of words and sum all their counts. Apply stop words filter. Filter out groups that consist of only one word.

Output: count of occurrences for the group of words, number of unique words in the group, comma-separated list of the words in the group in lexicographical order:
```
sum <tab> group size <tab> word1,word2,...
```
Example: assume ‘emit’ occurred 3 times, 'item' -- 2 times, 'time' -- 5 times; 3 + 2 + 5 = 10, group contains 3 words, so for this group result is:
```
10 3 emit,item,time
```
The result of the task is the output line with word ‘english’.

The result on the sample dataset:
```
7823    eghilns 5   english,helsing,hesling,shengli,shingle
```