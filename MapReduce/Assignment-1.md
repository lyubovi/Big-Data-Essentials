# Hadoop Streaming assignment 1: Words Rating
Create your own WordCount program and process Wikipedia dump. Use the second job to sort words by quantity in the reverse order (most popular first). Output format:
```
word <tab> count
```

The result is the 7th word by popularity and its quantity.

The result on the sample dataset:
```
is  126420
```

Hint: it is possible to use exactly one reducer in the second job to obtain a totally ordered result.