# Spark assignment 2: Collocations
As for the second part of the assignment, your task is to extract collocations: that is word combinations that occur together. For example, “high school” or “roman empire”.

To find collocations, you will use NPMI (normalized pointwise mutual information) metric.

PMI of two words, a & b, is defined as “PMI(a, b) = ln (P(ab) / (P(a) * P(b))”, where P(ab) is the probability of two words coming one after the other, and P(a) and P(b) are probabilities of words a & b respectively.

You will estimate probabilities with occurrence counts, that is “P(a) = # of occurrences of word a / total number of words”, and “P(ab) = # of occurrences of words ‘a b’ / total number of word pairs”.

To build an intuition behind the definition, see Reading material.

Therefore, rare combinations of coupled words have large PMI.

NPMI is computed as “NPMI(a, b) = PMI(a, b) / -ln P(ab)”. This normalizes the quantity to be within the range [-1; 1].

You task is a bit more complicated now:

- Extract all the words, as in the previous task.  
- Filter out stopwords using the dictionary (/datasets/stop_words_en.txt ) (do not forget to convert words to the lowercase!)
- Compute all bigrams (that is, pairs of consequent words)
- Leave only bigrams with at least 500 occurrences
- Compute NPMI for every bigram (note: when computing probabilities, you need unpruned counts!)
- Sort word pairs by NPMI in the descending order
- Print top 39 word pairs, with words delimited by the underscore “_”

For example,

>*roman_empire*  
>*south_africa*

The part of the result on the sample dataset:

```
...
references_reading
notes_references
award_best
north_america
new_zealand
...
```
Hint: if you did everything right, *“roman_empire”* and *“south_africa”* are going to be in the result.