# Sentiment Analysis using Hadoop

* This program performs sentiment analysis on large Yelp datasets of customer reviews from on kaggle using MapReduce and the Hadoop Distributed System.
* The data is processed as a long string and then tokenized to obtain for each record the Business ID, Review ID, Review text, and Star rating of the yelp reviews dataset.
* Phase 1:
- Mapper: map the Review text using Review ID as key.
- Reducer: perform sentiment analysis on the array of Review text for each business, then output the predicted star rating for each review ID, actual star rating and business ID.
* Phase 2:
- Mapper: map the predicted star rating, actual star rating to business ID.
- Reducer: for each review, calculate the difference between predicted star rating and actual star rating then average them for each business ID, output them as business ID, starDiff.
* Phase 3:
- Mapper: Map all the starDiff to a reducer.
- Reducer: calculate the average of all the starDiff of the business ID.
* The program checks the accuracy of the star rating of a given restaurant by going through the written reviews and calculating the sentiment of each business and giving out a predicted star rating. Then, it calculates the average star difference for each business. Finally, calculate the difference for each business. This will make it so business with only few reviews are taken into account more for the difference.
