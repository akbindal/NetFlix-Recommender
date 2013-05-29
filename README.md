NetFlix-Recommender
===================

This reco system implements uv decomposition algorithm

UV Decomposition Algorithm:
The utility matrix M, usually has lot of blank entries. One of the approach for estimating the missing entries is through UV decomposition algorithm [1]. This algorithm works on the speculation that there are D features which are common basis for both X and Y, where X and Y are the two types of entity sets represented along rows and column of the utility matrix M. 
Instead of directly looking at the utility of Xi with Yj, feature vectors of Xi and Yj are evaluated which are close to the corresponding utility Mij. UV algorithm applies the Iterative approach to find these feature vectors which can be further used to estimate the blank entries of M. One approach to the problem is to start with random matrices U and V. We converge to near optimal solution (not guaranteed) by
alternatively optimizing one matrix and another as fixed matrix. The convergence criteria which is
used in the implementation is RMSE (root mean square root error). This algorithm was actually
developed in the context of the Netflix Prize where it proved to be a very efficient technique [1].

The different stages of the algorithm are briefly described as follows:

1. Pre-processing: Normalization of the utility matrix M.
2. Initialization: Random initialization of the feature values in U and V matrices.
3. Iterative updation of U and V: update the elements (feature values) of U or V such that RMSE is minimized.

**Task**
We are provided with NetFlix dataset which have users, movies and the user rating. We are also
given with the number of features to choose for U and V i.e. 10. wWe need to submit the optimized
Feature vectors of U (users) and V (movies).

- Dataset Format: <userId, movieId, rating, date>
- Return Feature Vector Values

  * U: (U, userId, feature Index[1..10], feature value)
  * V: (V, feature_index[1..10], movieId, value)
  
- Number of users = #users
- Number of movies = #movies
- Number of Maps = #maps
- Number of Reducer = #reducers

**Parallelization Design and Implementation of the Algorithm**
The parallelization of the algorithm is need to be achieved through map and reduce model which is used in the functional programming. In the following part, we will look at the implementation of above mentioned stages of the UV Decomposition algorithm. Here we defines two operators which denotes the Map and Reduce model (map is always followed by reduce or in other words, output of map is input to the reduce).

Map1: (key, value) --> (key, value) OR Map2: (Text) --> (key, (value))
Reduce: (key, (value)*) --> (key, value)
Where * means the list.

**Preprocessing:** we have to normalize the user rating by the average of his all the rating. This was essentially to bring each user rating on the same scale and to remove the user behaviour bias.

Input: (userId, movieId, rating, date> (dataset)
Output: (userId, movieId, Normalized_rating)
  
* Map: (userId, movieId, rating, date) --> (userid, <movieId, rating))

It simply reads the each line form the dataset file and returns the userid as key and movieId
and ratings as value of custom type IntFloatPair;

* Reduce: (userId, (movieId, rating)*) --> (userId, (movieId, Normalized Ratings))

Here we have all the ratings given by the user. By taking average of the rating from the list of
values and further returns the userId as key and pair of MovieId and (rating-avg_rating) as
value.

* Parallelization: Each userId can be processes in parallel.
  
**Initialization:** we have to randomly initialize the U and V matrices. We need to simply generate the initial value and write down into the file for later use. For parallelizing this process, single map is used to return number (from 1 to #Reducers) as key which is then assigned to different reducers (due to hash partitioning). Then each reducer has one number k as key and it will generate the number of feature vectors of dimension D for IDs (movie or users) from:

  k*split_size to (k+1)*split_size
  where, split_size = (matrix_size/#Reducers)
  
* Map: () --> k, ())
* UReducer: (k, ()') --> (U, user, fi, fv) and VReducer(k, ()*) --> (V, fi, movieId, fv) {both
reducer have same map and fi=feature index, fv= feature value}
Initializing value: Here reducer generates random value fv for each fi, where - c < fv < c and the
value of c is deviation and ranges upto 0.1. We chosed this because average of the user ratings
after the normalization is zero then it will give closer value to the existing user ratings and
deviation for different starting points for exploring the search space.
To converge more quickly:: chose the feature at random, not in sequential order of Feature
index. This strategy is possible since we have sequential userIds and movieIds without exception.

**Iterative updation of U and V:** In this stage, we need to update the element of one matrix assuming the other matrix is fixed. Here we can implement different strategy for the parallelization but each has its own consequences. The most suitable strategy (and this is implemented) for the given dataset and cluster nodes (memory size) is described below along with other strategies. Here we are taking the case of Updating the Matrix U, for V it is symmetric. Suppose x some
arbitrary element in U matrix say Urs.

Take the Join of U and M, Read the value of V in the reducer while jobtask configure the reducer. Since we are not updating the V, therefore all the keys can access this common object in the memory. Moreover Size of Matrices V and U are not large, it is around 40 -60 MB.
Map1 (U, user, fi, fv) --> (user, <U, fi, fv))
Map2 (userid, movieid, normalized_rat) --> (userid, <M, movieid, rat)*)
Reducer(user, (U, fi, fv)/(M, movieid, rat)*) --> (U, user, (fi, fv’))
Here we have input in the reducer from two mappers which have common key as UserId. So in the reducer we have row r from matrix m and row r from matrix U. We also need to override the configure function in the main to load the Matrix V into the memory which can be accessed by reducer for calculating the new feature values.
Similarly we have also done for the updating the feature value so the V matrix.






