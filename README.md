# Map_Reduce_Project

# MapReduce

The MapReduce programming technique was designed to analyze massive data sets across a cluster. In this Jupyter notebook, you'll get a sense for how Hadoop MapReduce works; however, this notebook will run locally (or on colab) rather than on a cluster.

The biggest difference between Hadoop and Spark is that Spark tries to do as many calculations as possible in memory, which avoids moving data back and forth across a cluster. MapReduce writes intermediate calculations out to disk, which can be less efficient. MapReduce is an older technology than Spark and one of the cornerstone big data technologies.

You must use the file called "songplays.txt" and put it in your workspace. This is a text file where each line represents a song that was played in the Sparkify app. The MapReduce code will count how many times each song was played. In other words, the code counts how many times the song title appears in the list.


# MapReduce versus Hadoop MapReduce

Don't get confused by the terminology! MapReduce is a programming technique. Hadoop MapReduce is a specific implementation of the programming technique.

Some of the syntax will look a bit funny, so be sure to read the explanation and comments for each section. You'll learn more about the syntax in later lessons. 

Run each of the code cells below to see the output.

After running the code cells, implement the exercices of the lecture in order to understand well the way MapReduce works and how you can use it to solve your problems
