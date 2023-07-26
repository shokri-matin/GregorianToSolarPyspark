How can we change the Gregorian date to the Solar one in the Pyspark project? There is no native Pyspark function to handle that, so the first solution that comes to mind is to use the predefine Python date converter.

There are two conventional solutions to using the non-native spark function in a project.

1- User Define Functions: UDFs are used to extend the functions of the framework and reuse these functions on multiple DataFrames. For example, you wanted to capitalize every first word in a specific string column. PySpark build-in features do not have this function; hence create a UDF and reuse this as needed on many Data Frames.

2- Mapping functions is another way to do that.

The most crucial problem individuals face using UDF is that performance in these solutions is significantly slower than native spark functions. Sometimes UDFs may be 10x slower than native ones depending on the task.

To deal with this problem, I implemented the simple code to convert Gregorian to Solar date adopting native functions available on my Github.

For this purpose, I used the well-explained document available at https://jostrans.org/issue17/art_darani.php.
