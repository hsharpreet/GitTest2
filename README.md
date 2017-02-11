# GitTest2
Hadoop Twitter Who To Follow example

This program find how many followers can be recommended to a particular user. It implements 2 maps and 2 reducers to make
make and use intermedicate list by Map 1. Here are 2 steps performed by both Mappers and Reducers individually

1. Indexing: for each user Fi followed by user X, the mapper emits Fi as the key and X as the value. 
The reducer is the identity. It produces inverted lists of followers:
X,[Y1,Y2,... ,Yk ]
where the Yi all follow user X.

2. Similarity: for each inverted list X, [ Y1, Y2, ..., Yk ] the mapper emits 
all pairs (Yi,Yj) and (Yj,Yi) where i ∈ [1,k], j ∈ [1,k] and i ̸= j. As in “People You May Know”, 
the reducer receives a list X, [ F1, F2, ... ] where Fi appears exactly x times if X and Fi 
follow x people in common. It counts the occurrences of Fi whenever Fi is not followed by X 
and sorts the resulting recommendations by number of common followed people.

How and where to use:
This algorithm needs input like this in a text file

  1 345
  2 135 
  3 1245 
  4 1235 
  5 3

as this file says:
friend1 follows friend3,friend4,friend5 and so on with other friends
It will give output like:
  1 2(2)
  2 4(3)
  3
  4
  5 2(1) 1(1) 4(1)

which means friend2 is recommended to friend1 twice. List of recommended friends will be sorted 
according to higher number of recommendations

How to pass input and output file variables:
A. Traditional approach
  1. Set the run configurations and pass parameters.
    Three parameters are needed for this program.
  2. Set the parameters in the main method like:
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     
It works but if we have to run multiple times with different output or input address, we need to change Run Configuration
all the time. So to make it more easier use the second approach.

B. Set locations as variables(Recommended approach)

Write down the address of output folder like this in the main method
      String inputDir = "/Users/Harpreet/gitTest2/HadoopTest1/input";
    	String outputTempDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/temp4";
    	String outputFinalDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/final4";
      
and pass these variables in the file path functions like:
       FileInputFormat.addInputPath(job2, new Path(outputTempDir));
       FileOutputFormat.setOutputPath(job2, new Path(outputFinalDir));
       
Everytime we can rename the 'String outputTempDir' to make temp 1,2,3 folders and so on.

How to Run this program:
1. Set input file as arguement/parameter. ('String inputDir' is not used)
2. Set the location of folders in the 'String outputTempDir' and 'String outputFinalDir'

Or just modify the program to pass parameters.

Thanks,
Harpreet Singh - 40012679
Nitesh Kumar - 40038811
