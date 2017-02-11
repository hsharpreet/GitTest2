package harpreet.hadoop;
/*
 * Author
 * Harpreet Singh
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwitterWhoToFollow {

    public static class TwitterMapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
           
            // First, go through the list of all friends of user 'user' and emit 
            // (user,-friend), (friend,user)
            // 'friend1' will be used in the emitted pair of user and existing friend
            // 'friend2' will be used in the emitted pair
            IntWritable friend1 = new IntWritable();
            IntWritable friend2 = new IntWritable();
            while (st.hasMoreTokens()) {
            	//setting values in variables
            	Integer friend = Integer.parseInt(st.nextToken());
            	friend1.set(-friend);
                friend2.set(friend);
                //values are emitted here
                context.write(user, friend1);
                context.write(friend2, user);
            }
        }
    }

    public static class TwitterReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {

            // Attributes
            private int friendId;
            private int nCommonFriends;
            // Constructor
            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }
            // Getters
            public int getFriendId() {
                return friendId;
            }
            // String representation used in the reduce output            
            public String toString() {
                return String.valueOf(friendId);
            }
            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                return null;// Recommendation was not found!
            }
        }

        // The reduce method       
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            
            // 'allUsers' will store the friends of user 'user' 
            //and existing users(the negative values in 'values').
            ArrayList<Integer> allUsers = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                allUsers.add(value);
            }
           
            ArrayList<Recommendation> recommendations = new ArrayList<>();
            // Builds the recommendation array
            for (Integer userId : allUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                }
            }
            // Builds the output string that will be emitted
            // Using a StringBuffer is more efficient than concatenating strings
            StringBuffer sb = new StringBuffer(""); 
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            // Will give inverted list here 
            //and list of existing friends with negative signs
            context.write(user, result);
        }
    }
    
    public static class TwitterMapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));

            // 'positiveFriendsList' and 'negativeFriendsList' will store the 
            //list of friends of with negative and positive sign as per user 'user'
            ArrayList<Integer> positiveFriendsList = new ArrayList<>();
            ArrayList<Integer> negativeFriendsList = new ArrayList<>();
            while (st.hasMoreTokens()) {
                Integer friend = Integer.parseInt(st.nextToken());
                if(friend>0){
                	positiveFriendsList.add(friend);
                }else{
                	negativeFriendsList.add(friend);
                }
            }
            IntWritable frnd1 = new IntWritable();
            IntWritable frnd2 = new IntWritable();
            
          //Emit pairs of (x,y) and (y,x) with nested foor loop
            if(positiveFriendsList.size()>0){
                for (Integer tempFriend1 : positiveFriendsList) {
                	frnd1.set(tempFriend1);
                	for (Integer tempFriend2 : positiveFriendsList) {
                    	frnd2.set(tempFriend2);
                    	if(tempFriend1 != tempFriend2){
                        	context.write(frnd1, frnd2);
                    	}
                	}
                }
            }
           //List of existing friends are emit with user 'user'
            if(negativeFriendsList.size()>0){
                for (Integer friend : negativeFriendsList) {
                	frnd2.set(friend);
                	context.write(user, frnd2);
                }
            }
        }
    }

    public static class TwitterReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {

            // Attributes
            private int friendId;
            private int nCommonFriends;

            // Constructor
            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }

            // Getters
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend() {
                nCommonFriends++;
            }

            // String representation used in the reduce output            
            public String toString() {
                return friendId + "(" + nCommonFriends + ")";
            }

            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                // Recommendation was not found!
                return null;
            }
        }

        // The reduce method       
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            // 'existingFriends' will store the friends of user 'user'
            // (the negative values in 'values').
            ArrayList<Integer> existingFriends = new ArrayList();
            // 'recommendedUsers' will store the list of user ids recommended
            // to user 'user'
            ArrayList<Integer> recommendedUsers = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                if (value > 0) {
                    recommendedUsers.add(value);
                } else {
                    existingFriends.add(value);
                }
            }
            System.out.println(recommendedUsers);
            System.out.println(existingFriends);
            // 'recommendedUsers' now contains all the positive values in 'values'.
            // We need to remove from it every value -x where x is in existingFriends.
            // See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
            for (Integer friend : existingFriends) {
                recommendedUsers.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
            }
            ArrayList<Recommendation> recommendations = new ArrayList<>();
            // Builds the recommendation array
            for (Integer userId : recommendedUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
            // Sorts the recommendation array
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	
    	String inputDir = "/Users/Harpreet/gitTest2/HadoopTest1/input";
    	String outputTempDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/temp4";
    	String outputFinalDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/final4";

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "twitter1");
        job1.setJarByClass(TwitterWhoToFollow.class);
        job1.setMapperClass(TwitterMapper1.class);
        job1.setReducerClass(TwitterReducer1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(outputTempDir));
      //  System.exit(job1.waitForCompletion(true) ? 0 : 1);
        
        //Complete the first job 
        boolean success = job1.waitForCompletion(true);
        if (success) {
	        Job job2 = Job.getInstance(conf, "twitter2");
	        job2.setJarByClass(TwitterWhoToFollow.class);
	        job2.setMapperClass(TwitterMapper2.class);
	        job2.setReducerClass(TwitterReducer2.class);
	        job2.setOutputKeyClass(IntWritable.class);
	        job2.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job2, new Path(outputTempDir));
	        FileOutputFormat.setOutputPath(job2, new Path(outputFinalDir));
	        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}
