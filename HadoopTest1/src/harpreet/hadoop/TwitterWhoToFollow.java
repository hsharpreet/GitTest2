package harpreet.hadoop.a1;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwitterPart1 {

    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            
            
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            // 'friends' will store the list of friends of user 'user'
            ArrayList<Integer> friends = new ArrayList<>();
            // First, go through the list of all friends of user 'user' and emit 
            // (user,-friend)
            // 'friend1' will be used in the emitted pair
            IntWritable friend1 = new IntWritable();
            IntWritable friend2 = new IntWritable();
            while (st.hasMoreTokens()) {
                /*Integer friend = Integer.parseInt(st.nextToken());
                friend1.set(friend);
                //context.write(user, friend1);
                context.write(friend1, user);
                friends.add(friend); // save the friends of user 'user' for later*/
            
            	Integer friend = Integer.parseInt(st.nextToken());
                friend1.set(-friend);
                friend2.set(friend);
                context.write(user, friend1);
                context.write(friend2, user);
                friends.add(friend); // save the friends of user 'user' for later
            }
           
        }
    }

    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

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

            /*public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend() {
                nCommonFriends++;
            }*/

            // String representation used in the reduce output            
            public String toString() {
                return friendId + ""; 
                /*intentionally commented
                "(" + nCommonFriends + ")";*/
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
            ArrayList<Integer> allUsers = new ArrayList<>();
            ArrayList<Integer> recommendedUsersOld = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                allUsers.add(value);
            }
            // 'recommendedUsers' now contains all the positive values in 'values'.
            // We need to remove from it every value -x where x is in existingFriends.
            // See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
            /*for (Integer friend : existingFriends) {
                recommendedUsers.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
            }*/
            ArrayList<Recommendation> recommendations = new ArrayList<>();
            // Builds the recommendation array
            for (Integer userId : allUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } //intentionally commented 
                /*else {
                    p.addCommonFriend();
                }*/
            }
            // Sorts the recommendation array
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            /*recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });*/
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
    
    public static class AllPairsMapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));

            // 'friends' will store the list of friends of user 'user'
            ArrayList<Integer> pFriends = new ArrayList<>();
            ArrayList<Integer> nFriends = new ArrayList<>();
            // First, go through the list of all friends of user 'user' and emit 
            // (user,-friend)
            // 'friend1' will be used in the emitted pair
            while (st.hasMoreTokens()) {
                Integer friend = Integer.parseInt(st.nextToken());
                if(friend>0){
                	pFriends.add(friend);
                }else{
                	nFriends.add(friend);
                }
            }
            IntWritable frnd1 = new IntWritable();
            IntWritable frnd2 = new IntWritable();
            
            if(pFriends.size()>0){
                for (Integer tempFriend1 : pFriends) {
                	frnd1.set(tempFriend1);
                	for (Integer tempFriend2 : pFriends) {
                    	frnd2.set(tempFriend2);
                    	if(tempFriend1 != tempFriend2){
                        	context.write(frnd1, frnd2);
                            //context.write(frnd2,frnd1);
                    	}
                	}
                }
            }
            if(nFriends.size()>0){
                for (Integer friend : nFriends) {
                	frnd2.set(friend);
                	context.write(user, frnd2);
                }
            }
        }
    }

    public static class CountReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

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
    	String outputTempDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/temp2";
    	String outputFinalDir = "/Users/Harpreet/gitTest2/HadoopTest1/output/final2";

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "twitter1");
        job1.setJarByClass(TwitterPart1.class);
        job1.setMapperClass(AllPairsMapper.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(outputTempDir));
      //  System.exit(job1.waitForCompletion(true) ? 0 : 1);
        
        boolean success = job1.waitForCompletion(true);
        if (success) {
	        //Configuration conf2 = new Configuration();
	        Job job2 = Job.getInstance(conf, "twitter2");
	        job2.setJarByClass(TwitterPart1.class);
	        job2.setMapperClass(AllPairsMapper2.class);
	       // job2.setReducerClass(CountReducer2.class);
	        job2.setOutputKeyClass(IntWritable.class);
	        job2.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job2, new Path(outputTempDir));
	        FileOutputFormat.setOutputPath(job2, new Path(outputFinalDir));
	        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}