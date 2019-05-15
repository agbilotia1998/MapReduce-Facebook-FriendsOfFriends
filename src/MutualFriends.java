/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mutualfriends;

/**
 *
 * @author agbilotia1998
 */
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class MutualFriends {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException{
            String input = value.toString();
            String listKeyValues[] = input.split("[\\s*]");
            String friends[] = listKeyValues[1].split(",");
            
            for(int count = 0; count < friends.length; count++) {
                String[] pairList = new String[2];
                pairList[0] = listKeyValues[0];
                pairList[1] = friends[count];
                Arrays.sort(pairList);
                String pair = pairList[0] + " " + pairList[1];
                System.out.println(pair + "---->" + listKeyValues[1]);
                context.write(new Text(pair), new Text(listKeyValues[1]));
            }
        }
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text pair, Iterable<Text>friends, Context context) throws IOException, InterruptedException{
            int count = 0;
            String[] allFriends = new String[2];
            
            for(Text friendList: friends) {
               allFriends[count] = friendList.toString();
               System.out.println(count);
               System.out.println(friendList.toString());
               count++;
            }
            String firstFriendsArray[] = allFriends[0].split(",");
            HashSet<String> s1 = new HashSet<String>(Arrays.asList(firstFriendsArray));
            
            String secondFriendsArray[] = allFriends[1].split(",");
            HashSet<String> s2 = new HashSet<String>(Arrays.asList(secondFriendsArray));
            
            s1.retainAll(s2);
            String[] mutualFriendsList = s1.toArray(new String[s1.size()]);
            
            String mutualFriends = "";
            for(String mutualFriend: mutualFriendsList) {
                mutualFriends = mutualFriends + mutualFriend + " ";
            }
            mutualFriends = mutualFriends.trim();
            
            context.write(pair, new Text(mutualFriends));
        }
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
  
        //String str = "0	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15";
       Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "MutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path output = new Path(args[1]);
        
        output.getFileSystem(conf).delete(output, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       
    }
    
}
