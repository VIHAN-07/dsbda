import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MusicAnalyticsShares {

    public static class MusicMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text trackId = new Text();
        private Text userShared = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split(",");

                if (fields.length < 5) {
                    return;
                }

                String userId = fields[0].trim();
                String track = fields[1].trim();
                String shared = fields[2].trim();

                trackId.set(track);

                userShared.set(userId + "," + shared);

                context.write(trackId, userShared);

            } catch (Exception e) {
                // skip bad lines
            }
        }
    }

    public static class MusicReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            HashSet<String> uniqueUsers = new HashSet<String>();

            int shareCount = 0;

            for (Text val : values) {

                String[] parts = val.toString().split(",");

                String user = parts[0];
                int shared = Integer.parseInt(parts[1]);

                uniqueUsers.add(user);

                if (shared == 1) {
                    shareCount++;
                }
            }

            String result =
                    "UniqueListeners=" + uniqueUsers.size()
                    + ", Shares=" + shareCount;

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Music Analytics");

        job.setJarByClass(MusicAnalyticsShares.class);

        job.setMapperClass(MusicMapper.class);
        job.setReducerClass(MusicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
