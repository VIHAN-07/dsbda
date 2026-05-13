import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MusicAnalyticsRadioSkip {

    public static class MusicMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text trackId = new Text();
        private Text radioSkip = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split(",");

                if (fields.length < 5) {
                    return;
                }

                String track = fields[1].trim();
                String radio = fields[3].trim();
                String skip = fields[4].trim();

                trackId.set(track);

                radioSkip.set(radio + "," + skip);

                context.write(trackId, radioSkip);

            } catch (Exception e) {
                // skip malformed lines
            }
        }
    }

    public static class MusicReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            int radioCount = 0;
            int skipCount = 0;

            for (Text val : values) {

                String[] parts = val.toString().split(",");

                int radio = Integer.parseInt(parts[0]);
                int skip = Integer.parseInt(parts[1]);

                radioCount += radio;
                skipCount += skip;
            }

            String result = "RadioCount=" + radioCount
                          + ", SkipCount=" + skipCount;

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Music Analytics Radio Skip Count");

        job.setJarByClass(MusicAnalyticsRadioSkip.class);

        job.setMapperClass(MusicMapper.class);
        job.setReducerClass(MusicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
