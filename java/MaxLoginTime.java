import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxLoginTime {

    public static class LogMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text ip = new Text();
        private LongWritable durationValue = new LongWritable();

        private static final SimpleDateFormat sdf =
                new SimpleDateFormat("d/M/yyyy H:mm");

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split(",");

                if (fields.length < 8) {
                    return;
                }

                String userIP = fields[1].trim();
                String loginTime = fields[5].trim();
                String logoutTime = fields[7].trim();

                Date loginDate = sdf.parse(loginTime);
                Date logoutDate = sdf.parse(logoutTime);

                long duration =
                        (logoutDate.getTime() - loginDate.getTime())
                                / (1000 * 60);

                ip.set(userIP);
                durationValue.set(duration);

                context.write(ip, durationValue);

            } catch (Exception e) {
                System.out.println("Skipping Invalid Record");
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, LongWritable,
                            Text, LongWritable> {

        private LongWritable result = new LongWritable();

        public void reduce(Text key,
                           Iterable<LongWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            long total = 0;

            for (LongWritable val : values) {
                total += val.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Max Login Time");

        job.setJarByClass(MaxLoginTime.class);

        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(job.waitForCompletion(true)
                ? 0 : 1);
    }
}
