import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAnalytics {

    public static class MovieMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text movieId = new Text();
        private DoubleWritable ratingValue = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split(",");

                if (fields.length < 4) {
                    return;
                }

                String movie = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                movieId.set(movie);
                ratingValue.set(rating);

                context.write(movieId, ratingValue);

            } catch (Exception e) {
                // ignore bad records
            }
        }
    }

    public static class MovieReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        private Map<String, Double> movieAvgMap = new HashMap<String, Double>();

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = (count == 0) ? 0.0 : (sum / count);

            movieAvgMap.put(key.toString(), avg);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            List<Map.Entry<String, Double>> list =
                    new ArrayList<Map.Entry<String, Double>>(movieAvgMap.entrySet());

            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> a,
                                   Map.Entry<String, Double> b) {
                    return Double.compare(b.getValue(), a.getValue());
                }
            });

            int topN = Math.min(10, list.size());

            for (int i = 0; i < topN; i++) {
                Map.Entry<String, Double> entry = list.get(i);

                String result = "AvgRating=" + entry.getValue();

                context.write(new Text(entry.getKey()), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Top 10 Movie Recommendation");

        job.setJarByClass(MovieAnalytics.class);

        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
