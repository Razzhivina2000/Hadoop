import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class candle {
    public static class CandleMapper
            extends Mapper<Object, Text, Text, Text>{
        private String securities;
        private long width;
        private String sDateFrom;
        private String sDateTo;
        private String sTimeFrom;
        private String sTimeTo;

        protected void setup(Context context) throws InterruptedException {
            securities = context.getConfiguration().get("candle.securities", ".*");
            sDateFrom = context.getConfiguration().get("candle.date.from", "19000101");
            sDateTo = context.getConfiguration().get("candle.date.to", "20200101");
            sTimeFrom = context.getConfiguration().get("candle.time.from", "1000");
            sTimeTo = context.getConfiguration().get("candle.time.to", "1800");
            width = Long.parseLong(context.getConfiguration().get("candle.width", "300000"));

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (value.toString().charAt(0) == '#') {
                return;
            }
            SimpleDateFormat parser = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            String[] vals = value.toString().split(",");
            if (vals[0].matches(securities)) {
                if (vals[2].substring(0, 8).compareTo(sDateFrom) >= 0 && vals[2].substring(0, 8).compareTo(sDateTo) < 0 && vals[2].substring(8, 12).compareTo(sTimeFrom) >= 0 && vals[2].substring(8, 12).compareTo(sTimeTo) < 0) {
                    long time;
                    try {
                        time = parser.parse(vals[2]).getTime();
                    } catch (java.text.ParseException e)  {
                        return;
                    }
                    long timeBegin = time - time % width;
                    //System.out.println(vals[0] + "," + String.format("%d", timeBegin));
                    context.write(new Text(vals[0] + "," + parser.format(timeBegin)), new Text(vals[2] + "," + vals[3] + "," + vals[4]));
                }
            }
        }
    }

    public static class CandleReducer
            extends Reducer<Text,Text,Text,Text> {
        private MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double open = 0, high = Double.MIN_VALUE, low = Double.MAX_VALUE, close = 0;
            long openTime = Long.MAX_VALUE, openIdDeal = Long.MAX_VALUE, closeTime = Long.MIN_VALUE, closeIdDeal = Long.MIN_VALUE;
            String[] valsKey = key.toString().split(",");
            for (Text val : values) {
                String[] vals = val.toString().split(",");
                if (vals.length == 3) {
                    long time = Long.parseLong(vals[0]);
                    long idDeal = Long.parseLong(vals[1]);
                    double priceDeal = Double.parseDouble(vals[2]);
                    if (time < openTime || (time == openTime && idDeal < openIdDeal)) {
                        openTime = time;
                        openIdDeal = idDeal;
                        open = priceDeal;
                    }
                    if (time > closeTime || (time == closeTime && idDeal > closeIdDeal)) {
                        closeTime = time;
                        closeIdDeal = idDeal;
                        close = priceDeal;
                    }
                    if (priceDeal < low) {
                        low = priceDeal;
                    }
                    if (priceDeal > high) {
                        high = priceDeal;
                    }
                }
            }
            mos.write(NullWritable.get(), new Text(valsKey[0] + ',' + valsKey[1] + "," + String.format("%.1f", open) + "," + String.format("%.1f", high) + "," + String.format("%.1f", low) + "," + String.format("%.1f", close)), valsKey[0]);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }


    }

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String key1 = w1.toString();
            String key2 = w2.toString();
            return key1.compareTo(key2);
        }
    }

    public static class CandlePartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String pathIn = "";
        String pathOut = "";
        if (otherArgs.length < 2) {
            System.err.println("Usage: candle <in> <out>");
            System.exit(2);
        }
        if (otherArgs.length == 2) {
            pathIn = otherArgs[0];
            pathOut = otherArgs[1];
        } else {
            if(otherArgs[0] == "-conf") {
                conf.addResource(new Path(otherArgs[1]));
                pathIn = otherArgs[2];
                pathOut = otherArgs[3];
            }
        }

        conf.set("candle.width", conf.get("candle.width", "300000"));
        conf.set("candle.securities", conf.get("candle.securities", ".*"));
        conf.set("candle.date.from", conf.get("candle.date.from", "19000101"));
        conf.set("candle.date.to", conf.get("candle.date.to", "20200101"));
        conf.set("candle.time.from", conf.get("candle.time.from", "1000"));
        conf.set("candle.time.to", conf.get("candle.time.to", "1800"));
        conf.set("candle.num.reducers", conf.get("candle.num.reducers", "1"));

        Job job = new Job(conf, "candle");
        job.setJarByClass(candle.class);
        job.setMapperClass(CandleMapper.class);
        //job.setPartitionerClass(CandlePartitioner.class);
        //job.setCombinerClass(CandleReducer.class);
        job.setReducerClass(CandleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setSortComparatorClass(DescendingKeyComparator.class);
        //LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setNumReduceTasks(Integer.parseInt(conf.get("candle.num.reducers")));
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
