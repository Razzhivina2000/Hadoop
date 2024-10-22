import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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

public class mm {
    public static class mmMapper
            extends Mapper<Object, Text, Text, Text> {

        private int g;
        private int Arows, Brows, Acols, Bcols, gW, gH;
        private String tags;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tags = conf.get("mm.tags");
            Arows = Integer.parseInt(conf.get("mm.Arows"));
            Brows = Integer.parseInt(conf.get("mm.Brows"));
            Acols = Integer.parseInt(conf.get("mm.Acols"));
            Bcols = Integer.parseInt(conf.get("mm.Bcols"));
            g = Integer.parseInt(conf.get("mm.groups"));
            gW = Bcols / g;
            gH = Arows / g;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println("keym" + key);
            //System.out.println("valuem" + value);
            String[] vals = value.toString().split("\\t");
            String tag = vals[0];
            if (tag.equals(String.valueOf(tags.charAt(0)))) {
                for (int gIdx = 0; gIdx < g; gIdx++) {
                    int i = Integer.parseInt(vals[1]);
                    context.write(new Text(String.format("%d", i / gH) + "," + String.format("%d", gIdx)), new Text(tag + "," + vals[1] + "," + vals[2] + "," + vals[3]));
                }
            } else if (tag.equals(String.valueOf(tags.charAt(1)))) {
                for (int gIdx = 0; gIdx < g; gIdx++) {
                    int k = Integer.parseInt(vals[2]);
                    {
                        context.write(new Text(String.format("%d", gIdx) + "," + String.format("%d", k / gW)), new Text(tag + "," + vals[1] + "," + vals[2] + "," + vals[3]));
                    }
                }
                //System.out.println("vals " + vals[0] + " " + vals[1] + " " + vals[2] + " " + vals[3] + " " + vals[4]);
            }
        }
    }

    public static class mmReducer
            extends Reducer<Text,Text,Text,Text> {
        private int g;
        private int Arows, Brows, Acols, Bcols, gW, gH;
        private String tags, f;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            tags = conf.get("mm.tags");
            Arows = Integer.parseInt(conf.get("mm.Arows"));
            Brows = Integer.parseInt(conf.get("mm.Brows"));
            Acols = Integer.parseInt(conf.get("mm.Acols"));
            Bcols = Integer.parseInt(conf.get("mm.Bcols"));
            f = conf.get("mm.float-format");
            g = Integer.parseInt(conf.get("mm.groups"));
            gW = Bcols / g;
            gH = Arows / g;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double[][] rowsA = new double[gH][Acols];
            double[][] colsB = new double[Acols][gW];
            //System.out.println("key" + key);
            for (Text val : values) {
                //System.out.println("val" + val);

                String[] value = val.toString().split(",");
                if (value[0].equals(String.valueOf(tags.charAt(0)))) {
                    rowsA[Integer.parseInt(value[1]) % gH][Integer.parseInt(value[2])] = Double.parseDouble(value[3]);
                } else if (value[0].equals(String.valueOf(tags.charAt(1)))) {
                    colsB[Integer.parseInt(value[1])][Integer.parseInt(value[2]) % gW] = Double.parseDouble(value[3]);
                }
            }
            String[] kvals = key.toString().split(",");
            int gI = Integer.parseInt(kvals[0]);
            int gK = Integer.parseInt(kvals[1]);
            for (int i = 0; i < gH; i++) {
                for (int k = 0; k < gW; k++) {
                    double sum = 0;
                    for (int j = 0; j < Acols; j++) {
                        sum += rowsA[i][j] * colsB[j][k];
                    }
                    //System.out.println(String.format("%d", gI) + " " + String.format("%d", gH) + " " + String.format("%d", i));
                    //System.out.println(tags.charAt(2) + "\t" + String.format("%d", gI * gH + i) + "\t" + String.format("%d", gK * gW + k) + "\t" + String.format("%.4f", sum));
                    if (Math.abs(sum) > 1e-9) {
                        context.write(null, new Text(tags.charAt(2) + "\t" + String.format("%d", gI * gH + i) + "\t" + String.format("%d", gK * gW + k) + "\t" + String.format(f, sum)));
                    }
                }
            }
        }


    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String pathA = "";
        String pathB = "";
        String pathC = "";
        if (otherArgs.length < 3) {
            System.err.println("Usage: mm <in1> <in2> <out>");
            System.exit(2);
        }
        if (otherArgs.length == 3) {
            pathA = otherArgs[0];
            pathB = otherArgs[1];
            pathC = otherArgs[2];
        } else {
            if(otherArgs[0] == "-conf") {
                conf.addResource(new Path(otherArgs[1]));
                pathA = otherArgs[2];
                pathB = otherArgs[3];
                pathC = otherArgs[4];
            }
        }

        conf.set("mm.groups", conf.get("mm.groups", "1"));
        conf.set("mm.tags", conf.get("mm.tags", "ABC"));
        conf.set("mm.float-format", conf.get("mm.float-format", "%.3f"));
        conf.set("mapred.reduce.tasks", conf.get("mapred.reduce.tasks", "1"));

        FileSystem fs = FileSystem.get(URI.create(pathA), conf);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(pathA + "/size"))))) {
            String valSize[] = reader.readLine().split("\\t");
            conf.set("mm.Arows", valSize[0]);
            conf.set("mm.Acols", valSize[1]);
            conf.set("mm.Crows", valSize[0]);
        }
        fs = FileSystem.get(URI.create(pathB), conf);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(pathB + "/size"))))) {
            String valSize[] = reader.readLine().split("\\t");
            conf.set("mm.Brows", valSize[0]);
            conf.set("mm.Bcols", valSize[1]);
            conf.set("mm.Ccols", valSize[1]);
        }
        fs = FileSystem.get(URI.create(pathC), conf);
        try (BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(pathC + "/size"))))) {
            outputWriter.write(conf.get("mm.Crows") + "\t" + conf.get("mm.Ccols"));
        }

        Job job = new Job(conf, "mm");
        job.setJarByClass(mm.class);
        job.setMapperClass(mmMapper.class);
        //job.setPartitionerClass(DepartmentPartitioner.class);
        //job.setCombinerClass(CandleReducer.class);
        job.setReducerClass(mmReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setSortComparatorClass(DescendingKeyComparator.class);
        job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks")));
        FileInputFormat.addInputPath(job, new Path(pathA + "/data"));
        FileInputFormat.addInputPath(job, new Path(pathB + "/data"));
        FileOutputFormat.setOutputPath(job, new Path(pathC + "/data"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}