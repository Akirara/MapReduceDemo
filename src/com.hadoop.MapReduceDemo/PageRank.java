package com.hadoop.MapReduceDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Zhaoxuan on 2017/4/1.
 */
public class PageRank {
    private final static double N = 4.0;

    public static class AdjacentNodes {
        double value = 0.0;
        int num = 0;
        String[] nodes = null;

        public void formatInfo(String str) {
            String[] val = str.split("\t");
            this.setValue(Double.parseDouble(val[0]));
            this.setNum(val.length - 1);

            if (this.num != 0) {
                this.nodes = new String[this.num];
            }

            for (int i = 1; i < val.length; i++) {
                this.nodes[i - 1] = val[i];
            }
        }

        public void setNodes(String[] nodes) {
            this.nodes = nodes;
            this.num = nodes.length;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        public int getNum() {
            return num;
        }

        public String[] getNodes() {
            return nodes;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder(this.value + "");

            for (int i = 0; i < this.num; i++) {
                stringBuilder.append("\t");
                stringBuilder.append(this.nodes[i]);
            }

            return stringBuilder.toString();
        }
    }

    public static enum ValueEnum {
        CLOSURE_VALUE;
    }

    public static class RankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            AdjacentNodes adjacentNodes = new AdjacentNodes();
            int count = context.getConfiguration().getInt("count", 1);

            if (count == 1) {
                AdjacentNodes firstAdj = new AdjacentNodes();
                firstAdj.setValue(1.0);
                firstAdj.setNodes(value.toString().split("\t"));
                adjacentNodes = firstAdj;
            } else {
                adjacentNodes.formatInfo(value.toString());
            }

            context.write(key, new Text(adjacentNodes.toString()));

            double pageRank = adjacentNodes.getValue() / adjacentNodes.getNum();
            for (int i = 0; i < adjacentNodes.getNum(); i++) {
                String node = adjacentNodes.getNodes()[i];
                context.write(new Text(node), new Text(pageRank + ""));
            }
        }
    }

    public static class RankSumReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            AdjacentNodes adjacentNodes = new AdjacentNodes();

            Double sum = 0.0;
            for (Text adj : values) {
                String str = adj.toString();
                if (str.split("\t").length  > 1) {
                    adjacentNodes.formatInfo(str);
                } else {
                    sum += Double.parseDouble(str);
                }
            }

            double n = 0.8;
            double pageRank = sum * n + (1 - n) / N;

            int closure = (int)(Math.abs(pageRank - adjacentNodes.getValue()) * 1000);

            context.getCounter(ValueEnum.CLOSURE_VALUE).increment(closure);

            adjacentNodes.setValue(pageRank);
            context.write(key, new Text(adjacentNodes.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            int i = 0;
            while (true) {
                i++;
                conf.setInt("count", i);

                Job job = Job.getInstance(conf);
                job.setJarByClass(PageRank.class);
                job.setMapperClass(RankMapper.class);
                job.setReducerClass(RankSumReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setInputFormatClass(KeyValueTextInputFormat.class);
                if (i == 1) {
                    FileInputFormat.addInputPath(job, new Path("input/PageRank"));
                } else {
                    FileInputFormat.addInputPath(job, new Path("output/PageRank/pr" + (i - 1)));
                }

                Path path = new Path("output/PageRank/pr" + i);

                FileOutputFormat.setOutputPath(job, path);

                boolean b = job.waitForCompletion(true);
                if (b) {
                    long closure = job.getCounters().findCounter(ValueEnum.CLOSURE_VALUE).getValue();
                    double avg = closure / (1000.0 * N);
                    System.out.println("Iteration " + i + ": closure=" + closure + ", avg=" + avg);
                    if (avg < 0.001) {
                        System.out.println("Total iteration: " + i + ", Finish.");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
