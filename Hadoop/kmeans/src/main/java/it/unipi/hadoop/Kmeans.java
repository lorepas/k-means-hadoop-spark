
package it.unipi.hadoop;


import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;


public class Kmeans
{
    static double threshold = 0.1;
    
    public static class KmeansMapper extends Mapper<LongWritable, Text, Text, PointWritable>{
        
        private final Text reducerKey = new Text();
        private PointWritable reducerValue;
        private int dim;
        private int k;
        private Centroid[] initialCentroids;
        private String[] tokens;
        private String record;

        public void setup(Context context) throws IOException, InterruptedException
        {
            dim = context.getConfiguration().getInt("dim",2);
            k = context.getConfiguration().getInt("k",3);
            initialCentroids = new Centroid[k];
            
            String cent_i;
            String [] values;
            for(int i = 0; i<k; i++){
                cent_i = context.getConfiguration().get("cent" + i);
                values = cent_i.split(";");
                initialCentroids[i] = new Centroid(values, dim);
            }

            reducerValue = new PointWritable(dim);

        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            record = value.toString();
            if (record == null || record.length() == 0)
                return;

            tokens = record.trim().split(";");
            
            if (tokens.length == dim+1){
                
                int min_norm = 100000;
                int norm;
                int closest_centroid_index = 0;
                for(int j = 0; j < k; j++){
                    norm = 0;
                    for(int i = 0; i<dim; i++){
                        norm += Math.pow((Float.parseFloat(tokens[i]) - (initialCentroids[j].getValues()[i])), 2);
                    }
                    if(norm <= min_norm){
                        min_norm = norm;
                        closest_centroid_index = j;
                    }
                }

                reducerKey.set(String.valueOf(closest_centroid_index));
                reducerValue.set(tokens, 1, dim);
                context.write(reducerKey, reducerValue);
            }
        }
    }

    public static class KmeansCombiner extends Reducer<Text, PointWritable, Text, PointWritable>{
        
        private PointWritable updatedCentroid;

        public void reduce(Text key, Iterable<PointWritable> points, Context context) throws IOException, InterruptedException
        {
            updatedCentroid = PointWritable.copy(points.iterator().next());

            while(points.iterator().hasNext()){
                updatedCentroid.sum(points.iterator().next());
            }

            context.write(new Text(key), updatedCentroid);

        }
    }

    public static class KmeansReducer extends Reducer<Text, PointWritable, Text, Text>{
        
        private PointWritable updatedCentroid;

        public void reduce(Text key, Iterable<PointWritable> points, Context context) throws IOException, InterruptedException
        {
            updatedCentroid = PointWritable.copy(points.iterator().next());

            while(points.iterator().hasNext()){
                updatedCentroid.sum(points.iterator().next());
            }
            
            updatedCentroid.updatePoint();

            context.write(new Text(key), new Text(updatedCentroid.toString()));

        }
    }

    public static void main(String[] args) throws Exception{

	long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4) {
           System.err.println("Usage: Kmeans <n_points> <n_centroids> <dimension> <outputdir> <n_iteration>");
           System.exit(1);
        }
        System.out.println("args[0]: <n_points>=" + Integer.parseInt(otherArgs[0]));
        System.out.println("args[1]: <n_centroids>=" + Integer.parseInt(otherArgs[1]));
        System.out.println("args[2]: <dimension>=" + Integer.parseInt(otherArgs[2]));
        System.out.println("args[3]: <outputdir>=" + otherArgs[3]);

        final int n_points = Integer.parseInt(otherArgs[0]);
        final int n_centroids = Integer.parseInt(otherArgs[1]);
        final int dim = Integer.parseInt(otherArgs[2]);
        final Path output_path = new Path(otherArgs[3] + "/temp");
        boolean check = false;

        conf.setInt("n", n_points);
        conf.setInt("k", n_centroids);
        conf.setInt("dim", dim);

        Centroid[] previousCentroids = new Centroid[n_centroids];
        previousCentroids = readInitialCentroid(conf, dim, n_centroids, "initialCentroids.txt");
        
        Centroid[] currentCentroids = new Centroid[n_centroids];;     

        int iterationCounter = 0;
        while(true){

            if(iterationCounter == 0)
                setCentroidsInConf(conf, previousCentroids, n_centroids);
            else
                setCentroidsInConf(conf, currentCentroids, n_centroids);

            Job job = Job.getInstance(conf, "Kmeans");
            job.setJarByClass(PointWritable.class);

            // set mapper/combiner/reducer
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setReducerClass(KmeansReducer.class);

            // define mapper's output key-value
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PointWritable.class);

            // define reducer's output key-value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // define I/O
            FileInputFormat.addInputPath(job, new Path("dataset.txt"));
            FileOutputFormat.setOutputPath(job, output_path);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            boolean jobFinished = job.waitForCompletion(true);

            if(!jobFinished){
                System.err.println("Job " + iterationCounter + " failed");
                System.exit(1);
            }

            currentCentroids = readPoints(conf, dim, n_centroids, otherArgs[3] + "/temp");

            /*
            System.out.println("CURRENT_CENTROIDS");
            for(int p = 0; p < n_centroids; p++){
                System.out.println(currentCentroids[p].toString());
            }
            */

            if(kmeansConverged(previousCentroids, currentCentroids, n_centroids, threshold) || iterationCounter > 14){
                System.out.println("The kmeans algorithm has converged in " + iterationCounter +" steps.\n");
                check = true;
                break;
            }

            //System.out.println("PREVIOUS_CENTROIDS");
            for(int j = 0; j < n_centroids; j++){
                //System.out.println(previousCentroids[j].toString());
                previousCentroids[j].set(currentCentroids[j].getValues(), currentCentroids[j].getDim());
            }

            iterationCounter++;

        }
        if(check){
            long finishTime = System.currentTimeMillis();
            long timeSpent = finishTime - startTime;
            int seconds = (int) (timeSpent / 1000);
            System.out.println("Time spent for the process: " + String.valueOf(seconds) + " seconds.\n");
            writeOutputPoints(currentCentroids);
        }
        else{
            System.out.println("An error occurred.\n");
        }
        

        System.exit(0);

    }

    private static Centroid[] readInitialCentroid(Configuration conf, int dim, int k, String pathString)throws IOException{

        Centroid[] points = new Centroid[k];

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(pathString);
        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        String line;
        String arr[];
        int i = 0;
        while ((line = reader.readLine()) != null)
        {
          line.replace("\n","");
          arr = line.split(";");
          points[i] = new Centroid(arr, dim);
          i++;
        }

        reader.close();

        hdfs.delete(new Path(pathString),true);

        return points;
        
    }

    private static Centroid[] readPoints(Configuration conf, int dim, int k, String pathString)throws IOException{

        Centroid[] points = new Centroid[k];

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(pathString + "/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        String line;
        String arr[];
        int i = 0;
        while ((line = reader.readLine()) != null)
        {
          line.replace("\n","");
          arr = line.split("\\s+");
          arr = arr[1].split(";");
          points[i] = new Centroid(arr, dim);
          i++;
        }

        reader.close();

        hdfs.delete(new Path(pathString),true);

        return points;
        
    }

    private static void setCentroidsInConf(Configuration conf, Centroid[] initialCentroids, int k){
        
        for(int i = 0; i < k; i++){
            conf.set("cent" + i, initialCentroids[i].toString());
        }
        
    }


    private static boolean kmeansConverged(Centroid[] previousCentroids, Centroid[] currentCentroids, int k, double threshold) throws Exception{
        
        int counter = 0;
        for(int i = 0; i<k; i++){
            if(previousCentroids[i].getDistance(currentCentroids[i]) < threshold){
                counter++;
            }
        }
        if(counter == k)
            return true;
        else 
            return false;

    }

    private static void writeOutputPoints(Centroid[] centroids) throws IOException{
        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter("out.txt", true);
            out = new BufferedWriter(fstream);
            for(int i=0; i<centroids.length; i++){
                out.write(centroids[i].toString());
                out.write(String.valueOf(i));
                out.write("\n");
            }
        }

        catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        finally {
            if(out != null) {
                out.close();
            }
        }
    }
}

