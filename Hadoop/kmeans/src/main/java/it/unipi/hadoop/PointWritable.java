package it.unipi.hadoop;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class PointWritable implements Writable{
    
    private float[] values;
    private int counter;
    private int dim;
    
    public static PointWritable copy(final PointWritable kw)
    {
        return new PointWritable(kw.values, kw.counter, kw.dim);
    }
    
    public PointWritable(){
        
    }

    public PointWritable(int d)
    {
        this.dim = d;
        this.counter = 0;
        this.values = new float[dim];
    }

    public PointWritable(final float[] v, final int c, final int d)
    {
        this.values = new float[d];
        this.set(v,c,d);
    }

    public PointWritable(final String[] v, final int c, final int d)
    {
        this.values = new float[d];
        this.set(v,c,d);
    }

    public void set(final float[] v, final int c, final int d)
    {
        this.dim = d;
        for(int i = 0; i< d; i++){
            this.values[i] = v[i];
        }
        this.counter = c;
    }
    
    public void set(final String[] v, final int c, final int d)
    {
        this.dim = d;
        for(int i = 0; i< d; i++){
            this.values[i] = Float.parseFloat(v[i]);
        }
        this.counter = c;
    }
    
    public float[] getValues(){
        return this.values;
    }

    public int getCounter()
    {
        return this.counter;
    }
    
    public int getDim()
    {
        return this.dim;
    }
    
    public void setCounter(final int c)
    {
        this.counter = c;
    }
    
    public void getDim(final int d)
    {
        this.dim = d;
    }
    
    public void sum(PointWritable p) {
        this.counter += p.getCounter();
        for(int i = 0; i < p.getDim(); i++){
            this.values[i] += p.values[i];
        }
    }
    
    public void updatePoint() {
        for(int i = 0; i<this.getDim(); i++){
            this.values[i] /= this.getCounter();
        }
    }

    @Override
    public String toString()
    {
        String str = "";
        for(int i = 0; i<this.getDim(); i++) {
            str +=  this.values[i] + ";";
        }
        return str;
    }
    
    public double getDistance(PointWritable p1) throws Exception {
        
        if(this.getDim() != p1.getDim()) 
            throw new Exception("Invalid lenght!");
        
        double sum = 0;
        for (int i = 0; i < this.getDim(); i++){
            sum += Math.pow(this.values[i] - p1.values[i], 2);
        }
        return Math.sqrt(sum);
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.dim);
        for(int i = 0; i < this.dim; i++){
            out.writeFloat(this.values[i]);
        }
        out.writeInt(this.counter);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.dim = in.readInt();
        this.values = new float[this.dim];
        for(int i = 0; i<this.dim; i++){
            this.values[i] = in.readFloat();
        }
        this.counter = in.readInt();

    }
}

