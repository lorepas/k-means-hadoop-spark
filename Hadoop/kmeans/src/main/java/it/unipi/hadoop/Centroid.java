package it.unipi.hadoop;

import java.io.*;
import java.util.*;

public class Centroid{
    
    private float[] values;
    private int dim;

    public static Centroid copy(final Centroid cw)
    {
        return new Centroid(cw.values, cw.dim);
    }

    public Centroid(int d)
    {
        this.dim = d;
        this.values = new float[this.dim];
    }

    public Centroid(final float[] v, final int d)
    {
        this.values = new float[d];
        this.set(v,d);
    }

    public Centroid(final String[] v, final int d)
    {
        this.values = new float[d];
        this.set(v,d);
    }

    public void set(final float[] v, final int d)
    {
        this.dim = d;
        for(int i = 0; i< d; i++){
            this.values[i] = v[i];
        }
    }
    
    public void set(final String[] v, final int d)
    {
        this.dim = d;
        for(int i = 0; i< d; i++){
            this.values[i] = Float.parseFloat(v[i]);
        }
    }
    
    public float[] getValues(){
        return this.values;
    }
    
    public int getDim()
    {
        return this.dim;
    }
    
    public void getDim(final int d)
    {
        this.dim = d;
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
    
    public double getDistance(Centroid p1) throws Exception {
        
        if(this.getDim() != p1.getDim()) 
            throw new Exception("Invalid lenght!");
        
        double sum = 0;
        for (int i = 0; i < this.getDim(); i++){
            sum += Math.pow(this.values[i] - p1.getValues()[i], 2);
        }
        return Math.sqrt(sum);
    }

}

