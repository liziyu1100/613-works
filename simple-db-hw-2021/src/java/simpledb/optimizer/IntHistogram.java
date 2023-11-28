package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int[]bucket;
    private double rate;
    private int min_;
    private int max_;
    private int ntups;
    private int avg_w;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        bucket = new int[buckets];
        Arrays.fill(bucket,0);
        rate = (max-min+1)*1.0/buckets;
        min_ = min;
        max_ = max;
        ntups = 0;
        if (rate<1.0)avg_w = 1;
        else{
            avg_w = (int)rate;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = (int) ((v-min_)*1.0/rate);
        bucket[index] = bucket[index] + 1;
        ntups = ntups +1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if (v>max_)v=max_;
        else if (v<min_)v=min_;
        int index = (int) ((v-min_)*1.0/rate);
        int h = bucket[index];
        double selectivity = -1.0;
        if (op == Predicate.Op.EQUALS){
            selectivity = (h*1.0/avg_w*1.0)/ntups;
        }
        else if (op == Predicate.Op.GREATER_THAN){
            double b_f = h*1.0/ntups*1.0;
            double right = (index+1)*(rate)+min_*1.0;
            if (right>max_)right=max_;
            double b_part = (right-v)/avg_w;
            selectivity = b_f*b_part;
            for (int i=index+1;i<bucket.length;i++){
                selectivity = selectivity + bucket[i]*1.0/ntups*1.0;
            }
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
