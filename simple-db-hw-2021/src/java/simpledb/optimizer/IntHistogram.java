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
        rate = (max-min)*1.0/buckets;
        min_ = min;
        max_ = max;
        ntups = 0;
        if (rate<1.0)avg_w = 1; //如果桶的宽度最小置为1，因为一个范围内最少可以放下一个元素。
        else{
            avg_w = (int)rate;
            if (rate - avg_w*1.0>0.000001){
                avg_w = avg_w+1;
            }
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        double mid = (v-min_)*1.0/rate;
        int index = (int) ((v-min_)*1.0/rate);
        if (index == bucket.length)index = bucket.length-1;
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
        int old_v = v;
        if (v>max_){v=max_;}
        else if (v<min_){v=min_;}
        int index = (int) ((v-min_)*1.0/rate);
        if (index == bucket.length)index=bucket.length-1;
        int h = bucket[index];
        double selectivity = -1.0;
        if (op == Predicate.Op.EQUALS){
            if (old_v<min_||old_v>max_)selectivity = 0;
            else{
                selectivity = (h*1.0/avg_w*1.0)/ntups;
            }
        }
        else if (op == Predicate.Op.GREATER_THAN){
            if (avg_w >1 || old_v<min_){
                double b_f = h*1.0/ntups*1.0;
                double right = (index+1)*(rate)+min_*1.0;
                if (right>max_)right=max_;
                double b_part = (right-v)/rate;
                selectivity = b_f*b_part;
            }
            else{
                selectivity = 0;
            }
            for (int i=index+1;i<bucket.length;i++){
                selectivity = selectivity + bucket[i]*1.0/ntups*1.0;
            }
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ){
            selectivity = estimateSelectivity(Predicate.Op.GREATER_THAN,old_v)+estimateSelectivity(Predicate.Op.EQUALS,old_v);
        }
        else if (op == Predicate.Op.LESS_THAN){
            if (avg_w >1 || old_v>max_){
                double b_f = h*1.0/ntups*1.0;
                double left = (index)*(rate)+min_*1.0;
                if (left<min_)left=min_;
                double b_part = (v-left)/rate;
                selectivity = b_f*b_part;
            }
            else{
                selectivity = 0;
            }
            for (int i=index-1;i>=0;i--){
                selectivity = selectivity + bucket[i]*1.0/ntups*1.0;
            }
        }
        else if (op == Predicate.Op.LESS_THAN_OR_EQ){
            selectivity = estimateSelectivity(Predicate.Op.LESS_THAN,old_v)+estimateSelectivity(Predicate.Op.EQUALS,old_v);
        }
        else if (op == Predicate.Op.NOT_EQUALS){
            selectivity = 1 - estimateSelectivity(Predicate.Op.EQUALS,old_v);
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
