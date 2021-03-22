import java.io.*;
import java.util.Hashtable;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/* single color intensity */
class Color implements WritableComparable<Color>
{
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */

    Color(){}
    Color(short t, short i)
    {
        type=t;
        intensity=i;
    }

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
    	out.writeShort(type);
        out.writeShort(intensity);
	}

    @Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		type = in.readShort();
        intensity = in.readShort();
	}

    @Override
    public int compareTo(Color c)
	{
		// TODO Auto-generated method stub
		short type1 = this.type;
        short typeColor = c.type;
        short intensity1 = this.intensity;
        short intensityColor = c.intensity;

        if(type1 == typeColor)
        {
            if(intensity1==intensityColor)
        		  return 0;
          else if(intensity1>intensityColor)
            return 1;
        }
        else if(type1 > typeColor)
        	return 1;
        	return -1;
	}

    @Override
    public String toString()
	{
		return this.type + " " + this.intensity;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Color color = (Color) o;
        return type == color.type &&
                intensity == color.intensity;
    }

    @Override
    public int hashCode()
	{
		return this.type * 1000 + this.intensity;
	}
}

public class Histogram02 extends Configured implements Tool
{
	public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable>
	{
		@Override
		public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException
		{
			/* write your mapper code */
			Scanner sc = new Scanner(value.toString()).useDelimiter(",");

			Color red = new Color((short) 1, sc.nextShort());
			Color green = new Color((short) 2, sc.nextShort());
			Color blue = new Color((short) 3, sc.nextShort());

			sc.close();

			context.write(red,new IntWritable(1));
			context.write(green,new IntWritable(1));
			context.write(blue,new IntWritable(1));
		}
	}

    public static class HistogramInMapper extends Mapper<Object,Text,Color,IntWritable> {

	    public Hashtable<Color, Integer> H;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            H = new Hashtable<>();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /* write your mapper code */
            Scanner sc = new Scanner(value.toString()).useDelimiter(",");

            Color red = new Color((short) 1, sc.nextShort());
            Color green = new Color((short) 2, sc.nextShort());
            Color blue = new Color((short) 3, sc.nextShort());

            if(H.containsKey(red))
                H.put(red, H.get(red) + 1);
            else
                H.put(red, 1);

            if(H.containsKey(green))
                H.put(green, H.get(green) + 1);
            else
                H.put(green, 1);

            if(H.containsKey(blue))
                H.put(blue, H.get(blue) + 1);
            else
                H.put(blue, 1);

            sc.close();
        }

        // flush
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for(Color color : H.keySet())
                context.write(color, new IntWritable(H.get(color)));
        }
    }

	public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable>
    {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException
        {
            /* write your reducer code */
        	int sum=0;

        	for (IntWritable v: values)
                sum += v.get();

        	context.write(key, new IntWritable(sum));
        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable>
    {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException
        {
            /* write your reducer code */
        	long sum=0;

        	for (IntWritable v: values)
                sum += v.get();

        	context.write(key, new LongWritable(sum));
        }
    }

    @Override
    public int run( String[] args) throws Exception {
    Configuration conf = getConf();

    Job job1 = Job.getInstance(conf,"Job1");
    job1.setCombinerClass(HistogramCombiner.class);
    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf,"Job2");
    job2.waitForCompletion(true);
    return 0;
    }

	public static void main ( String[] args ) throws Exception {
        /* write your main program code */

		//ToolRunner.run(new Configuration(),new Histogram(),args);

    		// ----------------------------------------------------------------------Job 1


    		Job job = Job.getInstance();
            job.setJobName("Job1");

            job.setJarByClass(Histogram02.class);
            job.setMapperClass(HistogramMapper.class);
            job.setCombinerClass(HistogramCombiner.class);
            job.setReducerClass(HistogramReducer.class);

            job.setMapOutputKeyClass(Color.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Color.class);
            job.setOutputValueClass(LongWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // System Exit process

            job.waitForCompletion(true);

            // ------------------------------------------------------------------------Job 2
            Job job2 = Job.getInstance();
            job2.setJobName("Job2");

            job2.setJarByClass(Histogram02.class);
            job2.setMapperClass(HistogramInMapper.class);
            job2.setReducerClass(HistogramReducer.class);

            job2.setMapOutputKeyClass(Color.class);
            job2.setMapOutputValueClass(IntWritable.class);

            job2.setOutputKeyClass(Color.class);
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"2"));

            // System Exit process

            job2.waitForCompletion(true);
   }
}
