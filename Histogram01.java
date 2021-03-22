import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


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
}

public class Histogram01
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

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
        	long sum=0;

        	for (IntWritable v: values)
                sum += v.get();

        	context.write(key, new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */

            Job job = Job.getInstance();
            job.setJobName("MyJob");

            job.setJarByClass(Histogram01.class);
            job.setMapperClass(HistogramMapper.class);
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
   }
}
