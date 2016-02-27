import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
//import org.apache.hadoop.mapred.FileInputFormat;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Created by const on 1/24/16.
 */
public class MapreduceIT extends ClusterMapReduceTestCase {

    @Override
    protected void setUp() throws Exception {

        System.setProperty("hadoop.log.dir", "/tmp/logs");
        System.setProperty("hadoop.home", "C:\\Users\\chyzhykau_k\\Downloads\\hadoop-2.7.1\\hadoop-2.7.1");



        super.setUp();
    }

//    @Test
//    public void testMapReduce() throws SQLException {
//        try {
//            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
//        } catch (Exception e) {
//            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
//            e.printStackTrace();
//            return;
//        }
//        Connection c = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/mydb", "SA", "");
//        ResultSet rs = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
//                ResultSet.CONCUR_READ_ONLY).executeQuery("select count(*) from CASE_HISTORY");
//        rs.first();
//        assertEquals(5, rs.getInt("C1"));
//
//    }

    @Test
    public void testMapReduce() throws SQLException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Path inDir = new Path("testing/jobconf/input");
        Path outDir = new Path("testing/jobconf/output");

        OutputStream os = getFileSystem().create(new Path(inDir, "text.txt"));
        Writer wr = new OutputStreamWriter(os);
        wr.write("b a\n");
        wr.close();

        Job job = Job.getInstance(conf, "word count");


        job.setJobName("mr");

        //job.setJarByClass(WordCount2.class);
        job.setMapperClass(WordCountMapper.class);
        //job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);




//        conf.setJobName("mr");

//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(LongWritable.class);

//        conf.setMapperClass(WordCountMapper.class);
//        conf.setReducerClass(SumReducer.class);

        FileInputFormat.addInputPath(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);


        job.waitForCompletion(true);


        //assertTrue(JobClient.runJob(conf).isSuccessful());

        // Check the output is as expected
        Path[] outputFiles = FileUtil.stat2Paths(
                getFileSystem().listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));

        assertEquals(1, outputFiles.length);

        InputStream in = getFileSystem().open(outputFiles[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        assertEquals("a\t1", reader.readLine());
        assertEquals("b\t1", reader.readLine());
        assertNull(reader.readLine());
        reader.close();
    }
}
