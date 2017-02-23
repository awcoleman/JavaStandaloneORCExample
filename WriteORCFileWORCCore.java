import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/*
 * Basic code from https://orc.apache.org/docs/core-java.html#writing-orc-files
 *   Using Core Java - Writing ORC Files
 * 
 * 
 * orc-tools-X.Y.Z-uber.jar is required in the runtime classpath for io/airlift/compress/Decompressor
 * 
 * Creates myfile.orc AND .myfile.orc.crc, fails if myfile.orc exists.
 * 
 * awcoleman@gmail.com
 */
public class WriteORCFileWORCCore {

	public WriteORCFileWORCCore() throws IllegalArgumentException, IOException {
	
		String outfilename = "/tmp/myfile.orc";
		
		Configuration conf = new Configuration(false);
		
		/*
		 * Writer is in orc-core-1.2.1.jar and has dependencies on the
		 * Hadoop HDFS client libs
		 */
		TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
		Writer writer = OrcFile.createWriter(new Path(outfilename),
		                  OrcFile.writerOptions(conf)
		                         .setSchema(schema));
		
		/*
		 * VectorizedRowBatch and LongColumnVector are in hive-storage-api-2.1.1-pre-orc.jar
		 */
		VectorizedRowBatch batch = schema.createRowBatch();
		LongColumnVector x = (LongColumnVector) batch.cols[0];
		LongColumnVector y = (LongColumnVector) batch.cols[1];
		for(int r=0; r < 10000; ++r) {
		  int row = batch.size++;
		  x.vector[row] = r;
		  y.vector[row] = r * 3;
		  // If the batch is full, write it out and start over.
		  if (batch.size == batch.getMaxSize()) {
		    writer.addRowBatch(batch);
		    batch.reset();
		  }
		}
		//write last partial batch out
		writer.addRowBatch(batch);
		//close
		writer.close();
		
		//Output info to console
		System.out.println("Wrote "+writer.getNumberOfRows()+" records to ORC file "+(new Path(outfilename).toString()));
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		@SuppressWarnings("unused")
		WriteORCFileWORCCore mainobj = new WriteORCFileWORCCore();
	}

}