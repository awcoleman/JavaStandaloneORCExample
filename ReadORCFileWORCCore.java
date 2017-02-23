import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

/*
 * Basic code from https://orc.apache.org/docs/core-java.html#reading-orc-files
 *   Using Core Java - Reading ORC FilesPermalink
 * 
 * Reads ORC file written by WriteORCFileWORCCore
 * 
 * orc-tools-X.Y.Z-uber.jar is required in the runtime classpath for io/airlift/compress/Decompressor
 * 
 * awcoleman@gmail.com
 */
public class ReadORCFileWORCCore {

	public ReadORCFileWORCCore() throws IllegalArgumentException, IOException {

		String infilename = "/tmp/myfile.orc";

		Configuration conf = new Configuration(false);

		Reader reader = OrcFile.createReader(new Path(infilename),
				OrcFile.readerOptions(conf));

		RecordReader rows = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();

		//Some basic info about the ORC file
		TypeDescription schema = reader.getSchema();
		long numRecsInFile = reader.getNumberOfRows();

		System.out.println("Reading ORC file "+(new Path(infilename).toString()));
		System.out.println("ORC file schema: "+schema.toJson());
		System.out.println("Number of records in ORC file: "+numRecsInFile);


		while (rows.nextBatch(batch)) {
			System.out.println("Processing Batch of records from ORC file. Number of records in Batch: "+batch.size);
			LongColumnVector field1 = (LongColumnVector) batch.cols[0];
			LongColumnVector field2 = (LongColumnVector) batch.cols[1];
			
			for(int r=0; r < batch.size; ++r) {
				int field1rowr = (int) field1.vector[r];
				int field2rowr = (int) field2.vector[r];

				System.out.println("In this batch, for row "+r+" in this batch, field1 is: "+field1rowr+" and field2 is: "+field2rowr);
			}
		}
		rows.close();
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException {
		@SuppressWarnings("unused")
		ReadORCFileWORCCore mainObj = new ReadORCFileWORCCore();
	}

}
