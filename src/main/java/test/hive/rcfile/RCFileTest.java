package test.hive.rcfile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.SnappyCodec;

public class RCFileTest {

	private static final String RCFILE = "/user/do/testtable/000000_0";
	private static final String CFG_HDFS_SITE = "/home/do/Work/hadoop-2.3.0/etc/hadoop/hdfs-site.xml";
	private static final String CFG_CORE_SITE = "/home/do/Work/hadoop-2.3.0/etc/hadoop/core-site.xml";
	Configuration configuration;

	public static class Structure {
		public Structure(String col1, Long col2) {
			this.col1 = col1;
			this.col2 = col2;
		}

		String col1;
		Long col2;

	}

	public RCFileTest(Configuration cfg) {
		this.configuration = cfg;
	}

	public void createRCFile(final List<Structure> pRecords, final String rcFile)
			throws IOException, SerDeException {

		LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
		Properties props = getProperties(serde);
		serde.initialize(configuration, props);

		FileSystem fs = null;
		RCFile.Writer rcFileWriter = null;
		try {
			fs = FileSystem.get(this.configuration);
			final Path file = new Path(rcFile);

			RCFileOutputFormat.setColumnNumber(this.configuration, 2); // number
																		// of
																		// columns

			rcFileWriter = new RCFile.Writer(fs, this.configuration, file,
					null, new Metadata(), new SnappyCodec());

			StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(Structure.class,
							ObjectInspectorOptions.JAVA);

			for (Structure row : pRecords) {
				BytesRefArrayWritable braw = (BytesRefArrayWritable) serde
						.serialize(row, rowOI);
				rcFileWriter.append(braw);
			}
		} catch (final Exception e) {
			throw new IOException("Failed to create RC file " + rcFile, e);
		} finally {
			if (rcFileWriter != null) {
				try {
					rcFileWriter.close();
				} catch (IOException e) {
					System.err
							.println("Could not close RC File Writer for file:"
									+ rcFile + e.getMessage());
				} finally {
					rcFileWriter = null;
				}
			}
		}
	}

	private Properties getProperties(ColumnarSerDeBase serde)
			throws SerDeException {
		Properties props = new Properties();
		props.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
		props.setProperty("columns", "col1,col2");
		props.setProperty("columns.types", "string:bigint");
		props.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
		return props;
	}

	public static void main(String[] args) throws SQLException,
			ClassNotFoundException, InterruptedException, IOException,
			SerDeException {
		RCFileTest utils = new RCFileTest(createCfg());
		utils.createRCFile(generateRecords(), RCFILE);
	}

	private static Configuration createCfg() {
		Configuration cfg = new Configuration();
		cfg.addResource(new Path(CFG_CORE_SITE));
		cfg.addResource(new Path(CFG_HDFS_SITE));
		return cfg;
	}

	private static List<Structure> generateRecords() {
		List<Structure> records = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			records.add(new Structure("row" + i, (long) 1000000 + i));
		}
		return records;
	}

}
