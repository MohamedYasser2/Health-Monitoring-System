package hadoop;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetConvert extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "ParquetConvert");
        job.setJarByClass(getClass());
        job.setMapperClass(ParquetConvertMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // setting schema
        AvroParquetOutputFormat.setSchema(job, parseSchema());
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop-master:9000/outputs/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/outputs1/"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ParquetConvertMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {



        @Override
        public void map(LongWritable key, Text value, Context context) {
            List<GenericData.Record> recordList = new ArrayList<>();
            GenericRecord record = new GenericData.Record(parseSchema());
            String[] strings = value.toString().split(",");
            String[] valueStrings = strings[1].split("\t");
            SimpleDateFormat f = new SimpleDateFormat("yyyy MM dd HH:mm");
            Date d = null;
            try {
                d = f.parse(strings[0]);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            long milliseconds = d. getTime();

            record.put("serviceName", valueStrings[0]);
            record.put("maxCPU", Double.parseDouble(valueStrings[1]));
            record.put("maxRAM", Double.parseDouble(valueStrings[2]));
            record.put("maxDisk", Double.parseDouble(valueStrings[3]));
            record.put("stamp", milliseconds);
            record.put("count", Double.parseDouble(valueStrings[5]));
            recordList.add((GenericData.Record) record);
            try {
                Schema schema = parseSchema();

                Path path = new Path("/home/hadoopuser/DuckDB/data_1.parquet");

                File file = new File(path.toString());


                if (file.exists()) {
                    ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(path).build();
                    GenericData.Record nextRecord = reader.read();
                    while (nextRecord != null) {
                        recordList.add(nextRecord);
                        nextRecord = reader.read();
                    }
                }
                try (org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                        .withSchema(schema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                        .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                        .withConf(new Configuration())
                        .withValidation(false)
                        .withDictionaryEncoding(false)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build()) {

                    for (GenericData.Record record1 : recordList) {
                        writer.write(record1);
                    }
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

//            context.write(null, record);
        }

    }

        private static Schema parseSchema() {


            String schemaJson = "{\"namespace\": \"BidData\"," //Not used in Parquet, can put anything
                    + "\"type\": \"record\"," //Must be set as record
                    + "\"name\": \"BigDataRecord\"," //Not used in Parquet, can put anything
                    + "\"fields\": ["
                    + " {\"name\": \"serviceName\",  \"type\": \"string\"}"
                    + ", {\"name\": \"maxCPU\", \"type\": \"double\"}" //Required field
                    + ", {\"name\": \"maxRAM\", \"type\": \"double\"}" //Required field
                    + ", {\"name\": \"maxDisk\", \"type\": \"double\"}" //Required field
                    + ", {\"name\": \"count\", \"type\": \"int\"}" //Required field
                    + ", {\"name\": \"stamp\", \"type\": \"long\"}" //Required field
                    + " ]}";

            Schema.Parser parser = new Schema.Parser().setValidate(true);
            return parser.parse(schemaJson);
        }
    }
