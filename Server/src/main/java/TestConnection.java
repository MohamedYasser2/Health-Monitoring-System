
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;

public class TestConnection {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-master:9000"),conf);
        LocalDate date = LocalDate.now();
        String directoryName = "/"+date.toString()+"/data.log";
        Path path = new Path(directoryName);
        fileSystem.mkdirs(path);
        if(fileSystem instanceof DistributedFileSystem) {
            System.out.println("HDFS is the underlying filesystem");
        }
        else {
            System.out.println("Other type of file system "+fileSystem.getClass());
        }
    }
}