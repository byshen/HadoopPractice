import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

 
public class copyFile {
    //initialization
    static Configuration conf = new Configuration();
    static FileSystem hdfs;
    static FileSystem local;
    static {
        conf.set("fs.default.name","hdfs://localhost:9000");
        
        try {
            hdfs = FileSystem.get(conf);
            local = FileSystem.getLocal(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
    	//Path src = new Path("/home/byshen/HadoopInput/xjbz2.c");
    	//Path dst = new Path("/user/byshen/input");
    	Path inputDir = new Path("/home/byshen/视频");
    	Path acceptDir = new Path("/user/byshen/AcceptFile");
    	hdfs.mkdirs(acceptDir);
    	
    	
    	System.out.println("===== Mkdir ======\n");
    	
    	FileStatus files[] = local.listStatus(inputDir);
    	FSDataOutputStream out;
    	int i = 0;
    	while(files.length >0) {
    		//System.out.println(files[i].getPath().getName());
    		FSDataInputStream in = local.open(files[i].getPath());
    		out = hdfs.create(new Path("/user/byshen/AcceptFile/" + files[i].getPath().getName()));
    		byte buffer[] = new byte[256];
    		int bytesRead = 0;
    		while( (bytesRead = in.read(buffer)) > 0) {
    			out.write(buffer,0,bytesRead);
    		}
    		out.close();
    		in.close();
    		
    		File toDelete = new File("/home/byshen/视频/" + files[i].getPath().getName());
    		
    		toDelete.delete();
    		if(toDelete.exists()) {
    			System.out.println("delete " + files[i].getPath().getName() + "failed");	
    		}
    		files = local.listStatus(inputDir);
    		//System.out.println(files.length + " videos remained...");
    	}
    	System.out.println("all transfered success ... ");
    }
}

