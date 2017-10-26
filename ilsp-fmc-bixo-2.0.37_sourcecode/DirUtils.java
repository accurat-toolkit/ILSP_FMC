package gr.ilsp.fmc.utils;



import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DirUtils {
	private static final Logger LOGGER = Logger.getLogger(DirUtils.class);
	
	@SuppressWarnings("deprecation")
	public static void clearPreviousLoopDir(FileSystem fs, Path outputPath, int curLoop) {
		Path dir;
		Path crawldb;
		String dirName = "";
		int index = 0;
		try {
			FileStatus[] files = fs.listStatus(outputPath);
			for (FileStatus status: files){
				if (!status.isDir()) continue;
				dir = status.getPath();
				dirName = dir.getName();
				try {
					index = Integer.parseInt(dirName.split("-")[0]);
				} catch (NumberFormatException e){
					continue;
				}
				crawldb = new Path(dir,CrawlConfig.CRAWLDB_SUBDIR_NAME);
				if (index == curLoop-2 && fs.exists(crawldb)){
					fs.delete(crawldb);
					return;
				}				
			}
		} catch (IOException e) {
			LOGGER.info(e.getMessage());
		}
	}
	

}
