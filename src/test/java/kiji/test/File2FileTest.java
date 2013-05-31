/**
 * 
 */
package kiji.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import kiji.controller.ControllerThread;

import org.testng.annotations.*;

/**
 * @author c.witschel@gmail.com
 * 
 */
public class File2FileTest {

	@BeforeClass
	public void init() throws Exception {
		// prepare an adapter

		// prepare adapter
		File testDir = new File(".");

		try {
			File adapterDir = new File("src/test/resources/" + this.getClass().getName() + "/conf");
			copyFolder(adapterDir, new File(testDir, "conf"));

			adapterDir = new File("target/classes/lib");
			copyFolder(adapterDir, new File(testDir, "lib"));

			adapterDir = new File("src/test/resources/"	+ this.getClass().getName() + "/in");
			copyFolder(adapterDir, new File(testDir, "in"));

			//we need some empty directories for archive and output
			new File(testDir, "out").mkdir();
			new File(testDir, "archive").mkdir();

		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("test could not be prepared", e);
		}

	}

	@Test
	public void singleFileTest() {
		ControllerThread.getInstance().start();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		ControllerThread.getInstance().shutdown();

		assert new File("out/README").exists() : "File did not arrive at target";
		assert !new File("in/README").exists() : "File was not send";
		String[] archivedFiles = new File("archive/").list();
		assert archivedFiles.length == 1 : "File was not archived. Expected 1. Found "+archivedFiles.length;

		System.out.println("Test was successfull");
	}

	@AfterClass
	public void cleanUp() {
		// remove all test artifacts

		// remove test directory
		deleteFolder(new File("conf"));
		deleteFolder(new File("lib"));
		deleteFolder(new File("in"));
		deleteFolder(new File("out"));
		deleteFolder(new File("archive"));

	}

	private void deleteFolder(File file) {
		
		if (file.isDirectory()){
			
			// list all the directory contents
			File files[] = file.listFiles();

			for (File nextFile : files) {
				
				// recursive delete
				deleteFolder(nextFile);
			}
			
			//now that the folder is empty, it should be ok to delete it
			if (!file.delete())
				System.out.println("could not delete file: "+file.getName());
		}
		else{
			if (!file.delete())
				System.out.println("could not delete file: "+file.getName());
		}
	}

	/*
	 * recursively copies from source to target
	 */
	public static void copyFolder(File source, File target) throws IOException {

		// check whether this is a directory or file
		if (source.isDirectory()) {

			// if the destination directory does not exist create it
			if (!target.exists()) {
				target.mkdir();
				System.out.println("Created target directory: " + target);
			}

			// list all the directory contents
			String files[] = source.list();

			for (String file : files) {
				// prepare next recursion
				File sourceDir = new File(source, file);
				File targetDir = new File(target, file);
				// recursive copy
				copyFolder(sourceDir, targetDir);
			}

		} else {
			// if file, then copy it
			// Use bytes stream to support all file types
			InputStream in = new FileInputStream(source);
			OutputStream out = new FileOutputStream(target);

			byte[] buffer = new byte[1024];

			int length;
			// copy the file content in bytes
			while ((length = in.read(buffer)) > 0) {
				out.write(buffer, 0, length);
			}

			in.close();
			out.close();
			System.out.println("File copied from " + source + " to " + target);
		}
	}
}
