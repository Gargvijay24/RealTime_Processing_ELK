import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;


import java.io.File;
import java.util.Date;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class FileAlterationListenerImpl implements FileAlterationListener {

	@Override
	public void onDirectoryChange(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryCreate(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileChange(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileCreate(final File file) {
		// TODO Auto-generated method stub
		
		// System.out.println(file.getAbsoluteFile() + " was created.");
	      //  System.out.println("----------> length: " + file.length());
	        //System.out.println("----------> last modified: " + new Date(file.lastModified()));
	        //System.out.println("----------> readable: " + file.canRead());
	        //System.out.println("----------> writable: " + file.canWrite());
	        //System.out.println("----------> executable: " + file.canExecute());
		
	}

	@Override
	public void onFileDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStart(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStop(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}}
