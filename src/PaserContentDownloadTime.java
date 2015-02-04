import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

public class PaserContentDownloadTime {
	
	static HashMap<String,DownLoadInfo> map = new HashMap<String,DownLoadInfo>();
	
	public static void main(String[] args) throws Exception {
		
		ArrayList<String> machineList = new ArrayList<String>() {

			private static final long serialVersionUID = 1L;
			
			{
			//PRD rendering servers
//			add("ec2-54-145-22-115.compute-1.amazonaws.com");
//			add("ec2-54-147-159-236.compute-1.amazonaws.com");
//			add("ec2-107-20-105-128.compute-1.amazonaws.com");
//			add("ec2-54-147-24-107.compute-1.amazonaws.com");
//			add("ec2-54-147-134-77.compute-1.amazonaws.com");
//			add("ec2-54-167-63-142.compute-1.amazonaws.com");
//			add("ec2-50-16-190-80.compute-1.amazonaws.com");
//			add("ec2-54-91-2-170.compute-1.amazonaws.com");
//			add("ec2-54-147-156-90.compute-1.amazonaws.com");
			
			//STG rendering servers
			add("ec2-54-83-58-127.compute-1.amazonaws.com");
			}
		};
		
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-M-d");
		Date beginDate = sdf1.parse("2015-1-13");
		Date endDate = sdf1.parse("2015-1-14");
		Date d = beginDate;
		
		while(!d.equals(endDate)) {

			String strDate = sdf2.format(d).toString();
			System.out.println(strDate);


			for (String renderServer : machineList) {
				String sURL = "http://" + renderServer + "/get?file=..%5Clog%5Cconvert-" + strDate + ".log";
				
				parserDownloadLog(parserLogFile(sURL),sURL);
			}
			c.setTime(d);
			c.add(Calendar.DATE,1);
			d = c.getTime();
		}
		
		long downloadSum = 0;
		int avgDwonloadTime = 0;
		int count = 0;
		for (String k:map.keySet()){
			if (map.get(k).downloadSuccess == true) {
				System.out.println(map.get(k).downloadTime);
				downloadSum = downloadSum + map.get(k).downloadTime;
				count = count + 1;
				//System.out.println(map.get(k).bucket);
				System.out.println(k);
			}

		}

		
		avgDwonloadTime = (int) (downloadSum/count);
		
		System.out.println(avgDwonloadTime);
		System.out.println(downloadSum);
		System.out.println(count);
	}
	
	public static ArrayList<String> parserLogFile(String strUrl) throws Exception {

			URL url = new URL(strUrl);
			BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
			ArrayList<String> lines = new ArrayList<String>();
			String str = "";
			while ((str = br.readLine()) != null) {
				if (str.startsWith("[DOWNLOADER]")) {
					lines.add(str);
				}
			}
				br.close();
			return lines;

	}
	
	public static void parserDownloadLog(ArrayList<String> strLog,String url) throws ParseException {
		for(String line:strLog) {
			SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
			String contentId = "";
			String fileName = "";
			String id_fileName = "";
			String S3bucket = "";
			Date sTime = null;
			Date eTime = null;
			long dTime = 0;
			DownLoadInfo di = new DownLoadInfo();
			
			if (line.contains("download begin:")) {

				//Parser S3 bucket name
				Pattern p1 = Pattern.compile("https?://(.*).amazonaws.com");
				Matcher m1 = p1.matcher(line);
				while(m1.find()) {
					S3bucket = m1.group(1);
					di.bucket = S3bucket;
				}
				//Parser content id
				Pattern p2 = Pattern.compile("/i/([^/]+)/");
				//Pattern p2 = Pattern.compile("/i/([a-z0-9]+-.*)/");
				Matcher m2 = p2.matcher(line);
				while(m2.find()) {
					contentId = m2.group(1);
				}
				//Parser file name
				Pattern p4 = Pattern.compile("/([^/]+)$");
				Matcher m4 = p4.matcher(line);
				while(m4.find()) {
					fileName = m4.group(1);
				}
				
				id_fileName = contentId + "/" + fileName;
				
				//Parser download start time
				Pattern p3 = Pattern.compile("\\], 	\\[(.*)\\]");
				Matcher m3 = p3.matcher(line);
				while(m3.find()) {
					String start = m3.group(1);
					//System.out.println(id_fileName + line);
					sTime = sdf.parse(start);
					di.startTime = sTime;
				}
			}else if (line.contains("download and save to local successfully:")) {
				
				di.downloadSuccess = true;
				//parser content id
				//Pattern p1 = Pattern.compile("/content/([a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+)/");
				Pattern p1 = Pattern.compile("/content/(.*)/");
				Matcher m1 = p1.matcher(line);
				while(m1.find()) {
					contentId = m1.group(1);
				}
				//parser file name
				Pattern p3 = Pattern.compile("/([^/]+)$");
				Matcher m3 = p3.matcher(line);
				while(m3.find()) {
					fileName = m3.group(1);
					id_fileName = contentId + "/" + fileName;
//					System.out.println(id_fileName + line);
//					System.out.println(url);
					
					if (map.containsKey(id_fileName)) {
						sTime = map.get(id_fileName).startTime;
						di.startTime = sTime;
						di.bucket = map.get(id_fileName).bucket;
					}
				}
				
				Pattern p2 = Pattern.compile("\\], 	\\[(.*)\\]");
				Matcher m2 = p2.matcher(line);
				while(m2.find()) {
					String end = m2.group(1);
					eTime = sdf.parse(end);
					di.endTime = eTime;
					if (map.containsKey(id_fileName) && map.get(id_fileName).startTime != null) {
						dTime = (eTime.getTime() - sTime.getTime())/1000;
						di.downloadTime = dTime;
					}
				}
			}else if(line.contains("download failed")){
				
				di.downloadSuccess = false;
				
				//Parser content id
				Pattern p1 = Pattern.compile("/i/([^/]+)/");
				Matcher m1 = p1.matcher(line);
				while(m1.find()) {
					contentId = m1.group(1);
				}
				//Parser file name
				Pattern p3 = Pattern.compile("/([^/]+)$");
				Matcher m3 = p3.matcher(line);
				while(m3.find()) {
					fileName = m3.group(1);
				}
				
				id_fileName = contentId + "/" + fileName;
				
				//Parser S3 bucket name
				Pattern p2 = Pattern.compile("https?://(.*).amazonaws.com");
				Matcher m2 = p2.matcher(line);
				while(m2.find()) {
					S3bucket = m2.group(1);
					di.bucket = S3bucket;
				}
			}
			
			map.put(id_fileName, di);
		}
		
	} 
}
class DownLoadInfo {
	public boolean downloadSuccess;
	public String bucket;
	public Date startTime;
	public Date endTime;
	public long downloadTime;
}
