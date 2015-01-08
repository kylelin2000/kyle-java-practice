package idv.kyle.practice.solr.mr;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.noggit.CharArr;
import org.noggit.JSONUtil;

public class Util 
{	
	private final static SimpleDateFormat sdf_solr_readable = new SimpleDateFormat("yyyy-MM-dd' 'HH-mm-ss");
	private final static SimpleDateFormat sdf_solr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
	//final SimpleDateFormat sdf_SQL = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss");
	
	static public void getUserReadableString(String rawStr, CharArr out)
	{				
		char[] strByChars = rawStr.toCharArray();
		
		JSONUtil.writeStringPart(strByChars, 0, strByChars.length, out);					
	}
	
	
	static public String IP_longToStr(long val_IP) throws UnknownHostException
	{							
		byte[] bytes = java.math.BigInteger.valueOf(val_IP).toByteArray();
		
		InetAddress address = InetAddress.getByAddress(bytes);	
		
		return address.getHostAddress();							
	}
	
	static public String secToSolrDate(long sec)
	{
		return milliSecToSolrDate(sec * 1000L);
	}
	
	static public String milliSecToSolrDate(long milliSec)
	{									
		Date date = new Date(milliSec);
		
		return Util.sdf_solr.format(date);
	}
	
	static public String SolrDateToSearchableFormat(String date_Solr)
	{
		Date d;
		try 
		{
			d = Util.sdf_solr.parse(date_Solr);
		} 
		catch (ParseException e) 
		{				
			e.printStackTrace();
			return date_Solr;
		}
				
		return Util.sdf_solr_readable.format(d);
	}
	
	/*
	static public String SQLDateToSolrDate(String date_sql)
	{
		//ex : 05/16/2015 08:15:34			
		Date d;
		try 
		{
			d = Util.sdf_SQL.parse(date_sql);
		} 
		catch (ParseException e) {				
			e.printStackTrace();
			return date_sql;
		}
		
		//String xx = this.sdf_solr.format(d);
		return Util.sdf_solr.format(d);
	}
	*/
}
