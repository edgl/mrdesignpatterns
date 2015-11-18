package designpatterns;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TestJson {

	private static final SimpleDateFormat frmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
	@Test
	public void testName() throws Exception {
		
		JsonParser parser = new JsonParser();
		JsonElement el = parser.parse( 
				new FileReader(
						new File(TestJson.class.getClassLoader().getResource("sample.json").toURI())));
		
//		BufferedReader rd = new BufferedReader(new FileReader(new File(TestJson.class.getClassLoader().getResource("sample.json").toURI())));
		
//		System.out.println(rd.readLine());
		
		JsonObject obj = el.getAsJsonObject();
		JsonArray arr = obj.get("statuses").getAsJsonArray();
		
		Iterator<JsonElement> elems = arr.iterator();
		
		int counter = 0;
		while(elems.hasNext()) {
			counter++;
			JsonObject jObj =  elems.next().getAsJsonObject();
			String created_at = jObj.get("created_at").getAsString();
			Date dt_created_at = frmt.parse(created_at);
			String userId = jObj.get("user").getAsJsonObject().get("id").getAsString();			
			
		}
		
		Assert.assertEquals(15, counter);
		
		
	}
}
