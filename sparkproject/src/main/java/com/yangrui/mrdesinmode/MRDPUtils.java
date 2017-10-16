package com.yangrui.mrdesinmode;

import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {

   private static IntWritable one =new IntWritable(1);
   public static Map<String,String> transformXmlToMap(String xml){


       Map<String,String> map=new HashMap<>();

       try{

           String [] tokens=xml.trim().substring(5,xml.trim().length()-3).split("\"");
           for(int i=0;i<tokens.length-1;i+=2){

               //id="123244"  id=  12345
               String key =tokens[i].trim().substring(0,tokens[i].trim().length()-1);

               String value=tokens[i+1];

               map.put(key,value);
           }

       }catch (Exception e){

           System.err.println(xml);

       }

       return map;
   }

}
