package com.yangrui.hadoop.partitioner;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Partitioner;

public class ArePartioner<KEY, VALUE>
  extends Partitioner<KEY, VALUE>
{
  private static Map<String, Integer> areaMap = new HashMap();
  
  static
  {
    areaMap.put("136", Integer.valueOf(0));
    areaMap.put("137", Integer.valueOf(1));
    areaMap.put("138", Integer.valueOf(2));
    areaMap.put("139", Integer.valueOf(3));
  }
  
  public int getPartition(KEY key, VALUE value, int numPartitions)
  {
    Integer integer = (Integer)areaMap.get(key.toString().substring(0, 3));
    return integer == null ? 4 : integer.intValue();
  }
}
