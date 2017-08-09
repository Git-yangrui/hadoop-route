package com.yangrui.hadoop.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class FlowCountBean
  implements WritableComparable<FlowCountBean>
{
  private String phoneNum;
  private long up_flow;
  private long dw_flow;
  private long sum_flow;
  
  public void set(String phoneNum, long up_flow, long dw_flow)
  {
    this.phoneNum = phoneNum;
    this.up_flow = up_flow;
    this.dw_flow = dw_flow;
    this.sum_flow = (up_flow + dw_flow);
  }
  
  public String getPhoneNum()
  {
    return this.phoneNum;
  }
  
  public void setPhoneNum(String phoneNum)
  {
    this.phoneNum = phoneNum;
  }
  
  public long getUp_flow()
  {
    return this.up_flow;
  }
  
  public void setUp_flow(long up_flow)
  {
    this.up_flow = up_flow;
  }
  
  public long getDw_flow()
  {
    return this.dw_flow;
  }
  
  public void setDw_flow(long dw_flow)
  {
    this.dw_flow = dw_flow;
  }
  
  public long getSum_flow()
  {
    return this.sum_flow;
  }
  
  public void setSum_flow(long sum_flow)
  {
    this.sum_flow = sum_flow;
  }
  
  public void write(DataOutput out)
    throws IOException
  {
    out.writeUTF(this.phoneNum);
    out.writeLong(this.up_flow);
    out.writeLong(this.dw_flow);
    out.writeLong(this.sum_flow);
  }
  
  public void readFields(DataInput in)
    throws IOException
  {
    this.phoneNum = in.readUTF();
    this.up_flow = in.readLong();
    this.dw_flow = in.readLong();
    this.sum_flow = in.readLong();
  }
  
  public int compareTo(FlowCountBean o)
  {
    return this.sum_flow > o.getSum_flow() ? -1 : 1;
  }
  
  public String toString()
  {
    return this.up_flow + "\t" + this.dw_flow + "\t" + this.sum_flow;
  }
}
