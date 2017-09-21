package com.lijie.lixianxiangmu.pre;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class WebLogBean
  implements Writable
{
  private boolean valid = true;
  private String remote_addr;
  private String remote_user;
  private String time_local;
  private String request;
  private String status;
  private String body_bytes_sent;
  private String http_referer;
  private String http_user_agent;
  
  public void set(boolean valid, String remote_addr, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent)
  {
    this.valid = valid;
    this.remote_addr = remote_addr;
    this.remote_user = remote_user;
    this.time_local = time_local;
    this.request = request;
    this.status = status;
    this.body_bytes_sent = body_bytes_sent;
    this.http_referer = http_referer;
    this.http_user_agent = http_user_agent;
  }
  
  public String getRemote_addr()
  {
    return this.remote_addr;
  }
  
  public void setRemote_addr(String remote_addr)
  {
    this.remote_addr = remote_addr;
  }
  
  public String getRemote_user()
  {
    return this.remote_user;
  }
  
  public void setRemote_user(String remote_user)
  {
    this.remote_user = remote_user;
  }
  
  public String getTime_local()
  {
    return this.time_local;
  }
  
  public void setTime_local(String time_local)
  {
    this.time_local = time_local;
  }
  
  public String getRequest()
  {
    return this.request;
  }
  
  public void setRequest(String request)
  {
    this.request = request;
  }
  
  public String getStatus()
  {
    return this.status;
  }
  
  public void setStatus(String status)
  {
    this.status = status;
  }
  
  public String getBody_bytes_sent()
  {
    return this.body_bytes_sent;
  }
  
  public void setBody_bytes_sent(String body_bytes_sent)
  {
    this.body_bytes_sent = body_bytes_sent;
  }
  
  public String getHttp_referer()
  {
    return this.http_referer;
  }
  
  public void setHttp_referer(String http_referer)
  {
    this.http_referer = http_referer;
  }
  
  public String getHttp_user_agent()
  {
    return this.http_user_agent;
  }
  
  public void setHttp_user_agent(String http_user_agent)
  {
    this.http_user_agent = http_user_agent;
  }
  
  public boolean isValid()
  {
    return this.valid;
  }
  
  public void setValid(boolean valid)
  {
    this.valid = valid;
  }
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(this.valid);
    sb.append("\001").append(getRemote_addr());
    sb.append("\001").append(getRemote_user());
    sb.append("\001").append(getTime_local());
    sb.append("\001").append(getRequest());
    sb.append("\001").append(getStatus());
    sb.append("\001").append(getBody_bytes_sent());
    sb.append("\001").append(getHttp_referer());
    sb.append("\001").append(getHttp_user_agent());
    return sb.toString();
  }
  
  public void readFields(DataInput in)
    throws IOException
  {
    this.valid = in.readBoolean();
    this.remote_addr = in.readUTF();
    this.remote_user = in.readUTF();
    this.time_local = in.readUTF();
    this.request = in.readUTF();
    this.status = in.readUTF();
    this.body_bytes_sent = in.readUTF();
    this.http_referer = in.readUTF();
    this.http_user_agent = in.readUTF();
  }
  
  public void write(DataOutput out)
    throws IOException
  {
    out.writeBoolean(this.valid);
    out.writeUTF(this.remote_addr == null ? "" : this.remote_addr);
    out.writeUTF(this.remote_user == null ? "" : this.remote_user);
    out.writeUTF(this.time_local == null ? "" : this.time_local);
    out.writeUTF(this.request == null ? "" : this.request);
    out.writeUTF(this.status == null ? "" : this.status);
    out.writeUTF(this.body_bytes_sent == null ? "" : this.body_bytes_sent);
    out.writeUTF(this.http_referer == null ? "" : this.http_referer);
    out.writeUTF(this.http_user_agent == null ? "" : this.http_user_agent);
  }
}

