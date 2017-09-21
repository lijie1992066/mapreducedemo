package com.lijie.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TextPair implements Writable, DBWritable {
	
	private int id;
	private String email;
	private String name;
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((email == null) ? 0 : email.hashCode());
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextPair other = (TextPair) obj;
		if (email == null) {
			if (other.email != null)
				return false;
		} else if (!email.equals(other.email))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	public TextPair(int id, String email, String name) {
		super();
		this.id = id;
		this.email = email;
		this.name = name;
	}
	
	public TextPair() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getEmail() {
		return email;
	}
	
	public void setEmail(String email) {
		this.email = email;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public void readFields(ResultSet arg0) throws SQLException {
		// TODO Auto-generated method stub
		id = arg0.getInt(1);
		email = arg0.getString(2);
		name = arg0.getString(3);
	}
	
	@Override
	public void write(PreparedStatement arg0) throws SQLException {
		// TODO Auto-generated method stub
		arg0.setInt(1, id);
		arg0.setString(2, email);
		arg0.setString(3, name);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		id = arg0.readInt();
		String email = arg0.readUTF();
		String name = arg0.readUTF();
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(id);
		arg0.writeUTF(email);
		arg0.writeUTF(name);
	}
	
}
