package cn.itcast.hadoop.mr.dc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean implements Writable{

	private String brand;
	
	private long tianmao;
	
	private long jingdong;
	
	private long total;
	
	
	public DataBean(){}
	
	public DataBean(String brand, long tianmao, long jingdong) {
		super();
		this.brand = brand;
		this.tianmao = tianmao;
		this.jingdong = jingdong;
		this.total = tianmao + jingdong;
	}
	
	

	@Override
	public String toString() {
		return this.brand+ "\t" + this.tianmao + "\t" + this.jingdong + "\t" + this.total;
	}

	// notice : 1 type 2 order
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(brand);
		out.writeLong(tianmao);
		out.writeLong(jingdong);
		out.writeLong(total);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.brand = in.readUTF();
		this.tianmao = in.readLong();
		this.jingdong = in.readLong();
		this.total = in.readLong();
		
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public long getTianmao() {
		return tianmao;
	}

	public void setTianmao(long tianmao) {
		this.tianmao = tianmao;
	}

	public long getJingdong() {
		return jingdong;
	}

	public void setJingdong(long jingdong) {
		this.jingdong = jingdong;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	

}
