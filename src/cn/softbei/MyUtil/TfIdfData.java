package cn.softbei.MyUtil;

import java.io.Serializable;

public class TfIdfData implements Serializable{
	private String id;
	private String title;
	private String segment;
	
	
	
	public TfIdfData() {
		super();
		// TODO Auto-generated constructor stub
	}
	public TfIdfData(String id, String title, String segment) {
		super();
		this.id = id;
		this.title = title;
		this.segment = segment;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getSegment() {
		return segment;
	}
	public void setSegment(String segment) {
		this.segment = segment;
	}


}
