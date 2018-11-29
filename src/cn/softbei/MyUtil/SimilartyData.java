package cn.softbei.MyUtil;

import java.io.Serializable;

public class SimilartyData implements Serializable{

	private String id;
	private String title;
	private Double similarty;

	public SimilartyData() {
		super();
		// TODO Auto-generated constructor stub
	}

	

	public SimilartyData(String id, String title, Double similarty) {
		super();
		this.id = id;
		this.title = title;
		this.similarty = similarty;
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



	public Double getSimilarty() {
		return similarty;
	}



	public void setSimilarty(Double similarty) {
		this.similarty = similarty;
	}

	
}
