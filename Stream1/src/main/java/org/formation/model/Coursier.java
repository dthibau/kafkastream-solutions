package org.formation.model;

public class Coursier {

	private String id;
	private Position currentPosition;

	
	public Position getCurrentPosition() {
		return currentPosition;
	}

	public void setCurrentPosition(Position currentPosition) {
		this.currentPosition = currentPosition;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Courier [id=" + id + ", currentPosition=" + currentPosition + "]";
	}
	
	
}
