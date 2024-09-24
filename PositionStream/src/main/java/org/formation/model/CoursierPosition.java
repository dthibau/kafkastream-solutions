package org.formation.model;

import java.util.Objects;

public class CoursierPosition {

	private String id;
	private Position position;
	public CoursierPosition() {

	}


	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public Position getPosition() {
		return position;
	}

	public void setPosition(Position position) {
		this.position = position;
	}

	@Override
	public String toString() {
		return "CoursierPosition{" +
				"id='" + id + '\'' +
				", position=" + position +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CoursierPosition that = (CoursierPosition) o;
		return Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id);
	}
}
