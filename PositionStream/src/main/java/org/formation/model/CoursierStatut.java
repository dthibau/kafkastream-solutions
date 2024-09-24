package org.formation.model;

import java.util.Objects;

public class CoursierStatut {

	private String id;
	private String statut;
	private String firstName, lastName;

	public CoursierStatut() {
		super();
	}
	public CoursierStatut(String id, String statut, String firstName, String lastName) {
		this.id = id;
		this.statut = statut;
		this.firstName = firstName;
		this.lastName = lastName;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public String getStatut() {
		return statut;
	}

	public void setStatut(String statut) {
		this.statut = statut;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public String toString() {
		return "Coursier{" +
				"id='" + id + '\'' +
				", statut=" + statut +
				", firstName='" + firstName + '\'' +
				", lastName='" + lastName + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CoursierStatut that = (CoursierStatut) o;
		return Objects.equals(id, that.id);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id);
	}
}
