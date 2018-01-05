package data.streaming.dto;

public class Rating {
	private String idProject1, idProject2;
	private Double score;

	public Rating() {
		this.score = 0.0;
	}

	public Rating(String idProject1, String idProject2, Double score) {
		super();
		this.idProject1 = idProject1;
		this.idProject2 = idProject2;
		this.score = score;
	}
	
	public Boolean constains(String one, String two) {
		Boolean result = false;
		
		if((this.idProject1.equals(one) && this.idProject2.equals(two)) ||
				(this.idProject1.equals(two) && this.idProject2.equals(one))) {
			result = true;
		}
		
		return result;
	}

	public String getIdProject1() {
		return idProject1;
	}

	public void setIdProject1(String idProject1) {
		this.idProject1 = idProject1;
	}

	public String getIdProject2() {
		return idProject2;
	}

	public void setIdProject2(String idProject2) {
		this.idProject2 = idProject2;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idProject1 == null) ? 0 : idProject1.hashCode());
		result = prime * result + ((idProject2 == null) ? 0 : idProject2.hashCode());
		result = prime * result + ((score == null) ? 0 : score.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Rating other = (Rating) obj;
		if (idProject1 == null) {
			if (other.idProject1 != null)
				return false;
		} else if (!idProject1.equals(other.idProject1))
			return false;
		if (idProject2 == null) {
			if (other.idProject2 != null)
				return false;
		} else if (!idProject2.equals(other.idProject2))
			return false;
		if (score == null) {
			if (other.score != null)
				return false;
		} else if (!score.equals(other.score))
			return false;
		return true;
	}


	public String toString() {
		return "Rating [idProject1=" + idProject1 + ", idProject2=" + idProject2 + ", score=" + score + "]";
	}

}
