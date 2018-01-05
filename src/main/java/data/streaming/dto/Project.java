package data.streaming.dto;

import java.util.ArrayList;
import java.util.List;

public class Project {
	
	private String researcher, name, type, startDate, endDate, idProject;
	private List<String> researchers;
	private List<String> keywords;
	
	public Project() {
		this.researchers = new ArrayList<String>();
		this.keywords = new ArrayList<String>();
	}

	

	public List<String> getResearchers() {
		return researchers;
	}

	public void setResearchers(List<String> researchers) {
		this.researchers = researchers;
	}

	public void addResearcher(String researcher) {
		this.researchers.add(researcher);
	}

	public String getResearcher() {
		return researcher;
	}

	public void setResearcher(String researcher) {
		this.researcher = researcher;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getIdProject() {
		return idProject;
	}

	public void setIdProject(String idProject) {
		this.idProject = idProject;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}
	
	public void addKeyword(String keyword) {
		this.keywords.add(keyword);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((startDate == null) ? 0 : startDate.hashCode());
		result = prime * result + ((researchers == null) ? 0 : researchers.hashCode());
		result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
		result = prime * result + ((idProject == null) ? 0 : idProject.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((keywords == null) ? 0 : keywords.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((researcher == null) ? 0 : researcher.hashCode());
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
		Project other = (Project) obj;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		if (researchers == null) {
			if (other.researchers != null)
				return false;
		} else if (!researchers.equals(other.researchers))
			return false;
		if (endDate == null) {
			if (other.endDate != null)
				return false;
		} else if (!endDate.equals(other.endDate))
			return false;
		if (idProject == null) {
			if (other.idProject != null)
				return false;
		} else if (!idProject.equals(other.idProject))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (keywords == null) {
			if (other.keywords != null)
				return false;
		} else if (!keywords.equals(other.keywords))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (researcher == null) {
			if (other.researcher != null)
				return false;
		} else if (!researcher.equals(other.researcher))
			return false;
		
		return true;
	}

	public String toString() {
		return "Project ("+idProject+") [researcher=" + researcher
				+ ", name=" + name + ", type=" + type + ", startDate=" + startDate + ", endDate=" + endDate
				+ ", researchers=" + researchers + ", keywords=" + keywords + "]";
	}	
	
}
