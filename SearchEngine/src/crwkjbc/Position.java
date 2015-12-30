package crwkjbc;

public class Position {
	StringBuilder position;
	
	public Position() {
		this.position = new StringBuilder();
	}
	public Position(boolean isTitle, int m_start, int m_end) {
		this.position = new StringBuilder();
		this.add(isTitle, m_start, m_end);
	}
	public String getPosition() {
		return position.toString().substring(0,this.position.length()-1);
	}
	public void add(boolean isTitle, int m_start, int m_end) {
		String tmp = "";
		if (isTitle) {
			tmp +="1|";
		} else {
			tmp += "0|";
		}
		tmp += String.valueOf(m_start) + "|";
		tmp += String.valueOf(m_end) + "%";	
		this.position.append(tmp);
	}
	
	
}
