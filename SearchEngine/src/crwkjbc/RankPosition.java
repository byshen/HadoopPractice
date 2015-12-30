package crwkjbc;

import crwkjbc.Position;
public class RankPosition {
	float rank;
	Position positions;
	
	public RankPosition() {
		this.rank = 0;
		this.positions =new Position();
	}
	public RankPosition(float rank, boolean isTitle, int m_start, int m_end) {
		this.rank = rank;
		this.positions = new Position(isTitle, m_start, m_end);
	}
	public RankPosition add(float rank, boolean isTitle, int m_start, int m_end) {
		this.rank += rank;
		this.positions.add(isTitle, m_start, m_end);
		return this;
	}
	public float getRank() {
		return this.rank;
	}
	public String getPosition() {
		return this.positions.getPosition();
	}
}
