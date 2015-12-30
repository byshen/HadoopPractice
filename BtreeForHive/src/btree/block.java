package btree;

import java.util.ArrayList;

public class block implements Comparable<block> {

	/*
	 * 块值
	 */
	int value;
	/*
	 * Record list
	 */
	 ArrayList<Record> list;
	 
	 public block(){
		 list = new ArrayList<Record>();
	 }
	 public void add(Record d){
		 list.add(d);
	 }
	public void listShow(){
		for(Record i:list){
			i.show();
		}
	}
	public boolean isEmpty(){
		return list==null;
	}
	 /**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(block arg0) {
		
		return 0;
	}

}
