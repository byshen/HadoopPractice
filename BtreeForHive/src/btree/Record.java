package btree;

public class Record  {

	public int id;
	public String name;
	public int unum;
	public String context;
	
	public Record(int a, String b, int c, String d){
		this.id = a;
		this.name = b;
		this.unum = c;
		this.context = d;
	}
	
	public Record(String[] list){
		this.id = Integer.valueOf(list[0]);
		this.name = list[1];
		this.unum = Integer.valueOf(list[2]);
		this.context = list[3];
	}
	
	public void show(){
		System.out.println(this.id + this.name + this.unum + this.context);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Record a = new Record(1, "s", 4, "s");
		Record b = new Record(1, "s", 4, "s");
	
	}

}
