package btree;


import java.util.Random; 
	 
public class BplusTree implements B { 
	     
	    /** 根节点 */ 
	    protected Node root; 
	     
	    /** 阶数，M值 */ 
	    protected int order; 
	     
	    /** 叶子节点的链表头*/ 
	    protected Node head; 
	     
	    public Node getHead() { 
	        return head; 
	    } 
	 
	    public void setHead(Node head) { 
	        this.head = head; 
	    } 
	 
	    public Node getRoot() { 
	        return root; 
	    } 
	 
	    public void setRoot(Node root) { 
	        this.root = root; 
	    } 
	 
	    public int getOrder() { 
	        return order; 
	    } 
	 
	    public void setOrder(int order) { 
	        this.order = order; 
	    } 
	 
	    @Override 
	    public Object get(Comparable key) { 
	        return root.get(key); 
	    } 
	 
	    @Override 
	    public void remove(Comparable key) { 
	        root.remove(key, this); 
	 
	    } 
	 
	    @Override 
	    public void insertOrUpdate(Comparable key, Object obj) { 
	        root.insertOrUpdate(key, obj, this); 
	 
	    } 
	     
	    public BplusTree(int order){ 
	        if (order < 3) { 
	            System.out.print("order must be greater than 2"); 
	            System.exit(0); 
	        } 
	        this.order = order; 
	        root = new Node(true, true); 
	        head = root; 
	    } 
	     
	    //测试 
	    public static void main(String[] args) { 
	        BplusTree tree = new BplusTree(4); 
	        Random random = new Random(); 
	        long current = System.currentTimeMillis(); 
	        for (int j = 0; j < 100; j++) { 
	            for (int i = 0; i < 100; i++) { 
	                int randomNumber = random.nextInt(10); 
	                Record tmp = new Record(1, "s", randomNumber, "2");
	                tree.insertOrUpdate(1, tmp); 
	            } 
	        } 
	        System.out.println(tree.toString());
	        long duration = System.currentTimeMillis() - current; 
	        System.out.println("time elpsed for duration: " + Double.valueOf(duration/1000.0)+"s"); 
	        //Record a = (Record)tree.get(new Record(1, "s", 3, "2"));
	        //if(a!=null){
	        	//System.out.println(a.context);
	        //}
	    } 
	 
}


