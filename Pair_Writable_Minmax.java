
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.Comparator;
public class Pair_Writable_Minmax implements Writable, Comparable<Pair_Writable_Minmax>{
		private long star;
		private long count;
		
		public Pair_Writable_Minmax() {
			
		}
		public Pair_Writable_Minmax(Pair_Writable_Minmax pair){
			this.star = pair.getStar();
			this.count = pair.getCount();
		}

		public void write(DataOutput out) throws IOException{
			out.writeLong(star);
			out.writeLong(count);
		}

		public void readFields(DataInput in) throws IOException{
			star = in.readLong();
			count = in.readLong();
		}

		public long getStar(){
			return star;
		}

		public long getCount(){
			return count;
		}

		public void set(long star, long count){
			this.star = star;
			this.count = count;
		}

		@Override
		public int compareTo(Pair_Writable_Minmax o) {
			// TODO Auto-generated method stub
			return (int)(this.star - o.getStar());
		}
	
}
