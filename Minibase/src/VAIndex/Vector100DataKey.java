package VAIndex;
 
import global.Vector100Dtype;
import btree.KeyClass;
 
public class Vector100DataKey extends KeyClass {
    private Vector100Dtype vec;
    public Vector100DataKey(Vector100Dtype vec){
        this.vec = vec;
    }
    public Vector100Dtype getVec() {
        return vec;
    }
    public void setVec(Vector100Dtype vec) {
        this.vec = vec;
    }
 
}