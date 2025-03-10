package hadoop;

public class Record{
    private double sum;
    private int num;
    public Record(){
        this.sum = 0;
        this.num = 0;
    }
    public double getSum(){
        return sum;
    }
    public void setSum(double sum){
        this.sum = sum;
    }
    public int getNum(){
        return num;
    }
    public void setNum(int num){
        this.num = num;
    }
}