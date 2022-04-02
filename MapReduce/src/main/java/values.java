import org.apache.hadoop.io.IntWritable;

public class values extends IntWritable {
    private double cpu;
    private double disk;
    private double ram;
    private Long time;
    private double count;
    public values(double cpu, double disk, double ram, Long time) {
        this.cpu = cpu;
        this.disk = disk;
        this.ram = ram;
        this.time = time;
    }
    public values(double cpu, double disk, double ram, Long time,double count) {
        this.cpu = cpu;
        this.disk = disk;
        this.ram = ram;
        this.time = time;
        this.count = count;
    }
    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public void setDisk(double disk) {
        this.disk = disk;
    }

    public void setRam(double ram) {
        this.ram = ram;
    }

    public void setTime(Long time) {
        this.time = time;
    }
    public void setCount(double count){this.count=count;}
    public double getCpu() {
        return cpu;
    }

    public double getDisk() {
        return disk;
    }

    public double getRam() {
        return ram;
    }

    public Long getTime() {
        return time;
    }
    public double getCount(){return count;}

}
