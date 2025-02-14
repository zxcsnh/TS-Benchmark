import cn.edu.ruc.start.TSBM;

public class TSDBTest {
    private static String dataPath = "";
    private static int thread_num = 1;
    private static int cacheLine = 400;
    public static void main(String[] args) throws Exception {
        System.out.println("datapath: "+args[2]);
        System.out.println("adapter: "+args[0]);
        System.out.println("method: "+args[1]);
        System.out.println("thread_num: "+args[3]);
        System.out.println("cacheLine: "+args[4]);
        // 3-i 4-w 5-r 6-ir 7-wr
        dataPath = args[2];
        thread_num = Integer.parseInt(args[3]);
        cacheLine = Integer.parseInt(args[4]);
        boolean loadParam = false, appendParam = false, queryParam = false, irParam = false;
        if ("1".equals(args[1])) {
            loadParam = true;
            appendParam = true;
            queryParam = true;
            irParam = false;
        } else if ("2".equals(args[1])) {
            loadParam = false;
            appendParam = true;
            queryParam = true;
            irParam = false;
        } else if ("3".equals(args[1])) {
            loadParam = true;
            appendParam = false;
            queryParam = false;
            irParam = false;
        } else if ("4".equals(args[1])) {
            loadParam = false;
            appendParam = true;
            queryParam = false;
            irParam = false;
        } else if ("5".equals(args[1])) {
            loadParam = false;
            appendParam = false;
            queryParam = true;
            irParam = false;
        } else if ("6".equals(args[1])) {
            loadParam = false;
            appendParam = false;
            queryParam = false;
            irParam = true;
        } else if ("7".equals(args[1])) {
            loadParam = false;
            appendParam = true;
            queryParam = true;
            irParam = false;
        }
        
        String className = "";
        String ip = "";
        String port = "";
        String userName = "";
        String passwd = "";
        // Influxdb
        if ("1".equals(args[0])) {
            className = "cn.edu.ruc.InfluxdbAdapter";
            ip = "127.0.0.1";
            port = "8086";
            userName = "root";
            passwd = "root";
        }
        // Timescaledb
        else if ("2".equals(args[0])) {
            className = "cn.edu.ruc.TimescaledbAdapter";
            ip = "127.0.0.1";
            port = "5432";
            userName = "postgres";
            passwd = "postgres";
        }
        // Iotdb
        else if ("3".equals(args[0])) {
            className = "cn.edu.ruc.IotdbAdapter";
            ip = "127.0.0.1";
            port = "6667";
            userName = "root";
            passwd = "root";
        }
        // Opentsdb
        else if ("4".equals(args[0])) {
            className = "cn.edu.ruc.OpentsdbAdapter";
            ip = "127.0.0.1";
            port = "4242";
            userName = "root"; // not required
            passwd = "root"; // not required
        }
        // Druid
        else if ("5".equals(args[0])) {
            className = "cn.edu.ruc.DruidAdapter";
            ip = "127.0.0.1";
            port = "";
            userName = "root"; // not required
            passwd = "root"; // not required
        }
        //暂时为空
        else if ("6".equals(args[0])) {
            
        }
        // TDengine
        else if ("7".equals(args[0])) {
            className = "cn.edu.ruc.TdengineAdapter2";
            ip = "127.0.0.1";
            port = "6030";
            userName = "root"; // not required
            passwd = "taosdata"; // not required
        }
        if ("0".equals(args[1])) {
            TSBM.generateData(dataPath); //生成数据
        } else {
            TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd, thread_num, cacheLine, false,
                loadParam, appendParam, queryParam, irParam);
        }
    }

}
