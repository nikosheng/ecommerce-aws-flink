package flink.aws.generator.input;

import com.csvreader.CsvWriter;
import flink.aws.generator.util.Args;
import flink.aws.generator.util.RandomArgs;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class RandomUserBehavior {
    public static void main(String[] args) throws CmdLineException {
//        String filePath = "/Users/jiasfeng/IdeaProjects/ecommerce-aws-flink/ecommerce-aws-flink-generator/src/main/resources/UserBehavior_random_10000.csv";
        RandomArgs arguments = new RandomArgs();
        CmdLineParser parser = new CmdLineParser(arguments);
        parser.parseArgument(args);
        String filePath = arguments.getFile();
        CsvConfig csvConfig = new CsvConfig("utf-8","yyyy-MM-dd HH:mm:ss:SSS",',');
        CSVUtils csvUtil = new CSVUtils();

        ArrayList<UserBehavior> behaviors = new ArrayList<>();
        Random random = new Random();

        List<Pair<String, Integer>> behaviorList = new ArrayList<>();
        behaviorList.add(new Pair<>("pv", 8));
        behaviorList.add(new Pair<>("fav", 2));
        behaviorList.add(new Pair<>("cart", 3));
        behaviorList.add(new Pair<>("buy", 1));
        WeightRandom<String, Integer> weightRandom = new WeightRandom<>(behaviorList);

        final int SIZE = 10000;
//        final String[] BEHAVIORS = new String[]{
//                "pv",
//                "fav",
//                "buy",
//                "cart"
//        };

        int[] USERIDS = new int[100];

        for (int i = 0; i < 100; i++) {
            USERIDS[i] = random.nextInt(10000) + 10000;
        }

        int[] ITEMIDS = new int[10];

        for (int i = 0; i < 10; i++) {
            ITEMIDS[i] = random.nextInt(100000) + 10000;
        }

        int[] CATEGORYIDS = new int[5];

        for (int i = 0; i < 5; i++) {
            CATEGORYIDS[i] = random.nextInt(50000) + 10000;
        }

        for (int i = 0; i < SIZE; i++) {
            Long ts = Instant.now().getEpochSecond() + 10 * i;
            UserBehavior behavior = new UserBehavior(
                    USERIDS[random.nextInt(100)],
                    ITEMIDS[random.nextInt(10)],
                    CATEGORYIDS[random.nextInt(5)],
                    weightRandom.random(),
                    ts
            );
            behaviors.add(behavior);
        }


        csvUtil.writeFile(filePath, behaviors, UserBehavior.class, csvConfig);
    }

    private static class CSVUtils {
        private final Logger logger = LoggerFactory.getLogger(CSVUtils.class);

        public <T> boolean writeFile(String filePath, List<T> cacheContainer, Class<T> t, CsvConfig csvConfig) {
            CsvWriter writer;
            if (StringUtils.isBlank(filePath) || CollectionUtils.isEmpty(cacheContainer) || null == t) {
                return false;
            }
            this.checkCsvConfig(csvConfig);
            writer = new CsvWriter(filePath, csvConfig.getSeparator(), Charset.forName(csvConfig.getCharset()));
            //获取实体类中的属性
            Field[] fields = t.getDeclaredFields();
            //生成数据头
            String[] headers = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                headers[i] = fields[i].getName();
            }
            //写入到文件
            try {
//                writer.writeRecord(headers);
                for (T obj : cacheContainer) {
                    String[] str = coverFieldsValue(obj,csvConfig);
                    writer.writeRecord(str);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            writer.close();
            return true;
        }

        /**
         * 把缓存内容写入到csv文件
         * 默认编码格式是本地编码格式
         * @param filePath       文件路径
         * @param cacheContainer 缓存容器
         * @return 写入是否成功
         */
        public <T> boolean writeFile(String filePath, List<T> cacheContainer, Class<T> t) {
            return this.writeFile(filePath,cacheContainer,t,this.initCsvConfig());
        }

        /**
         * 把传入的实例属性的值封装成字符串数组
         *
         * @param obj       实例
         * @return          字符串数组
         * @throws Exception    异常
         */
        private <T>  String[] coverFieldsValue(T obj, CsvConfig csvConfig) throws Exception {
            String[] result ;
            Class<?> clazz = obj.getClass();
            Field[] fields = clazz.getDeclaredFields();
            if (null == fields || fields.length <= 0) {
                return null;
            }
            result = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                new Date();
                String methodName = "get" + fields[i].getName().substring(0, 1).toUpperCase() + fields[i].getName().substring(1);
                Method method = clazz.getMethod(methodName);
                Object value = method.invoke(obj);
                if(null == value){
                    continue;
                }
                if("Date".equals(fields[i].getType().getSimpleName())){
                    Date date = (Date)value;
                    result[i] = new SimpleDateFormat(csvConfig.getDateFormat()).format(date);
                    continue;
                }
                result[i] = value.toString();
            }
            return result;
        }

        /**
         * 构造一个默认的配置实例
         * 默认编码格式为本地系统编码格式
         * @return      设有默认值的配置实例
         */
        private CsvConfig initCsvConfig(){
            String charset = System.getProperty("file.encoding");
            return new CsvConfig(charset,"yyyy-MM-dd HH:mm:ss:SSS",',');
        }

        /**
         * 检测给予系统配置信息的完整性
         * @param csvConfig        给定的配置实例
         */
        private void checkCsvConfig(CsvConfig csvConfig){
            if(null == csvConfig){
                csvConfig = initCsvConfig();
            }else{
                if(null == csvConfig.getCharset()){
                    csvConfig.setCharset(System.getProperty("file.encoding"));
                }
                if(null == csvConfig.getDateFormat()){
                    csvConfig.setDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                }
                //没有指定分隔符
                if(0 == csvConfig.getSeparator()){
                    csvConfig.setSeparator(',');
                }
            }
        }



    }

    private static class CsvConfig {
        /** 字符编码 */
        private String charset;
        /** 日期格式 */
        private String dateFormat;
        /** 分隔符 */
        private char separator;

        public CsvConfig(){

        }
        public CsvConfig(String charset, String dateFormat, char separator){
            this.charset = charset;
            this.dateFormat = dateFormat;
            this.separator = separator;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = charset;
        }

        public String getDateFormat() {
            return dateFormat;
        }

        public void setDateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public char getSeparator() {
            return separator;
        }

        public void setSeparator(char separator) {
            this.separator = separator;
        }

    }
}


