package flink.aws.generator.input;

import com.csvreader.CsvReader;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class UserBehaviorCsvFileReader implements Supplier<UserBehavior> {

    private final String filePath;
    private CsvReader csvReader;

    public UserBehaviorCsvFileReader(String filePath) throws IOException {

        this.filePath = filePath;
        try {
            csvReader = new CsvReader(filePath);
            csvReader.readHeaders();
        } catch (IOException e) {
            throw new IOException("Error reading records from file: " + filePath, e);
        }
    }

    @Override
    public UserBehavior get() {
        UserBehavior userBehavior = null;
        try{
            if(csvReader.readRecord()) {
                csvReader.getRawRecord();
                userBehavior = new UserBehavior(
                        Long.valueOf(csvReader.get(0)),
                        Long.valueOf(csvReader.get(1)),
                        Long.valueOf(csvReader.get(2)),
                        csvReader.get(3),
                        Long.valueOf(csvReader.get(4))*1000L);
            }
        } catch (IOException e) {
            throw new NoSuchElementException("IOException from " + filePath);
        }

        if (null == userBehavior) {
            throw new NoSuchElementException("All records read from " + filePath);
        }

        return userBehavior;
    }
}
