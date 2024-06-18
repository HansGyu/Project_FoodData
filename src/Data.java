import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Data {
    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/foodsearch";
    private static final String DATABASE_USER = "root";
    private static final String DATABASE_PASSWORD = "1234";
    private static final int BATCH_SIZE = 500;

    public static void main(String[] args) {
        String csvFilePath = "C:\\Users\\gchan\\Documents\\식단데이터.csv";
        uploadCSVToDatabase(csvFilePath, "fooddata");
    }

    public static void uploadCSVToDatabase(String csvFilePath, String tableName) {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
             FileReader fileReader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            connection.setAutoCommit(false); // 트랜잭션 시작

            String sql = generateInsertSQL(tableName);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                int count = 0;

                for (CSVRecord csvRecord : csvParser) {
                    try {
                        int index = 1;
                        preparedStatement.setString(index++, csvRecord.get("식품코드"));
                        preparedStatement.setString(index++, csvRecord.get("식품명"));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("에너지(kcal)")));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("단백질(g)")));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("지방(g)")));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("탄수화물(g)")));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("당류(g)")));
                        preparedStatement.setFloat(index++, parseFloat(csvRecord.get("나트륨(mg)")));
                        preparedStatement.addBatch();

                        if (++count % BATCH_SIZE == 0) {
                            preparedStatement.executeBatch();
                        }
                    } catch (SQLException | NumberFormatException e) {
                        System.out.println("Error inserting data for record: " + csvRecord.toString());
                        e.printStackTrace();
                    }
                }

                preparedStatement.executeBatch(); // 남아있는 배치 실행
                connection.commit(); // 트랜잭션 커밋
                System.out.println("Data uploaded to table: " + tableName);
                System.out.println("Total records inserted: " + count);
            } catch (SQLException e) {
                connection.rollback(); // 트랜잭션 롤백
                throw e;
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateInsertSQL(String tableName) {
        return "INSERT INTO " + tableName + " (`식품코드`, `식품명`, `에너지(kcal)`, `단백질(g)`, `지방(g)`, `탄수화물(g)`, `당류(g)`, `나트륨(mg)`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private static float parseFloat(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0f; // 기본값을 0.0f로 설정
        }
        return Float.parseFloat(value);
    }
}
