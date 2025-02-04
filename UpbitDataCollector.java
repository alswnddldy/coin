package com.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class UpbitDataCollector {

    // MySQL 설정
    private static final String DB_HOST = "localhost";
    private static final String DB_USER = "alswnddldy";
    private static final String DB_PASSWORD = "1234";
    private static final String DB_NAME = "alswnddldy";
    private static final String TABLE_NAME = "upbit_data_java";

    // 상태 파일 경로
    private static final String STATUS_FILE = "/home/rlaalswnd/바탕화면/progress_java.txt";

    // 수집할 코인 목록
    private static final Map<String, String> COINS = new HashMap<>();

    static {
        COINS.put("ICX", "KRW-ICX");
        COINS.put("AERGO", "KRW-AERGO");
        COINS.put("META", "KRW-META");
        COINS.put("BORA", "KRW-BORA");
        COINS.put("BTC", "KRW-BTC");
        COINS.put("ETH", "KRW-ETH");
        COINS.put("XRP", "KRW-XRP");
        COINS.put("SOL", "KRW-SOL");
        COINS.put("USDT", "KRW-USDT");
        COINS.put("ADA", "KRW-ADA");
    }

    // 데이터베이스 생성
    private static void createDatabaseIfNotExists() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://" + DB_HOST, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            String createDbQuery = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
            statement.executeUpdate(createDbQuery);
        }
    }

    // 테이블 생성
    private static void createTableIfNotExists() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://" + DB_HOST + "/" + DB_NAME, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "date DATE NOT NULL," +
                "code VARCHAR(20) NOT NULL," +
                "opening_price DOUBLE NOT NULL," +
                "closing_price DOUBLE NOT NULL," +
                "high_price DOUBLE NOT NULL," +
                "low_price DOUBLE NOT NULL," +
                "volume DOUBLE NOT NULL," +
                "prev_closing_price DOUBLE NOT NULL," +
                "UNIQUE(date, code)" +
                ");", TABLE_NAME);
            statement.executeUpdate(createTableQuery);
        }
    }

    // API 요청
    private static JSONArray fetchData(String market, String toDate) {
        try {
            String apiUrl = String.format("https://api.upbit.com/v1/candles/days?count=1&market=%s&to=%sT00:00:00", market, toDate);
            HttpURLConnection connection = (HttpURLConnection) new URL(apiUrl).openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                return new JSONArray(response.toString());
            } else {
                System.out.println("API 요청 실패: " + responseCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // 데이터베이스 저장
    private static void saveDataToDb(JSONArray dataArray, String market) throws SQLException {
        String insertQuery = String.format(
            "INSERT IGNORE INTO %s " +
            "(date, code, opening_price, closing_price, high_price, low_price, volume, prev_closing_price) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);", TABLE_NAME);

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://" + DB_HOST + "/" + DB_NAME, DB_USER, DB_PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {

            for (int i = 0; i < dataArray.length(); i++) {
                JSONObject data = dataArray.getJSONObject(i);
                String date = data.getString("candle_date_time_kst").substring(0, 10);
                double openingPrice = data.getDouble("opening_price");
                double closingPrice = data.getDouble("trade_price");
                double highPrice = data.getDouble("high_price");
                double lowPrice = data.getDouble("low_price");
                double volume = data.getDouble("candle_acc_trade_volume");
                double prevClosingPrice = data.getDouble("prev_closing_price");

                preparedStatement.setString(1, date);
                preparedStatement.setString(2, market);
                preparedStatement.setDouble(3, openingPrice);
                preparedStatement.setDouble(4, closingPrice);
                preparedStatement.setDouble(5, highPrice);
                preparedStatement.setDouble(6, lowPrice);
                preparedStatement.setDouble(7, volume);
                preparedStatement.setDouble(8, prevClosingPrice);

                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    // 상태 파일 읽기
    private static String getLastProcessedDate() {
        try (BufferedReader reader = new BufferedReader(new FileReader(STATUS_FILE))) {
            return reader.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    // 상태 파일 업데이트
    private static void updateStatusFile(String lastDate) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(STATUS_FILE))) {
            writer.write(lastDate);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            createDatabaseIfNotExists();
            createTableIfNotExists();

            String lastProcessedDate = getLastProcessedDate();
            LocalDate startDate = (lastProcessedDate != null)
                ? LocalDate.parse(lastProcessedDate)
                : LocalDate.of(2015, 1, 1);

            LocalDate endDate = LocalDate.now();

            while (!startDate.isAfter(endDate)) {
                String currentDate = startDate.format(DateTimeFormatter.ISO_DATE);

                for (Map.Entry<String, String> coin : COINS.entrySet()) {
                    String market = coin.getValue();
                    System.out.println("데이터 수집 중: " + market + " 날짜: " + currentDate);
                    JSONArray data = fetchData(market, currentDate);
                    if (data != null && data.length() > 0) {
                        saveDataToDb(data, market);
                    }
                }

                System.out.println("날짜별 데이터 수집 완료: " + currentDate);
                startDate = startDate.plusDays(1);
            }

            updateStatusFile(LocalDate.now().format(DateTimeFormatter.ISO_DATE));
            System.out.println("모든 데이터 수집 완료.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
