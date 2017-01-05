package com.example;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Main {

    static boolean autoComm;

    public static void main(String[] args) {
        if(args.length==1){
            autoComm = new Boolean(args[0]);
        }
        useTransactions(autoComm);

    }

    private static void useTransactions(boolean autoComm) {
        Connection conn = null;
        String newName = "Steve two";


        try {
            conn = DatabaseUtils.getInstance().getConnection();
            System.out.println(conn.getAutoCommit());
            conn.setAutoCommit(false);
            System.out.println(conn.getAutoCommit());

            String addPerson = "INSERT into person (name,dob) VALUES (?,?)";
            PreparedStatement pStmt = conn.prepareStatement(addPerson, Statement.RETURN_GENERATED_KEYS);

            LocalDate dob = LocalDate.of(1989,7,7);
            long epochMilliseconds = dob.atStartOfDay(ZoneId.systemDefault().systemDefault()).toEpochSecond()* 1000;
            pStmt.setString(1,newName);
            pStmt.setDate(2,new java.sql.Date(epochMilliseconds));
            pStmt.executeUpdate();

            String addAddress = "INSERT into address (street1,city,stateAbbr,zip,person_id) VALUES (?,?,?,?,?)";
            PreparedStatement pAddress = conn.prepareStatement(addAddress);
            pAddress.setString(1,"567 Houston");
            pAddress.setString(2,"Dallas");
            pAddress.setString(3,"TX");
            pAddress.setString(4,"77565");

            int genId = 0;
            try (ResultSet resultSet =pStmt.getGeneratedKeys()) {
                if (resultSet.next()) {
                    genId = resultSet.getInt(1);
                    pAddress.setInt(5,genId);
                }
                else {
                    throw new SQLException("Creating person failed, no ID obtained.");
                }
            }
            pAddress.executeUpdate();

            String addEmail = "INSERT into email (email,person_id) VALUES (?,?)";
            PreparedStatement pEmail = conn.prepareStatement(addEmail);
            pEmail.setString(1,"steven.smeven@aol.org");
            try (ResultSet resultSet =pStmt.getGeneratedKeys()) {
                if (resultSet.next()) {
                    pEmail.setInt(2,resultSet.getInt(1));
                }
                else {
                    throw new SQLException("Creating person failed, no ID obtained.");
                }
            }
            pEmail.executeUpdate();

            if (autoComm){
                System.out.println("commit");
                conn.commit();
            } else {
                System.out.println("rollback");
                conn.rollback();
            }

            String persons = "select * from person";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(persons);

            System.out.println("id,name,dob,gender,contacted");
            while (rs.next()){
                StringBuilder sb = new StringBuilder();
                sb.append(rs.getInt("id")+ ",");
                sb.append(rs.getString("name") + ",");

                Timestamp timestamp = rs.getTimestamp("dob");
                LocalDateTime ldt = timestamp.toLocalDateTime();
                LocalDate ld = LocalDate.from(ldt);
                sb.append(ld + ",");

                sb.append(rs.getString("gender") + ",");
                timestamp = rs.getTimestamp("contacted");
                if (timestamp != null) {
                    ldt = timestamp.toLocalDateTime();
                    ld = LocalDate.from(ldt);
                    sb.append(ld);
                }
                System.out.println(sb.toString());
            }


        } catch (SQLException e) {
            DatabaseUtils.printSQLException(e);

        } finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e1) {
                    DatabaseUtils.printSQLException(e1);
                }
            }
        }
    }
}
