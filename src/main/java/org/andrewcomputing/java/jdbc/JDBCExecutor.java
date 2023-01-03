package org.andrewcomputing.java.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class JDBCExecutor {
    public static void main (String[] args){
        DatabaseConnectionManager databaseConnectionManager
                = new DatabaseConnectionManager("localhost","hplussport","postgres","mysecretpassword");
        try{
            Connection connection = databaseConnectionManager.getConnection();
            CustomerDAO customerDAO = new CustomerDAO(connection);
            System.out.println("PAGED");
            System.out.println("");
            for(int i=1; i<3; i++){
                System.out.println("Page number: "+i);
                customerDAO.findAllPaged(10,i).forEach(System.out::println);
            }
        }catch(SQLException sqlException){
            sqlException.printStackTrace();
        }catch(Exception exception){
            exception.printStackTrace();
        }
    }
}
