package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//Переделал на вариант без try with resourses, т.к. зависал тест, а причину не смог найти

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        Connection conn = null;
        Statement statement = null;
        String sqlCommand = "CREATE TABLE IF NOT EXISTS usersTable(" +
                "id BIGINT NOT NULL AUTO_INCREMENT, name varchar(20) NOT NULL, lastName varchar(20) NOT NULL, age TINYINT NOT NULL, PRIMARY KEY (id))";
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы");
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void dropUsersTable() {
        Connection conn = null;
        Statement statement = null;
        String sqlCommand = "DROP TABLE IF EXISTS usersTable";
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.err.println("Ошибка при удалении таблицы");
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String sqlCommand = "INSERT INTO usersTable(name, lastname, age) VALUES (?, ?, ?)";
        try {
            conn = Util.getConnection();
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(sqlCommand);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            conn.commit();
            System.out.println("User с именем – " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public void removeUserById(long id) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String sqlCommand = "DELETE FROM usersTable WHERE id = ?";
        try {
            conn = Util.getConnection();
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(sqlCommand);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public List<User> getAllUsers() {
        Connection conn = null;
        Statement statement = null;
        List<User> list = new ArrayList<>();
        String sql = "SELECT NAME, LASTNAME, AGE, ID FROM usersTable";
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                User user = new User(resultSet.getString(1),
                        resultSet.getString(2),
                        resultSet.getByte(3));
                user.setId(resultSet.getLong(4));
                list.add(user);
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при выгрузке списка всех пользователей");
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    public void cleanUsersTable() {
        Connection conn = null;
        Statement statement = null;
        String sqlCommand = "TRUNCATE TABLE usersTable";
        try {
            conn = Util.getConnection();
            conn.setAutoCommit(false);
            statement = conn.createStatement();
            statement.executeUpdate(sqlCommand);
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
                System.err.println(e.getMessage());
                System.err.println("Transaction rollback");
            } catch (SQLException ex) {
                System.err.println(ex.getMessage());
                System.err.println("There was an error making a rollback");
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
}
