package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String sqlCommand = "CREATE TABLE IF NOT EXISTS usersTable(" +
                "id BIGINT NOT NULL AUTO_INCREMENT, name varchar(20) NOT NULL, lastName varchar(20) NOT NULL, age TINYINT NOT NULL, PRIMARY KEY (id))";
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate(sqlCommand);
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы");
        }
    }

    public void dropUsersTable() {
        String sqlCommand = "DROP TABLE IF EXISTS usersTable";
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate(sqlCommand);
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Ошибка при удалении таблицы");
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sqlCommand = "INSERT INTO usersTable(name, lastname, age) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlCommand)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User с именем – " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            System.err.println("Ошибка при добавлении пользователя");
        }

    }

    public void removeUserById(long id) {
        String sqlCommand = "DELETE FROM usersTable WHERE id = id";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.err.println("Ошибка при удалении пользователя");
        }

    }

    public List<User> getAllUsers() {
        List<User> list = new ArrayList<>();
        String sql = "SELECT NAME, LASTNAME, AGE, ID FROM usersTable";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                User user = new User(resultSet.getString(1),
                        resultSet.getString(2),
                        resultSet.getByte(3));
                user.setId(resultSet.getLong(4));
                list.add(user);
            }

        } catch (SQLException e) {
            System.err.println("Ошибка при выгрузке списка всех пользователей");
        }
        return list;
    }

    public void cleanUsersTable() {
        String sqlCommand = "TRUNCATE TABLE usersTable";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCommand);
        } catch (SQLException e) {
            System.err.println("Ошибка при очистке таблицы");
        }
    }
}
