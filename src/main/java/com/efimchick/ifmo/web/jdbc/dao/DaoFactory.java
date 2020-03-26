package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {

    private ResultSet getRs(String query) {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(query);
        } catch (SQLException exception) {
            return null;
        }
    }

    private Employee mapEmployee(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("ID"));
        FullName name = new FullName(rs.getString("FIRSTNAME"), rs.getString("LASTNAME"),
                rs.getString("MIDDLENAME"));
        Position pos = Position.valueOf(rs.getString("POSITION"));
        LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
        BigDecimal wage = new BigDecimal(rs.getString("SALARY"));
        BigInteger manager = BigInteger.valueOf(rs.getInt("MANAGER"));
        BigInteger deparmentID = BigInteger.valueOf(rs.getInt("DEPARTMENT"));
        return new Employee(
                id,
                name,
                pos,
                date,
                wage,
                manager,
                deparmentID
        );
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String query = "SELECT * FROM EMPLOYEE WHERE DEPARTMENT= " + department.getId();
                ResultSet rs = getRs(query);
                List<Employee> employee = new ArrayList<>();
                try {
                    while (rs.next()) {
                        employee.add(mapEmployee(rs));
                    }
                } catch (SQLException e) {
                    return null;
                }
                return employee;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String query = "SELECT * FROM EMPLOYEE WHERE MANAGER= " + employee.getId();
                ResultSet rs = getRs(query);
                List<Employee> employees = new ArrayList<>();
                try {
                    while (rs.next()) {
                        employees.add(mapEmployee(rs));
                    }
                } catch (SQLException e) {
                    return null;
                }
                return employees;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                String query = "SELECT * FROM EMPLOYEE WHERE ID= " + Id.toString();
                ResultSet rs = getRs(query);
                try {
                    assert rs != null;
                    if (rs.next()) {
                        return Optional.of(mapEmployee(rs));
                    }
                } catch (SQLException ignored) {
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                String query = "SELECT * FROM EMPLOYEE";
                ResultSet rs = getRs(query);
                List<Employee> employees = new ArrayList<>();
                try {
                    while (rs.next()) {
                        employees.add(mapEmployee(rs));
                    }
                } catch (SQLException e) {
                    return null;
                }
                return employees;
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    String query = "INSERT INTO EMPLOYEE VALUES (?,?,?,?,?,?,?,?,?)";
                    PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    preparedStatement.setInt(1, employee.getId().intValue());
                    preparedStatement.setString(2, employee.getFullName().getFirstName());
                    preparedStatement.setString(3, employee.getFullName().getLastName());
                    preparedStatement.setString(4, employee.getFullName().getMiddleName());
                    preparedStatement.setString(5, employee.getPosition().toString());
                    preparedStatement.setInt(6, employee.getManagerId().intValue());
                    preparedStatement.setDate(7, Date.valueOf(employee.getHired()));
                    preparedStatement.setDouble(8, employee.getSalary().doubleValue());
                    preparedStatement.setInt(9, employee.getDepartmentId().intValue());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    return null;
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                String query = "DELETE FROM EMPLOYEE WHERE ID= " + employee.getId();
                try {
                    ConnectionSource.instance().createConnection().prepareStatement(query).executeUpdate();
                } catch (SQLException e) {
                    e.getSQLState();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                String query = "SELECT * FROM DEPARTMENT WHERE ID= " + Id.toString();
                ResultSet rs = getRs(query);
                try {
                    assert rs != null;
                    if (rs.next()) {
                        return Optional.of(new Department(Id, rs.getString("NAME"), rs.getString("LOCATION")));
                    }
                } catch (SQLException ignored) {
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                String query = "SELECT * FROM DEPARTMENT";
                ResultSet rs = getRs(query);
                List<Department> employees = new ArrayList<>();
                try {
                    while (rs.next()) {
                        employees.add(new Department(new BigInteger(rs.getString("ID")), rs.getString("NAME"), rs.getString("LOCATION")));
                    }
                } catch (SQLException e) {
                    return null;
                }
                return employees;
            }

            @Override
            public Department save(Department department) {
                try {
                    ResultSet rs = ConnectionSource.instance().createConnection().createStatement().executeQuery("SELECT * FROM DEPARTMENT WHERE ID= " + department.getId());
                    if (!rs.next()) {
                        String query = "INSERT INTO DEPARTMENT VALUES (?,?,?)";
                        PreparedStatement preparedStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                        preparedStatement.setInt(1, department.getId().intValue());
                        preparedStatement.setString(2, department.getName());
                        preparedStatement.setString(3, department.getLocation());
                        preparedStatement.executeUpdate();
                    } else {
                        String query = "UPDATE DEPARTMENT SET NAME='" + department.getName() + "',LOCATION='" + department.getLocation() + "' WHERE ID=" + department.getId();
                        System.out.println(query);
                        Statement stmnt = ConnectionSource.instance().createConnection().createStatement();
                        stmnt.executeUpdate(query);
                    }

                } catch (SQLException e) {
                    return null;
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                try {
                    String query = "DELETE FROM DEPARTMENT WHERE ID= " + department.getId();
                    ConnectionSource.instance().createConnection().createStatement().executeUpdate(query);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
