package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet resultSet) {
                Set<Employee> employee = new LinkedHashSet<>();
                try {
                    while (resultSet.next()) {
                        employee.add(map(resultSet));
                    }
                }
                catch (SQLException e) {
                    throw new IllegalArgumentException();
                }
                return employee;
            }
        };
    }

    private Employee map(ResultSet rs) throws SQLException{
        BigInteger id = new BigInteger(rs.getString("ID"));
        FullName name = new FullName(rs.getString("FIRSTNAME"), rs.getString("LASTNAME"),
                rs.getString("MIDDLENAME"));
        Position pos = Position.valueOf(rs.getString("POSITION"));
        LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
        BigDecimal wage = new BigDecimal(rs.getString("SALARY"));
        Employee manager = mapManager(rs);
        return new Employee(
                id,
                name,
                pos,
                date,
                wage,
                manager
        );
    }

    private Employee mapManager(ResultSet rs) throws SQLException{
        int pointer = rs.getRow();

        if (Integer.parseInt(rs.getString("MANAGER") == null ? "0" : rs.getString("MANAGER")) == 0) return null;
        int ID = Integer.parseInt(rs.getString("MANAGER"));
        Employee manager = null;
        rs.beforeFirst();
        while(rs.next())  {
            if (Integer.parseInt(rs.getString("ID")) == ID) manager = map(rs);
        }
        rs.absolute(pointer);
        return manager;
    }
}
