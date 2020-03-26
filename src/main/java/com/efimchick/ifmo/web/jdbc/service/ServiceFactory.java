package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ServiceFactory {

    private ResultSet getRs(String query) {
        try {
            return ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query);
        } catch (SQLException exception) {
            return null;
        }
    }

    private Department mapDep(BigInteger id) {
        ResultSet rs = getRs("SELECT * FROM DEPARTMENT WHERE ID=" + id);
        Department dep;
        try {
            rs.next();
            dep = new Department(new BigInteger(rs.getString("ID")), rs.getString("NAME"), rs.getString("LOCATION"));
        } catch (SQLException e) {
            return null;
        }
        return dep;
    }

    private Employee mapManager(BigInteger id) {
        ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE ID=" + id);

        try {
            rs.next();
            BigInteger Id = new BigInteger(rs.getString("ID"));
            FullName name = new FullName(rs.getString("FIRSTNAME"), rs.getString("LASTNAME"),
                    rs.getString("MIDDLENAME"));
            Position pos = Position.valueOf(rs.getString("POSITION"));
            LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
            BigDecimal wage = new BigDecimal(rs.getString("SALARY"));
            Department dep = rs.getString("DEPARTMENT") == null ? null : mapDep(new BigInteger(rs.getString("DEPARTMENT")));
            Employee manager = null;
            return new Employee(
                    Id,
                    name,
                    pos,
                    date,
                    wage,
                    manager,
                    dep
            );
        } catch (SQLException e) {
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
        Department dep = rs.getString("DEPARTMENT") == null ? null : mapDep(new BigInteger(rs.getString("DEPARTMENT")));
        Employee manager = rs.getString("MANAGER") == null ? null : mapManager(new BigInteger(rs.getString("MANAGER")));
        return new Employee(
                id,
                name,
                pos,
                date,
                wage,
                manager,
                dep
        );
    }

    private List<Employee> page(List<Employee> list, Paging paging) {
        int cur = paging.itemPerPage;
        int start = paging.itemPerPage * (paging.page - 1);
        return list.subList(start, Math.min(cur * paging.page, list.size()));
    }

    private List<Employee> rsToList(ResultSet rs, Paging paging) {
        List<Employee> employees = new ArrayList<>();
        try {
            while (rs.next()) {
                employees.add(mapEmployee(rs));
            }
            return page(employees, paging);
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> rsToList(ResultSet rs) {
        List<Employee> employees = new ArrayList<>();
        try {
            while (rs.next()) {
                employees.add(mapEmployee(rs));
            }
            return employees;
        } catch (SQLException e) {
            return null;
        }
    }

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY HIREDATE");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY LASTNAME");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY SALARY");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY DEPARTMENT, LASTNAME");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY HIREDATE");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY SALARY");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY LASTNAME");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY LASTNAME");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY HIREDATE");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY SALARY");
                assert rs != null;
                List<Employee> employee = rsToList(rs, paging);
                return employee;
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE ID=" + employee.getId());
                Employee emp = null;
                try {
                    rs.next();
                    emp = mapEmployeeWMC(rs);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return emp;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY SALARY DESC");
                assert rs != null;
                List<Employee> employees = rsToList(rs);
                return employees.get(salaryRank - 1);
            }
        };
    }

    private Employee mapEmployeeWMC(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("ID"));
        FullName name = new FullName(rs.getString("FIRSTNAME"), rs.getString("LASTNAME"),
                rs.getString("MIDDLENAME"));
        Position pos = Position.valueOf(rs.getString("POSITION"));
        LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
        BigDecimal wage = new BigDecimal(rs.getString("SALARY"));
        Department dep = rs.getString("DEPARTMENT") == null ? null : mapDep(new BigInteger(rs.getString("DEPARTMENT")));
        Employee manager = rs.getString("MANAGER") == null ? null : mapManagerWMC(new BigInteger(rs.getString("MANAGER")));
        return new Employee(
                id,
                name,
                pos,
                date,
                wage,
                manager,
                dep
        );
    }

    private Employee mapManagerWMC(BigInteger id) {
        ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE ID=" + id);

        try {
            rs.next();
            BigInteger Id = new BigInteger(rs.getString("ID"));
            FullName name = new FullName(rs.getString("FIRSTNAME"), rs.getString("LASTNAME"),
                    rs.getString("MIDDLENAME"));
            Position pos = Position.valueOf(rs.getString("POSITION"));
            LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
            BigDecimal wage = new BigDecimal(rs.getString("SALARY"));
            Department dep = rs.getString("DEPARTMENT") == null ? null : mapDep(new BigInteger(rs.getString("DEPARTMENT")));
            Employee manager = rs.getString("MANAGER") == null ? null : mapManagerWMC(new BigInteger(rs.getString("MANAGER")));
            return new Employee(
                    Id,
                    name,
                    pos,
                    date,
                    wage,
                    manager,
                    dep
            );
        } catch (SQLException e) {
            return null;
        }
    }
}
