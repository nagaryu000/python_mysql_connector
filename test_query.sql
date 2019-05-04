use mysql_connector_python;

select * from salaries;

select first_name, last_name, hire_date from employees 
where hire_date between '2019-01-01' AND '2019-12-31';