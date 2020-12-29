select * from staff;
select * from company_regions;
select * from company_divisions;

---query that return all of those employees that work in the domestic division
---and start date of employee is greater than all of the start date of employee that work in the Tools dept
select last_name, salary, start_date, department from staff
where department IN (select department from company_divisions
					 where company_division = 'Domestic')
AND start_date > ALL (select start_date from staff
					where Department = 'Tools')
order by salary;				

--categirized the salary
select last_name,start_date, gender, salary,
case
	when salary < 80000 then 'Under_paid'
	when salary > 80000 and salary < 140000 then 'Well_paid'
	when salary > 140000 then 'Executive'
End as category
from staff
order by salary;

--How many are under paid, well paid and executive?
select category, count(*) from 
(select last_name,start_date, gender, salary,
  case
	when salary < 80000 then 'Under_paid'
	when salary > 80000 and salary < 140000 then 'Well_paid'
	when salary > 140000 then 'Executive'
End as category
from staff
order by salary) sub
group by sub.category;

---


--compute the average of the salaries while excluding the smallest salary and the highest salary from the record.
select to_char(round(avg(salary)),'$999,999.999') from staff
where salary not in ((select max(salary) from staff), (select min(salary) from staff));
					
-- check if there are any 
select * from staff where email is NULL;

-- specific domain
select * from staff where email like '%parallels.com';

---Find how many employees have same email domain, highest count on the top
select substring(email, POSITION('@' IN email)+1) "email_domains", count(*)
from staff
group by substring(email, POSITION('@' IN email)+1)
order by count(*) desc;

-- Conditional statement
select last_name, email, gender, salary from staff
where gender = 'Female' 
and department = 'Tools'
and salary >100000;

--Return the last name and start date for 2 conditional statement
select last_name,start_date, gender, salary from staff
where salary >165000
OR (department = 'Sports'
and gender = 'Male');

-----Male employee who work in the automation dept and earn more than $40000 and less than $100000 as well as females that work in the toys dept

select last_name,start_date, gender, salary from staff
where (salary between 40000 and 100000
and department = 'Automation'
	  and gender = 'Male')
or (gender = 'Female' and department = 'Toys')
order by Salary;

--Get the record for employees that earn over $130000 and are based in canada
select last_name,start_date, gender, TO_char((salary), '$999,999.99'), country
from staff join company_regions
on staff.region_id = company_regions.region_id 
where salary >130000
and country like 'Canada'
order by staff.region_id;

--Get the record for employees that earn over $130000 and are based in canada
select last_name,start_date, gender, TO_char((salary), '$999,999.99'), region_id
from staff where salary >130000
and  region_id IN (select region_id 
				   from company_regions
				   where Country like 'Canada')
order by region_id;

-- How much less does the employees in the USA earn compared to the highest earner in the company
select last_name, department, 
to_char((select max(salary) from staff), '$999,999.999') "highest salary",
salary "Employee Annual salary",
(select max(salary) from staff) - salary "difference in Salary"
from staff
where region_id IN (select region_id 
				   from company_regions
				   where Country like 'USA')
group by last_name, department, salary;				   
					


--Retrieve records for sum, ave for all department and provide employee count per department
select distinct(department), sum(salary) as total_salary, round(avg(Salary)) as average, count(*) as Total
from staff 
group by department
order by department;

--gender classifiction with respect to their departments and see to it that the sum of this classification is obtained 
select distinct(department), gender, count(*) "gender_per_department", sum(salary ) "sum of salaries by Category"
from staff 
group by department, gender
order by department;




