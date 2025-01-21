SELECT e.*
FROM employees e
JOIN leaves l ON e.empid = l.empid
WHERE (
    ((l.leave_start_date like '%2025-01%') or (l.leave_end_date like '%2025-01%'))
)
AND (
    ((l.leave_start_date like '%2025-08%') or (l.leave_end_date like '%2025-08%'))
);


SELECT e.* from employees e 
join leaves l on e.empid =l.empid 
where ((l.leave_start_date like '%2025-01%') or (l.leave_end_date like '%2025-01%')) GROUP BY e.empid ;

SELECT e.* from employees e 
join leaves l on e.empid =l.empid 
where ((l.leave_start_date like '%2025-08%') or (l.leave_end_date like '%2025-08%')) GROUP BY e.empid ;


SELECT e.*
FROM employees e
JOIN leaves l1 ON e.empid = l1.empid
JOIN leaves l2 ON e.empid = l2.empid
WHERE 
    ((l1.leave_start_date LIKE '%2025-01%' OR l1.leave_end_date LIKE '%2025-01%') 
     AND 
     (l2.leave_start_date LIKE '%2025-08%' OR l2.leave_end_date LIKE '%2025-08%'))
GROUP BY e.empid;

SELECT * FROM leaves l where ((l.leave_start_date like '%2025-01%') or (l.leave_end_date like '%2025-01%')) ; 

SELECT e.* 
FROM employees e 
JOIN leaves l 
  ON e.empid = l.empid 
WHERE 
  ((l.leave_start_date BETWEEN '2025-01-01' AND '2025-01-31') 
   AND 
  (l.leave_end_date BETWEEN '2025-08-01' AND '2025-08-31'));
) ;