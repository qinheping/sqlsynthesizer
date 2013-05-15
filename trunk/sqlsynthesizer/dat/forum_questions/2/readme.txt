http://forums.tutorialized.com/sql-basics-113/sql-query-379547.html

lets say,I have a table which holds invoices.
Fields are Project Code,invoice no,invoice amount.
I want to find total invoice amount for each project. 
any suggestions how? 

Select Project_code, invoice_no, sum(invoice_amount)
from your_table
group by 1,2

project_code	invoice_no	invoice_amount			project_code	invoice_no	invoice_amount
1	2	100			1	2	100
1	2	200			2	3	300
2	3	300			2	4	400
2	4	400					

