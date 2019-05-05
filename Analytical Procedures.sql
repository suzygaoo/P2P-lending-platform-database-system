
##borrower
#1. Find the borrowers with the top 5 monthly income for each of the top 5 states (based on number of borrowers).

SELECT STATE, BORROWER, INCOME

FROM

(SELECT i.state AS STATE, i.borrower_id AS BORROWER,

i.stated_monthly_income AS INCOME, RANK()OVER(PARTITION BY i.state ORDER BY i.stated_monthly_income DESC) AS b_rank, row_number

FROM (SELECT i.stated_monthly_income, b.borrower_id, b.state

FROM income_info i JOIN borrower b on i.borrower_id = b.borrower_id) AS i JOIN

(SELECT state, ROW_NUMBER()over(ORDER BY a_rank ASC) AS row_number

FROM

(SELECT state, RANK()OVER(ORDER BY COUNT(borrower_id) DESC) AS a_rank

FROM borrower

GROUP BY state) AS FOO

WHERE a_rank <= 5) AS c ON i.state = c.state) AS FOO

WHERE b_rank <= 5

ORDER BY row_number ASC, INCOME DESC;

 

#2. Find the borrowers with the lowest 5 monthly income for each of the top 5 states (based on number of borrowers).

SELECT STATE, BORROWER, INCOME

FROM

(SELECT i.state AS STATE, i.borrower_id AS BORROWER,

i.stated_monthly_income AS INCOME, RANK()OVER(PARTITION BY i.state ORDER BY i.stated_monthly_income ASC) AS b_rank, row_number

FROM (SELECT i.stated_monthly_income, b.borrower_id, b.state

FROM income_info i JOIN borrower b on i.borrower_id = b.borrower_id) AS i JOIN

(SELECT state, ROW_NUMBER()over(ORDER BY a_rank ASC) AS row_number

FROM

(SELECT state, RANK()OVER(ORDER BY COUNT(borrower_id) DESC) AS a_rank

FROM borrower

GROUP BY state) AS FOO

WHERE a_rank <= 5) AS c ON i.state = c.state) AS FOO

WHERE b_rank <= 5

ORDER BY row_number ASC, INCOME ASC;

 

#3. Find the average loan amount of the top 5 occupations.

SELECT e.occupation, ROUND(avg(l.loan_amount),0) AS loan_amount

FROM employment e JOIN loan_ticket l on e.borrower_id = l.borrower_id

GROUP BY e.occupation

ORDER BY loan_amount DESC;

 

#4. Average score from each credit rating agency.

SELECT credit_rating_agency,

ROUND(AVG(credit_score),0) AS credit_score

FROM credit_info

JOIN credit_rating_agency USING(credit_rating_agency_id)

GROUP BY credit_rating_agency;

 

#5. Top 5 states and cities which have highest borrower average monthly income.

SELECT state, city, COUNT(borrower_id),ROUND(AVG(stated_monthly_income)) AS monthly_income

FROM borrower

JOIN income_info USING(borrower_id)

GROUP BY state,city

ORDER BY COUNT(borrower_id) DESC

LIMIT 5;

 

#6. Reason for loan which generate the most delinquent.

SELECT reason_for_loan, ROUND(AVG(amount_delinquent),0)

FROM loan_ticket

JOIN delinquency_history USING(borrower_id)

GROUP BY reason_for_loan

ORDER BY AVG(amount_delinquent) DESC;

 

#7. Gap between borrower's monthly payment and loan amount in each month.

SELECT EXTRACT (MONTH FROM liability_start_date ) AS month, SUM(monthly_repayment_amount) AS monthly_repayment, SUM(loan_amount) AS loan_amount

FROM borrow_liability

JOIN loan_ticket USING(loan_ticket_id)

GROUP BY month

ORDER BY month ASC;

 

#8. Transform credit rating scores into 8 levels(AA, A, B, C, D, E, F, N)

CREATE OR REPLACE FUNCTION score_to_letter (prosper_rating numeric (2,1))

RETURNS varchar(2) AS

$fun$

DECLARE rating_lvl varchar(2);

BEGIN

 IF $1 = '1' THEN rating_lvl = 'F';

 ELSEIF $1 = '2' THEN rating_lvl = 'E';

 ELSEIF $1 = '3' THEN rating_lvl = 'D';

 ELSEIF $1 = '4' THEN rating_lvl = 'C';

 ELSEIF $1 = '5' THEN rating_lvl = 'B';

 ELSEIF $1 = '6' THEN rating_lvl = 'A';

 ELSEIF $1 = '7' THEN rating_lvl = 'AA';

 ELSE rating_lvl ='N';

 END IF;

RETURN rating_lvl;

END;

$fun$

language plpgsql;

 

SELECT *, score_to_letter (prosper_rating) Rating_Lvl

FROM prosper_index;

 

#9. Transform the income information into High/Middle/Low groups and calculate the size of each group.

CREATE OR REPLACE FUNCTION income_to_lvl (stated_monthly_income numeric (10,2))

RETURNS varchar(2) AS

$fun$

DECLARE income_lvl varchar(2);

BEGIN

 IF $1 < 3000 THEN income_lvl = 'L';

 ELSEIF $1 < '6000' THEN income_lvl = 'M';

 ELSE income_lvl ='H';

 END IF;

RETURN income_lvl;

END;

$fun$

language plpgsql;

 

SELECT *, income_to_lvl (stated_monthly_income) Income_Lvl

FROM income_info;

 

SELECT COUNT( income_to_lvl (stated_monthly_income)) AS H

FROM (SELECT *,income_to_lvl (stated_monthly_income) Income_Lvl

 FROM income_info) AS a

WHERE Income_Lvl = 'H';

 

SELECT COUNT( income_to_lvl (stated_monthly_income)) AS M

FROM (SELECT *,income_to_lvl (stated_monthly_income) Income_Lvl

 FROM income_info) AS a

WHERE Income_Lvl = 'M';

 

SELECT COUNT( income_to_lvl (stated_monthly_income)) AS L

FROM (SELECT *,income_to_lvl (stated_monthly_income) Income_Lvl

FROM income_info) AS a

WHERE Income_Lvl = 'L';

 

SELECT COUNT( income_to_lvl (stated_monthly_income)) AS H

FROM (SELECT *,income_to_lvl (stated_monthly_income) Income_Lvl

FROM income_info) AS a

WHERE Income_Lvl = 'H';

 

#10. Top 5 states with the most investors.

SELECT b.state, COUNT(borrower_id)

FROM borrower b

GROUP BY b.state

ORDER BY COUNT(borrower_id) DESC

LIMIT 5;

 

##Investor

#1. Average investment limit for investors in different aging ranges.

CREATE OR REPLACE FUNCTION dob_to_age(date_of_birth date)

RETURNS varchar(20) AS

$fun$

DECLARE age varchar(20);

BEGIN

  IF $1 BETWEEN '1960-01-01' AND '1969-12-31' THEN age = '60s';

  ELSEIF $1 BETWEEN '1970-01-01' AND '1979-12-31' THEN age = '70s';

  ELSEIF $1 BETWEEN '1980-01-01' AND '1989-12-31' THEN age = '80s';

  ELSEIF $1 BETWEEN '1990-01-01' AND '1999-12-31' THEN age = '90s';

  END IF;

 RETURN age;

 END;

$fun$

language plpgsql;

 

SELECT *, dob_to_age(date_of_birth)

FROM investor;

 

SELECT dob_to_age(date_of_birth), ROUND(AVG(investment_limit), 2)

FROM investor

GROUP BY dob_to_age(date_of_birth)

ORDER BY ROUND(AVG(investment_limit), 2) DESC;

 

#2. Top 5 cities with the largest number of investors.

SELECT city, COUNT(investor_id)

FROM investor

GROUP BY city

ORDER BY COUNT(investor_id) DESC

 

#3. The number of investors of each payment bank.

SELECT payment_bank_name, COUNT(investor.investor_id)

FROM investor

JOIN payment_method ON investor.investor_id = payment_method.investor_id

JOIN payment_bank ON payment_method.payment_bank_id = payment_bank.payment_bank_id

GROUP BY payment_bank_name

ORDER BY COUNT(investor.investor_id) DESC;
                                                 
                                                 
###Create Views
                                                 
CREATE VIEW borrower_income_credit_rating_details AS
	SELECT b.borrower_id, b.first_name, b.last_name, b.zip_code, e.occupation, e.employment_status_duration,
			c.current_credit_lines, c.credit_score,r.credit_rating_agency
	FROM borrower b
	JOIN employment e ON b.borrower_id = e.borrower_id
	JOIN income_info i ON b.borrower_id = i.borrower_id
	JOIN credit_info c ON b.borrower_id = c.borrower_id
	JOIN credit_rating_agency r ON c.credit_rating_agency_id = r.credit_rating_agency_id;                                                 
