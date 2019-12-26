Through aggregating all user profiles and loan status, analyze if each user is likely to produce collection.

**User Profile**: home_ownership, addr_state, emp_length, avg_annual_inc

**Load**: term, title, avg_loan_amount, avg_DTI, avg_installment

**output:**
	1. average loan amount
	2. average interest rate
	3. average annual increase
	4. average DTI
	5. # of collections reveived
	6. average installment
    
    
**job pipeline**
      1. read and standardize load data: compute missing columns for the DS fields(DTI, has_collection)
      2. read and standardize rejection loan data: fill nulls for missing columns
      3. union two datasets and group by the same user/loan profile, to generate the analyzed results for each user/loan profile
      4. write results as JSON
 

