## ETL process for Prosper lending data


### Extraction

In this stage, the data is collected, often from multiple and different types of sources. In order to obtain border aspects of information on the Peer-to-peer platform, 
three sets of data were used in this project: Lending Club Load Data, Borrower data, and 10-Year Treasury Constant Maturity Rate. T
he CSV file for the prosper loan data was downloaded from the Prosper website(prosper.com). 
The borrower data was randomly generated from Mockaroo website (https://mockaroo.com/), which includes
mostly geographic information of the borrowers. The 10-year US treasury data was gathered from the Interworks website. (https://wdc.portals.interworks.com/fred_20/)

We have total of 19 tables in nomalized 3NF, and we need to transform all the data to the database from its original form. The first
step is to extract data from each data source, which is the process of reading data from a database. 

#### For the borrower part
First, we load the dataset and packages

```r
require (RPostgreSQL)
require (dplyr)
require (tidyr)
require(readxl)
```

After that, connect R to PostgreSQL and Load data
```r
drv = dbDriver('PostgreSQL')
con = dbConnect(drv,dbname = 'Prosper-Loan',
                host = 's19db.apan5310.com', port = 50201 ,
                user = 'postgres' , password = '4rabf037')

df <- read_xlsx("~/Desktop/SPRING 19/SQL/project/borrower_new.xlsx")
```
#### For the investor part

we noticed that data in the investor sections are simple text data, which is not suitable for our schema and may cause trouble loading in. 
Thus, we need to convert them into proper data types according: date_of_birth, transaction_date, transaction_amount, and closing_date.

```r
investor <- read_excel("~/Downloads/SQL_Project_Group1.xlsx", 
                       sheet = "Investor", col_types = c("numeric", 
                                                         "text", "text", "text", "date", "text", 
                                                         "text", "text", "text", "text", "text", 
                                                         "text", "text", "text", "text", "text", 
                                                         "text", "text", "text", "text", "date", 
                                                         "text", "text", "numeric", "date", 
                                                         "numeric"))
```

### Transformation

Our transformation process includes removing extraneous or erroneous data (cleaning), applying business rules, checking data integrity, and creating aggregates as necessary. 
Below are the steps for data transformation: 

1. Splitting: Splitting a single column into multiple columns
for columns that have multiple entries in the same row, first we need to separate them in R using function 'seperate_row':  
credit_rating_agency, establish_year, credit_score since they have multiple entries in the same row, 
phone_numbers, payment_account_types, payment_account_holder_names, payment_account_numbers,payment_bank_names

Borrower
```r
df <- separate_rows(df, credit_rating_agency,establish_year,credit_score,
                    sep = '\\|')
#separate phone column
df <- df%>%
  separate_rows(phone_numbers)
 
 ```
 
 Investor
 ```r
 #separate phone column
investor <- investor%>%
  separate_rows(phone_numbers)

#separate rows
investor <- investor %>%
  separate_rows(payment_account_types, payment_account_holder_names, payment_account_numbers,
                payment_bank_names, sep= '\\|')
investor <- filter(investor, payment_account_types != '')
investor<- filter(investor, phone_numbers !='')
df<-investor
```

2. Deduplication: Identifying and removing duplicate records, create unique identifiers
for columns that need to separate from the original dataset and create new tables in the database in order to achieve 3NF, 
we need to assign a unique identifier for each of them: employment_id, credit_rating_agency_id, credit_info_id, 
prosper_index_id, borrow_liability_id, delinquency_history_id, income_info_id, nominee_id, phone_id, 
payment_bank_id, payment_method_id, investor_id, account_statement_id


Borrower

```r
##b_phone table
df1 <- df %>% select('phone_number'= phone_numbers) %>% distinct()
df2 <- bind_cols('phone_id' = sprintf('p%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('phone_numbers'='phone_number'))

#assign eamployment_id and write employment info
df1 <- df %>% select(borrower_id,employment_status_duration,occupation,
                     employment_status) %>% distinct()
df2 <- bind_cols('employment_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'borrower_id')

#credit rating agency
df1 <- df %>% select(credit_rating_agency,establish_year) %>% distinct()
df2 <- bind_cols('credit_rating_agency_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'credit_rating_agency')

#credit info
df1 <- df %>% select(borrower_id,current_credit_lines,credit_score,credit_rating_agency_id) %>% distinct()
df2 <- bind_cols('credit_info_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'borrower_id')


#prosper index
df1 <- df %>% select(borrower_id,prosper_rating,prosper_score,
                     credit_info_id) %>% distinct()
df2 <- bind_cols('prosper_index_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'borrower_id')


#borrow liability
df1 <- df %>% select(loan_ticket_id,monthly_repayment_amount,
                     liability_start_date,liability_end_date) %>% distinct()
df2 <- bind_cols('borrow_liability_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'loan_ticket_id')

#delingquency history
df1 <- df %>% select(borrower_id,current_delinquency,amount_delinquent) %>% distinct()
df2 <- bind_cols('delinquency_history_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'borrower_id')


#income info
df1 <- df %>% select(borrower_id,income_range_bottom,income_range_top,
                     income_verifiable,stated_monthly_income) %>% distinct()
df2 <- bind_cols('income_info_id' = sprintf('%09d', 1:nrow(df1)), df1)
df <- df %>% inner_join(y = df2, by = 'borrower_id')
```

investor

```r
#assign account_statement_id 
investor$X__1 <- NULL
investor <- bind_cols('account_statement_id' = sprintf('a%09d',1:nrow(investor)), investor)

##nominee table
df1 <- df %>% select(nominee_first_name, nominee_last_name, nominee_date_of_birth, nominee_relationship_with_investor) %>% distinct()
df2 <- bind_cols('nominee_id' = sprintf('n%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('nominee_first_name','nominee_last_name'))


##phone table
df1 <- df %>% select('phone_number'= phone_numbers) %>% distinct()
df2 <- bind_cols('phone_id' = sprintf('p%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('phone_numbers'='phone_number'))

##payment_bank_name
df1 <- df %>% select('payment_bank_name'= payment_bank_names) %>% distinct()
df2 <- bind_cols('payment_bank_id' = sprintf('B%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('payment_bank_names'='payment_bank_name'))

##investor
df1 <- df %>% select(firstname, lastname, ssn,nominee_id,zip_code,address,date_of_birth,escrow_account_number,
                     investment_limit,fund_committed, city) %>% distinct()
df2 <- bind_cols('investor_id' = sprintf('i%09d', 1:nrow(df1)),df1)
df2$investment_limit <-as.numeric(df2$investment_limit)
df2$fund_committed <- as.numeric(df2$fund_committed)
df <- df %>% inner_join(y = df2, by = c('firstname','lastname'))

#payment_method
df1 <- df %>% select('payment_account_type'= payment_account_types,'payment_account_number'=payment_account_numbers,
                     'payment_account_holder_name'= payment_account_holder_names, payment_bank_id,investor_id) %>% distinct()
df2 <- bind_cols('payment_method_id' = sprintf('M%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('payment_account_numbers'='payment_account_number'))

##account_statement
df1 <- df %>% select(transaction_type,transaction_amount,transaction_date,closing_balance,'investor_id'=investor_id.x) %>% distinct()
df2 <- bind_cols('account_statement_id' = sprintf('a%09d', 1:nrow(df1)),df1)
df <- df %>% inner_join(y = df2, by = c('investor_id.x'='investor_id'))
```



3. Missing Values: 
For some of the columns that contain NA values, we need to find the best way to deal with those missing values. 
Our dataset contains few missing values, also, according to our scenario, 
there is no need to fill all the missing values because the real-world data is not perfect most of the time, 
and it is reasonable that some fields are empty in the dataset. For example, ssn, phone_number, zip_code, etc., 
these features are meaningless for the following process of conducting analysis and getting insights.



### Load
Loading data to the target multidimensional structure is the final step in ETL process. 
In this step, extracted and transformed data is written into the dimensional structures actually accessed by the end users and application systems, 
where they can be integrated, rearranged, and consolidated, creating a new type of unified information base for reports and reviews. 

1. create tables in PostgreSQL through R
2. write data into database 


Below is the code for loading data into our database:

```r
dbWriteTable(con,name = 'b_phones',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'borrower', value = df1, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'loan_ticket', value = df1, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'employment', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'credit_rating_agency', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'borrow_liability', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'credit_info', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'prosper_index', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'borrow_liability', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'delinquency_history', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'income_info', value = df2, row.names = FALSE, append = TRUE)
dbWriteTable(con,name = 'borrower_phone',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'nominee',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'phones',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'payment_bank',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'investor',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'account_statement',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con,name = 'investor_phone',value = df2, row.names = FALSE,append=TRUE)
dbWriteTable(con, name = 'investor_proposal', value = df2, row.names = FALSE, append = TRUE)
```


















