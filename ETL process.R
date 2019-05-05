require (RPostgreSQL)
require (dplyr)
require (tidyr)
require(readxl)

#connect R to SQL database
drv = dbDriver('PostgreSQL')
con = dbConnect(drv,dbname = 'Prosper-Loan',
                host = 's19db.apan5310.com', port = 50201 ,
                user = 'postgres' , password = '4rabf037')


df <- read_xlsx("~/Desktop/SPRING 19/SQL/project/borrower_new.xlsx")


df <- separate_rows(df, credit_rating_agency,establish_year,credit_score,
                    sep = '\\|')
#separate phone column
df <- df%>%
  separate_rows(phone_numbers)


#CREAT TABLES
stmt <- 'CREATE TABLE borrower (
borrower_id varchar(10),
first_name varchar(50),
last_name varchar(50),
state varchar(50),
city varchar(50),
street_address varchar(50),
zip_code varchar(10),
PRIMARY KEY (borrower_id)
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE loan_ticket (
loan_ticket_id varchar(10),
borrower_id varchar(10),
loan_amount numeric(10,2),
loan_tenure_in_month numeric,
reason_for_loan text,
borrow_rate decimal(8,6),
borrower_apr decimal(8,6),
loan_status varchar(50),
PRIMARY KEY (loan_ticket_id),
FOREIGN KEY (borrower_id) REFERENCES borrower
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE employment (
employment_id integer,
borrower_id varchar(10),
employment_status_duration integer,
occupation varchar(50),
employment_status varchar(50),
PRIMARY KEY (employment_id),
FOREIGN KEY (borrower_id) REFERENCES borrower
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE credit_rating_agency (
credit_rating_agency_id integer,
credit_rating_agency varchar(50),
establish_year integer,
PRIMARY KEY (credit_rating_agency_id)
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE credit_info (
credit_info_id varchar(10),
borrower_id varchar(10),
current_credit_lines integer,
credit_score integer,
credit_rating_agency_id integer,
PRIMARY KEY (credit_info_id),
FOREIGN KEY (borrower_id) REFERENCES borrower,
FOREIGN KEY (credit_rating_agency_id) REFERENCES credit_rating_agency
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE prosper_index (
prosper_index_id varchar(10),
borrower_id varchar(10),
prosper_rating numeric(2,1),
prosper_score numeric(3,1),
credit_info_id varchar(10),
PRIMARY KEY (prosper_index_id),
FOREIGN KEY (borrower_id) REFERENCES borrower,
FOREIGN KEY (credit_info_id) REFERENCES credit_info
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE borrow_liability (
borrow_liability_id varchar(10),
loan_ticket_id varchar(10),
monthly_repayment_amount numeric(10,2),
liability_start_date date,
liability_end_date date,
PRIMARY KEY (borrow_liability_id),
FOREIGN KEY (loan_ticket_id) REFERENCES loan_ticket
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE delinquency_history (
delinquency_history_id integer,
borrower_id varchar(10),
current_delinquency integer,
amount_delinquent numeric(10,2),
PRIMARY KEY (delinquency_history_id),
FOREIGN KEY (borrower_id) REFERENCES borrower
);'

dbGetQuery(con, stmt)



stmt <- 'CREATE TABLE income_info (
income_info_id integer,
borrower_id varchar(10),
income_range_bottom numeric(10,2),
income_range_top numeric(10,2),
income_verifiable boolean,
stated_monthly_income numeric(10,2),
PRIMARY KEY (income_info_id),
FOREIGN KEY (borrower_id) REFERENCES borrower
);'

dbGetQuery(con, stmt)

stmt<- 'CREATE TABLE borrower_phone(
phone_id char(10),
borrower_id char(10),
phone_type varchar(10),
PRIMARY KEY (phone_id, borrower_id),
FOREIGN KEY (borrower_id) REFERENCES borrower(borrower_id),
FOREIGN KEY (phone_id) REFERENCES b_phones(phone_id)
);'
dbGetQuery(con, stmt)


#####import borrower data#####

 
##borrower_phone
df1 <- df %>% select(phone_id, borrower_id) %>% distinct()
df2 <- bind_cols(df1, 'phone_type' = NULL)
dbWriteTable(con,name = 'borrower_phone',value = df2, row.names = FALSE,append=TRUE)


#borrower
df1 <- df %>% select(borrower_id,first_name,last_name,state,city,
                     street_address,zip_code) %>% distinct()
dbWriteTable(con,name = 'borrower', value = df1, row.names = FALSE, append = TRUE)


#loan_ticket
df1 <- df %>% select(loan_ticket_id,borrower_id,loan_amount,loan_tenure_in_month,reason_for_loan,
                     borrow_rate,borrower_apr,loan_status) %>% distinct()
dbWriteTable(con,name = 'loan_ticket', value = df1, row.names = FALSE, append = TRUE)


#assign eamployment_id and write employment info
df1 <- df %>% select(borrower_id,employment_status_duration,occupation,
                     employment_status) %>% distinct()
df2 <- bind_cols('employment_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'employment', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'borrower_id')


#credit rating agency
df1 <- df %>% select(credit_rating_agency,establish_year) %>% distinct()
df2 <- bind_cols('credit_rating_agency_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'credit_rating_agency', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'credit_rating_agency')




#credit info
df1 <- df %>% select(borrower_id,current_credit_lines,credit_score,credit_rating_agency_id) %>% distinct()
df2 <- bind_cols('credit_info_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'credit_info', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'borrower_id')




#prosper index
df1 <- df %>% select(borrower_id,prosper_rating,prosper_score,
                     credit_info_id) %>% distinct()
df2 <- bind_cols('prosper_index_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'prosper_index', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'borrower_id')



#borrow liability
df1 <- df %>% select(loan_ticket_id,monthly_repayment_amount,
                     liability_start_date,liability_end_date) %>% distinct()
df2 <- bind_cols('borrow_liability_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'borrow_liability', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'loan_ticket_id')



#delingquency history
df1 <- df %>% select(borrower_id,current_delinquency,amount_delinquent) %>% distinct()
df2 <- bind_cols('delinquency_history_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'delinquency_history', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'borrower_id')



#income info
df1 <- df %>% select(borrower_id,income_range_bottom,income_range_top,
                     income_verifiable,stated_monthly_income) %>% distinct()
df2 <- bind_cols('income_info_id' = sprintf('%09d', 1:nrow(df1)), df1)
dbWriteTable(con,name = 'income_info', value = df2, row.names = FALSE, append = TRUE)
df <- df %>% inner_join(y = df2, by = 'borrower_id')



#CREAT TABLE
stmt <- 'CREATE TABLE phones (
phone_id char(10),
phone_number varchar(20),
PRIMARY KEY(phone_id)
);'
dbGetQuery(con, stmt)


stmt<- 'CREATE TABLE nominee(
nominee_id varchar(50),
nominee_first_name varchar(50),
nominee_last_name varchar(50),
nominee_relationship_with_investor varchar(30),
nominee_date_of_birth date,
PRIMARY KEY (nominee_id)
);'
dbGetQuery(con, stmt)


stmt<- 'CREATE TABLE payment_bank(
payment_bank_id char(10),
payment_bank_name varchar(50),
PRIMARY KEY (payment_bank_id)
);'
dbGetQuery(con, stmt)


stmt<-'CREATE TABLE investor (
investor_id char(10),
nominee_id char(10),
firstname varchar(50),
lastname varchar(50),
ssn varchar(11),
zip_code varchar(5),
address varchar(100),
city varchar(100),
payment_method_id char(10),
date_of_birth date,
escrow_account_number varchar(20),
investment_limit numeric(8,0),
fund_committed numeric(8,0),
PRIMARY KEY (investor_id),
FOREIGN KEY(nominee_id) REFERENCES nominee(nominee_id)
);'
dbGetQuery(con, stmt)

stmt<-'CREATE TABLE payment_method(
payment_method_id char(10),
investor_id char(10),
payment_account_type varchar(20),
payment_account_number varchar(30),
payment_account_holder_name varchar(50),
payment_bank_id char(10),
PRIMARY KEY(payment_method_id),
FOREIGN KEY(payment_bank_id) REFERENCES payment_bank(payment_bank_id),
FOREIGN KEY(investor_id) REFERENCES investor(investor_id)
);'
dbGetQuery(con, stmt)


stmt<-'CREATE TABLE account_statement(
account_statement_id char(10),
investor_id char(10),
transaction_type varchar(30),
transaction_amount numeric(7,2),
transaction_date date,
closing_balance numeric(9,2),
PRIMARY KEY (account_statement_id),
FOREIGN KEY (investor_id) REFERENCES investor(investor_id)
);'
dbGetQuery(con, stmt)


stmt<- 'CREATE TABLE investor_phone(
phone_id char(10),
investor_id char(10),
phone_type varchar(10),
PRIMARY KEY (phone_id, investor_id),
FOREIGN KEY (investor_id) REFERENCES investor(investor_id),
FOREIGN KEY (phone_id) REFERENCES phones(phone_id)
);'
dbGetQuery(con, stmt)

stmt<- 'CREATE TABLE borrower_phone(
phone_id char(10),
borrower_id char(10),
phone_type varchar(10),
PRIMARY KEY (phone_id, borrower_id),
FOREIGN KEY (borrower_id) REFERENCES borrower(borrower_id),
FOREIGN KEY (phone_id) REFERENCES b_phones(phone_id)
);'
dbGetQuery(con, stmt)


#import investor data
library(readxl)
investor <- read_excel("~/Downloads/SQL_Project_Group1.xlsx", 
                       sheet = "Investor", col_types = c("numeric", 
                                                         "text", "text", "text", "date", "text", 
                                                         "text", "text", "text", "text", "text", 
                                                         "text", "text", "text", "text", "text", 
                                                         "text", "text", "text", "text", "date", 
                                                         "text", "text", "numeric", "date", 
                                                         "numeric"))

#####investor#####
#assign account_statement_id 
investor$X__1 <- NULL
investor <- bind_cols('account_statement_id' = sprintf('a%09d',1:nrow(investor)), investor)

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

#load data
##nominee table
df1 <- df %>% select(nominee_first_name, nominee_last_name, nominee_date_of_birth, nominee_relationship_with_investor) %>% distinct()
df2 <- bind_cols('nominee_id' = sprintf('n%09d', 1:nrow(df1)),df1)
dbWriteTable(con,name = 'nominee',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('nominee_first_name','nominee_last_name'))


##phone table
df1 <- df %>% select('phone_number'= phone_numbers) %>% distinct()
df2 <- bind_cols('phone_id' = sprintf('p%09d', 1:nrow(df1)),df1)
dbWriteTable(con,name = 'phones',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('phone_numbers'='phone_number'))

##payment_bank_name
df1 <- df %>% select('payment_bank_name'= payment_bank_names) %>% distinct()
df2 <- bind_cols('payment_bank_id' = sprintf('B%09d', 1:nrow(df1)),df1)
dbWriteTable(con,name = 'payment_bank',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('payment_bank_names'='payment_bank_name'))

##investor
df1 <- df %>% select(firstname, lastname, ssn,nominee_id,zip_code,address,date_of_birth,escrow_account_number,
                     investment_limit,fund_committed, city) %>% distinct()
df2 <- bind_cols('investor_id' = sprintf('i%09d', 1:nrow(df1)),df1)
df2$investment_limit <-as.numeric(df2$investment_limit)
df2$fund_committed <- as.numeric(df2$fund_committed)
dbWriteTable(con,name = 'investor',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('firstname','lastname'))

#payment_method
df1 <- df %>% select('payment_account_type'= payment_account_types,'payment_account_number'=payment_account_numbers,
                     'payment_account_holder_name'= payment_account_holder_names, payment_bank_id,investor_id) %>% distinct()
df2 <- bind_cols('payment_method_id' = sprintf('M%09d', 1:nrow(df1)),df1)
dbWriteTable(con,name = 'payment_method',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('payment_account_numbers'='payment_account_number'))

##account_statement
df1 <- df %>% select(transaction_type,transaction_amount,transaction_date,closing_balance,'investor_id'=investor_id.x) %>% distinct()
df2 <- bind_cols('account_statement_id' = sprintf('a%09d', 1:nrow(df1)),df1)
dbWriteTable(con,name = 'account_statement',value = df2, row.names = FALSE,append=TRUE)
df <- df %>% inner_join(y = df2, by = c('investor_id.x'='investor_id'))

##investor_phone
df1 <- df %>% select(phone_id, 'investor_id'=investor_id.x) %>% distinct()
df2 <- bind_cols(df1, 'phone_type' = NULL)
dbWriteTable(con,name = 'investor_phone',value = df2, row.names = FALSE,append=TRUE)


#import Investor_Proposal

SQL_Project_Investor_proposal <- read_excel("SQL_Project_Group1.xlsx"
,sheet='Investor_proposal'
)
df <- SQL_Project_Investor_proposal
df1 <- df %>% select(loan_ticket_id, investor_id) %>% distinct()
df2 <- bind_cols('investor_proposal_id' = sprintf('ip%09d', 1:nrow(df1)), df1)
dbWriteTable(con, name = 'investor_proposal', value = df2, row.names = FALSE, append = TRUE)

