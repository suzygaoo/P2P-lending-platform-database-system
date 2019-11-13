# Data Modeling and Database Design for P2P Lending Platform(PostgreSQL)
!(data/lending.png)


## Scenario
Implementing a database management system for a P2P lending platform.

 

## The reason, motivation, and research
The peer-to-peer(P2P) lending marketplace works through a simple online platform, which connects borrowers and lenders. Since P2P are wide-area and large-scale systems that provide content sharing and storage services, having a well-designed database is crucial for them to manage the massive amount of information. To be specific, P2P systems do not lend their own funds but act as facilitators to both the borrower and the lender. With the rising popularity of peer-to-peer lending platforms, competition and products have increased as well. While these marketplaces operate on the same basic principle, they vary in terms of eligibility criteria, loan rates, amounts, and tenures as well as offerings. Some focus on personal loans, and a few target students and young professionals, while some cater exclusively to business needs.

 

## The initial plan of action
The initial plan of actions after proposing the scenario and finding data includes but are not limited to fully exploring the datasets, deciding and assigning tasks, normalizing the raw datasets, developing a relational schema, drawing the ER diagrams, designing the automatically loading data process, conducting analysis on the relational database, obtaining insights for the P2P lending platform, constructing interactive dashboards, and self-evaluation and improvement. The initial design for our database involves three main entities: the investors who lend money, the borrowers who request it, and load information which may include loan fulfillment and repayment data.

 

## Improve the company's decision-making process and other benefits
By designing a clear database for automatically storing and easily retrieving the data, our work will help the company keep track of basic transactions, provide information that will help the company run the business more efficiently, help managers and employees make better decisions, mitigate investment risks, and at last improve the whole decision-making process.

 

## Data source and a brief description
https://s3.amazonaws.com/udacity-hosted-downloads/ud651/prosperLoanData.csv  (Links to an external site.)
The raw data source contains a matrix of about 2.26 million observations and 145 variables, covering complete loan data (current loan status and latest payment information) for all loans which were issued from 2007 to 2015, as well as additional features such as credit score, address, number of finance inquiries, etc.


## Dashboard
Metabase Dashboard: http://s19db.apan5310.com:3201/public/dashboard/ee8a48c2-7d83-4229-ba12-619efa7586ce
Tableau Dashboard: https://public.tableau.com/profile/jia.yang#!/
