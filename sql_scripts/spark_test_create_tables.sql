
CREATE TABLE message
(
  id smallint
, senderId smallint
, receiverId smallint
, createdAt datetime
, etl_insert_dt datetime NOT NULL

)
;

CREATE TABLE users
(
  id smallint
, city VARCHAR(100)
, country VARCHAR(100)
, emailDomain VARCHAR(100)
, age smallint
, isSmoking VARCHAR(10)
, gender VARCHAR(100)
, income decimal(8, 2)
, createdAt datetime
, updatedAt datetime
, etl_insert_dt datetime NOT NULL

)
;

CREATE TABLE subscription
(

  id smallint
, status VARCHAR(100)
, amount decimal(8, 2)
, createdAt datetime
, startDate datetime
, endDate datetime
, etl_insert_dt datetime NOT NULL
)
;


CREATE TABLE user_analysis
(
  id INT
, account_createdAt DATETIME
, age INT
, city VARCHAR(100)
, country VARCHAR(100)
, emailDomain VARCHAR(100)
, gender VARCHAR(100)
, isSmoking VARCHAR(10)
, income DECIMAL(8,2)
, subscrptn_createdAt DATETIME
, subscrptn_startDate DATETIME
, subscrptn_endDate DATETIME
, status VARCHAR(100)
, amount DECIMAL(8,2)
, receiverId INT
, msg_createdAt DATETIME
, etl_insert_dt datetime NOT NULL
)
;