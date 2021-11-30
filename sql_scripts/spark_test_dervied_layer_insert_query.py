
user_analysis_insert_qry = '''insert into user_analysis
(id
,account_createdAt
,age
,city
,country
,emailDomain
,gender
,isSmoking
,income
,subscrptn_createdAt
,subscrptn_startDate
,subscrptn_endDate
,status
,amount
,receiverId
,msg_createdAt
,etl_insert_dt) 
select 
u.id
,u.createdAt account_createdAt 
,u.age 
,u.city 
,u.country 
,u.emailDomain
,u.gender 
,u.isSmoking 
,u.income 
,s.createdAt subscrptn_createdAt 
,s.startDate subscrptn_startDate 
,s.endDate subscrptn_endDate 
,s.status 
,s.amount
,m.receiverId
,m.createdAt as msg_createdAt,
CURRENT_TIMESTAMP
from users u 
left join subscription s 
on s.id = u.id
left join message m
on m.senderId = u.id 
'''

