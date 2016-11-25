insert into filtered_sentiments_v2
select * from sentiments
where sentiments.reviewerId in 
(select top 100 reviewerId from (select reviewerId, count(*) as NUM_OF_REVIEWS from sentiments group by reviewerId order by NUM_OF_REVIEWS desc) 
where NUM_OF_REVIEWS >= 200 and NUM_OF_REVIEWS <= 10000);
