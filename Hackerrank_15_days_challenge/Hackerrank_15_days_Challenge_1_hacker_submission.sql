/*
 Write a query to print total number of unique hackers who made at least 1 submission each day
 (starting on the first day of the contest), and find the hacker_id and name of the hacker who made
 maximum number of submissions each day. If more than one such hacker has a maximum
 number of submissions, print the lowest hacker_id. The query should print this
 information for each day of the contest, sorted by the date.

 Hackerrank challenge link- https://www.hackerrank.com/challenges/15-days-of-learning-sql/problem
 */

-- Unique Hacker made at least 1 submission
-- Very important thing is need to fetch hackers who are common in respective days
-- Logic
   -- 1st subquery daily_submission_count will make a distinct list of submission-date and hacker_id combination
   -- Then 1st subquery used to map submission_count against hackers id which will help to deduce if respective hacker was present on each day
   -- 2nd subquery hacker_count will count such hackers who are present every day of submission
   -- 3rd subquery unique_hacker_count used as column to map final hacker_count against submission_date
with unique_hacker_every_day as (select distinct submission_date,
                                                 (select count(hacker_id)
                                                  from (select hacker_id, count(*) as submission_count
                                                        from (select distinct submission_date, hacker_id
                                                              from submissions
                                                              where submission_date between '2016-03-01' and sb.submission_date) daily_submission_count
                                                        group by hacker_id) hacker_count
                                                  where submission_count = (datediff(day, '2016-03-01', sb.submission_date) + 1)) as unique_hacker_count
                                 from submissions sb),

-- Map Rank based upon number of submission each day
     rank_submission as (SELECT submission_date,
                                hacker_id,
                                submission_count,
                                dense_rank() over (partition by submission_date order by submission_count desc ) as rank
                         from (select submission_date, hacker_id, count(*) as submission_count
                               from submissions
                               GROUP BY submission_date, hacker_id) test),

-- Max submission Ranker
     max_submission as (select rs.submission_date, rs.hacker_id, h.name, rs.submission_count, rank
                        from rank_submission rs
                                 left join hackers h on rs.hacker_id = h.hacker_id
                        where rank = 1),

-- Prepare final output by combining unique hacker and max rank
     final as (select uh.submission_date,
                      uh.unique_hacker_count,
                      ms.hacker_id,
                      ms.name,
                      -- Rank based upon hacker_id ascending order
                      row_number() over (partition by uh.submission_date order by ms.hacker_id ) as lower_id_rank
               from unique_hacker_every_day uh
                        left join max_submission ms on ms.submission_date = uh.submission_date)

-- Consider  lower hacker_id in case tie in number of max submission
-- sort by submission_date
select submission_date,
       unique_hacker_count,
       hacker_id,
       name
from final
where lower_id_rank = 1
order by submission_date;

