/*
Enter your query here.
Please append a semicolon ";" at the end of the query and enter your query in a single line to avoid error.
*/
-- We have contest_id, hacker_id,college_id and challenge_id till here we have unique data
-- After this we have submistted stats and view_status for combination of contest_id, hacker_id,college_id and challenge_id

-- First fetch all details for submission_stats
With submission as (Select c.contest_id,
                           c.hacker_id,
                           c.name,
                           col.college_id,
                           ch.challenge_id,
                           sum(ss.total_submissions)          as total_submissions,
                           sum(ss.total_accepted_submissions) as total_accepted_submissions
                    from contests c
                             left join colleges col on c.contest_id = col.contest_id
                             left join challenges ch on col.college_id = ch.college_id
                             left join submission_stats ss on ch.challenge_id = ss.challenge_id
                    group by c.contest_id, c.hacker_id, c.name,
                             col.college_id,
                             ch.challenge_id),

-- Fetch details for view_stats
     view_status as (Select c.contest_id,
                            c.hacker_id,
                            c.name,
                            col.college_id,
                            ch.challenge_id,
                            sum(vs.total_views)        as total_views,
                            sum(vs.total_unique_views) as total_unique_views
                     from contests c
                              left join colleges col on c.contest_id = col.contest_id
                              left join challenges ch on col.college_id = ch.college_id
                              left join view_stats vs on ch.challenge_id = vs.challenge_id
                     group by c.contest_id, c.hacker_id, c.name,
                              col.college_id,
                              ch.challenge_id),


-- combine Both stats
     combine_both as (select sb.contest_id,
                             sb.hacker_id,
                             sb.name,
                             sb.college_id,
                             sb.challenge_id,
                          isnull (sb.total_submissions, 0) as total_submissions,
                          isnull (sb.total_accepted_submissions, 0) as total_accepted_submissions,
                          isnull (vs.total_views, 0) as total_views,
                          isnull (vs.total_unique_views, 0) as total_unique_views
                      from submission sb left join view_status vs
                      on sb.contest_id=vs.contest_id
                          and sb.hacker_id=vs.hacker_id
                          and sb.college_id=vs.college_id
                          and sb.challenge_id=vs.challenge_id),
-- Final data source preparation
     final as (select contest_id,
                      hacker_id,
                      name,
                      sum(total_submissions)          as total_submissions,
                      sum(total_accepted_submissions) as total_accepted_submissions,
                      sum(total_views)                as total_views,
                      sum(total_unique_views)         as total_unique_views
               from combine_both
               group by contest_id,
                        hacker_id,
                        name)
     -- Applied last condition if all four sum not equal to zero and order by contest_id
Select *
from final
where (total_submissions + total_accepted_submissions + total_views + total_unique_views) <> 0
order by contest_id;


