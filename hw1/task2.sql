/* Question 1 */

select distinct u.user_id, u.user_name, (u.review_count-u.fans) as diff
from users u, review r, business b
where u.user_id = r.user_id and r.business_id = b.business_id
and u.review_count > u.fans and b.stars>3.5 order by diff desc, u.user_id desc;

/* Question 2 */

select u.user_name, b.business_name, te.date, te.compliment_count
from users u, tip te, business b
where u.user_id = te.user_id and te.business_id = b.business_id
and b.state = 'TX' and te.compliment_count>2 and b.is_open=true order by te.compliment_count desc, te.date desc;

/* Question 3 */

select s.user_name, s.count
from
	(select u.user_name, COUNT(DISTINCT f.user_id1)
	from users u, friend f
	where u.user_id = f.user_id1 group by u.user_name order by count desc limit 20) s
order by s.count desc, s.user_name desc;

/* Question 4 */

select u.user_name, u.average_stars, u.yelping_since
from 
	(select distinct u.user_id, u.user_name
	from users u, review r, business b
	where u.user_id = r.user_id and r.business_id = b.business_id
	and r.stars < b.stars) res, users u
where res.user_id = u.user_id order by u.average_stars desc, u.yelping_since desc;

/* Question 5 */

select b.business_name, b.state, b.stars
from
	(select max(res.count)
	from
		(select b.business_id, count(res.business_id)
		from
			(select b.business_id
			from business b, tip te
			where b.business_id = te.business_id
			and b.is_open = true and te.date like '%2020%' and te.tip_text like '%good%') res, business b
		where res.business_id = b.business_id group by b.business_id ) res, business b
	where res.business_id = b.business_id) res1,
	(select b.business_id, count(res.business_id)
		from
			(select b.business_id
			from business b, tip te
			where b.business_id = te.business_id
			and b.is_open = true and te.date like '%2020%' and te.tip_text like '%good%') res, business b
		where res.business_id = b.business_id group by b.business_id ) res2, business b
where res2.business_id = b.business_id and res2.count = res1.max order by b.stars desc, b.business_name desc;


/* Question 6 */

select u.user_name, u.yelping_since, u.average_stars
from	
	(select u1.user_id as id1, min(u2.average_stars) as av2
	from users u1, users u2, friend f
	where u1.user_id = f.user_id1 and u2.user_id = f.user_id2 group by id1) res,
	users u
where u.user_id = res.id1 and u.average_stars <res.av2 order by u.average_stars desc, u.yelping_since desc;

/* Question 7 */

select state, AVG(stars) as average from business group by state order by average desc limit 10;

/* Question 8 */

select reslast1.y, restlast2.avg
from
	(select res.y from
		(select c1.y, c1.count, c2.count, ((c1.count+0.0)*100/(c2.count+0.0)) as per from
			(select ds.y, count(tt.compliment_count)   from
			(select date,substring(date,0,5) y from tip) ds,
			tip tt
			where ds.date = tt.date and tt.compliment_count>0 group by ds.y) c1,
			(select ds.y, count(tt.compliment_count)   from
			(select date,substring(date,0,5) y from tip) ds,
			tip tt
			where ds.date = tt.date group by ds.y) c2
		where c1.y=c2.y) res
	where res.per>1) reslast1,
	(select avg(ts.compliment_count), rese.yy
	from
	(select date, substring(date,0,5) yy from tip) rese,
	tip ts
	where ts.date = rese.date
	group by rese.yy) restlast2
where reslast1.y = restlast2.yy order by reslast1.y desc;

/* Question 9 */

select u3.user_name
from
	((select distinct u.user_id
	from review r, users u, business b
	where r.user_id = u.user_id and r.business_id = b.business_id and b.stars>3.5 )
	EXCEPT
	(select distinct u1.user_id
	from review r1, users u1, business b1
	where r1.user_id = u1.user_id and r1.business_id = b1.business_id and b1.stars<=3.5)) res,
	users u3
where u3.user_id = res.user_id	order by u3.user_name asc;

/* Question 10 */

select bu.business_name, res3.y, res3.av
from
	(select bbb.business_id, substring(rr.date,0,5) as y , avg(rr.stars) as av
	from
		(select bb.business_id
		from
			(select b.business_id,count(b.business_id) as s
			from business b, review r
			where b.business_id = r.business_id
			group by b.business_id) res,
			business bb
		where bb.business_id = res.business_id and res.s>1000) res1,
		review rr, business bbb
	where bbb.business_id = res1.business_id and bbb.business_id = rr.business_id group by bbb.business_id, y) res3,
	business bu
where bu.business_id = res3.business_id and res3.av>3 order by res3.y asc;

/* Question 11 */

select uu.user_name, res.s, res.cc, (res.s-res.cc) dif
from
	(select u.user_id, sum(r.useful) s, sum(r.cool) cc
	from 
	users u, review r
	where u.user_id = r.user_id group by u.user_id) res,
	users uu
where uu.user_id = res.user_id and res.s>res.cc order by dif desc, uu.user_name desc;

/* Question 12 */

select distinct r.user_id as user1, r1.user_id as user2, r.business_id, r.stars
from friend f ,review r, review r1
where f.user_id1 = r.user_id and f.user_id2 = r1.user_id and r.business_id = r1.business_id
and r.stars = r1.stars and f.user_id1<f.user_id2
order by r.business_id desc, r.stars desc;

/* Question 13 */

select stars, state, count(business_id)
from business
where is_open = true
group by grouping sets ((stars,state),(stars),(state),());

/* Question 14 */

select * 
from
(select u.user_id,u.review_count,u.fans, rank() over (
		PARTITION BY u.fans
		ORDER BY u.review_count desc
)
	from users u
	where u.fans>=50 and u.fans<=60) res
	where res.rank<4;
