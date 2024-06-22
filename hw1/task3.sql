
/* Trigger Question 1 */

CREATE TRIGGER reviewCount BEFORE INSERT ON review
FOR EACH ROW EXECUTE PROCEDURE countInc();

CREATE FUNCTION countInc() RETURNS trigger AS $emp_stamp$
    BEGIN
	UPDATE users
		SET review_count = review_count + 1
		where new.user_id = user_id;
        RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;

/* Trigger Question 2 */

CREATE TRIGGER zeroStar
BEFORE INSERT ON review
FOR EACH ROW EXECUTE PROCEDURE zero();

CREATE FUNCTION zero()
  RETURNS trigger AS
$func$
BEGIN
   IF (new.stars = 0) THEN
   		delete from review where user_id = new.user_id;
   		delete from tip where user_id = new.user_id;
   		RETURN NULL;
   END IF;
   RETURN NEW;
END
$func$  LANGUAGE plpgsql;


/* View Question 1 */

CREATE VIEW BusinessCount AS
select b.business_id, b.business_name, res.count
from business b, (select r2.business_id, count(*)
from review r2 group by r2.business_id) res
where b.business_id = res.business_id;

