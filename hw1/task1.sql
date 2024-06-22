CREATE TABLE Business (
    business_id varchar(255),
    business_name varchar(255),
    address varchar(255),
	state varchar(255),
	is_open boolean,
    stars float,
    PRIMARY KEY (business_id)
);


COPY Business (business_id, business_name, address, state, is_open, stars)
FROM 'D:\dersler son\352\nihan hoca\projects\p1\yelp_academic_dataset_business.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE Users (
    user_id varchar(255),
    user_name varchar(255),
    review_count INT,
	yelping_since varchar(255),
	useful INT,
	funny INT,
	cool INT,
	fans INT,
    average_stars float,
    PRIMARY KEY (user_id)
);

COPY Users (user_id, user_name, review_count, yelping_since, useful, funny,cool, fans, average_stars)
FROM 'D:\dersler son\352\nihan hoca\projects\p1\yelp_academic_dataset_user.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE Friend (
    user_id1 varchar(255),
	user_id2 varchar(255),
	foreign key (user_id1) references users(user_id),
	foreign key (user_id2) references users(user_id)
);

COPY Friend (user_id1, user_id2)
FROM 'D:\dersler son\352\nihan hoca\projects\p1\yelp_academic_dataset_friend.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE Review (
    review_id varchar(255),
    user_id varchar(255),
	business_id varchar(255),
    stars float,
	date varchar(255),
	useful INT,
	funny INT,
	cool INT,
    PRIMARY KEY (review_id),
	foreign key (user_id) references Users(user_id),
	foreign key (business_id) references Business(business_id)
);

COPY Review (review_id, user_id, business_id, stars, date, useful, funny, cool)
FROM 'D:\dersler son\352\nihan hoca\projects\p1\yelp_academic_dataset_reviewNoText.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE Tip (
    tip_id SERIAL,
	business_id varchar(255),
    user_id varchar(255),
	date varchar(255),
	compliment_count INT,
	tip_text varchar(500),
	foreign key (user_id) references Users(user_id),
	foreign key (business_id) references Business(business_id)
);


COPY Tip (tip_text,date,compliment_count,business_id, user_id)
FROM 'D:\dersler son\352\nihan hoca\projects\p1\yelp_academic_dataset_tip.csv'
DELIMITER ','
CSV HEADER;













