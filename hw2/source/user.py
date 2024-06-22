class User:
    def __init__(self, user_id=0, user_name="", review_count=0, yelping_since="", useful=0, funny=0, cool=0, fans=0, average_stars=0.0, session_count="",):
        self.user_id = user_id
        self.user_name = user_name
        self.review_count = review_count
        self.yelping_since = yelping_since
        self.useful = useful
        self.funny = funny
        self.cool = cool
        self.fans = fans
        self.average_stars = average_stars
        self.session_count = session_count

    def __str__(self):
        return '%s (%s)' % (self.user_name, self.user_id)
