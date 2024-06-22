from user import User

import psycopg2

from config import read_config
from messages import *

from datetime import datetime

POSTGRESQL_CONFIG_FILE_NAME = "database.cfg"

"""
    Connects to PostgreSQL database and returns connection object.
"""


def connect_to_db():
    db_conn_params = read_config(filename=POSTGRESQL_CONFIG_FILE_NAME, section="postgresql")
    conn = psycopg2.connect(**db_conn_params)
    conn.autocommit = False
    return conn


"""
    Splits given command string by spaces and trims each token.
    Returns token list.
"""


def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]


"""
    Prints list of available commands of the software.
"""


"""
*********************************************************************
Membership tablom aşağıdaki gibidir. Testlerimi ona göre yaptım.
1 | Free | 1 | 0
2 | Premium | 2 | 10
*********************************************************************
"""



def help(user):
    # TODO: Create behaviour of the application for different type of users: Non Authorized (not signed id), Free and Premium users. 

    
    sql2 = "select * \
            from users u, subscription s \
            where u.user_id = s.user_id and s.membership_id = '2' and u.user_id = %s;"
    try:
        if user != None:
            conn = connect_to_db()
            cursor = conn.cursor()

            cursor.execute(sql2, (user.user_id,))
            data1 = cursor.fetchone()
            cursor.close()

        if user == None:
            print("\n*** Please enter one of the following commands ***")
            print("> help")
            print("> sign_up <user_id> <first_name> <last_name>")
            print("> sign_in <user_id>")
            print("> quit")
        elif data1 == None:
            print("\n*** Please enter one of the following commands ***")
            print("> help")
            print("> sign_out")
            print("> show_memberships")
            print("> show_subscription")
            print("> subscribe <membership_id>")
            print("> review <review_id> <business_id> <stars>")
            print("> search_for_businesses <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>")
            print("> quit")
        else:
            print("\n*** Please enter one of the following commands ***")
            print("> help")
            print("> sign_out")
            print("> show_memberships")
            print("> show_subscription")
            print("> subscribe <membership_id>")
            print("> review <review_id> <business_id> <stars>")
            print("> search_for_businesses <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>")
            print("> suggest_businesses")
            print("> get_coupon")
            print("> quit")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False, CMD_EXECUTION_FAILED



"""
    Saves user with given details.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_up(conn, user_id, user_name):
    # TODO: Implement this function
    sql1 = "SELECT * FROM users WHERE  user_id = %s"
    sql2 = "INSERT INTO users(user_id,user_name,review_count,yelping_since,useful,funny,cool,fans,average_stars,session_count) VALUES (%s,%s,0,%s,0,0,0,0,0,0);"
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    try:
        if conn == None:
            conn = connect_to_db()

        
        cursor = conn.cursor()

        cursor.execute(sql1, (user_id,))
        data = cursor.fetchone()
        

        if data != None:
            cursor.close()
            return False, CMD_EXECUTION_FAILED

        cursor.execute(sql2, (user_id,user_name,dt_string))
        conn.commit()
        cursor.close()
        return True, CMD_EXECUTION_SUCCESS

    except (Exception, psycopg2.DatabaseError) as error:
        return False, CMD_EXECUTION_FAILED

    finally:
        if conn is not None:
            cursor.close()



"""
    Retrieves user information if there is a user with given user_id and user's session_count < max_parallel_sessions.
    - Return type is a tuple, 1st element is a user object and 2nd element is the response message from messages.py.
    - If there is no such user, return tuple (None, USER_SIGNIN_FAILED).
    - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (user, CMD_EXECUTION_SUCCESS).
    - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
"""


def sign_in(conn, user_id):
    # TODO: Implement this function
    sql1 = "SELECT * FROM users WHERE  user_id = %s"
    sql2 = "select membership_id from subscription where user_id = %s;"
    sql3 = "select max_parallel_sessions from membership where membership_id = %s;"
    sql4 = "select session_count from users where user_id = %s;"
    sql5 = "update users set session_count = session_count + 1 where user_id = %s;"


    try:
        if conn == None:
            conn = connect_to_db()

        cursor = conn.cursor()
        cursor.execute(sql1, (user_id,))
        data = cursor.fetchone()
        
        if data == None:
            cursor.close()
            return None, USER_SIGNIN_FAILED
        
        
        cursor.execute(sql2, (user_id,))
        membershipsId = cursor.fetchone()

        if membershipsId == None:
            membershipsId = 1

        cursor.execute(sql3, (membershipsId,))
        max_parallel = cursor.fetchone()
        cursor.execute(sql4, (user_id,))
        session_count = cursor.fetchone()
        if session_count<max_parallel:
            cursor.execute(sql5, (user_id,))
            conn.commit()
            user = User(data[0], data[1], int(data[2]), data[3], int(data[4]), int(data[5]),int(data[6]),int(data[7]), float(data[8]), int(data[9])+1)
            cursor.close()
            return user , CMD_EXECUTION_SUCCESS
        else:
            cursor.close()
            return None, USER_ALL_SESSIONS_ARE_USED

    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        conn.rollback()
        return None, USER_SIGNIN_FAILED

    finally:
        if cursor is not None:
            cursor.close()




"""
    Signs out from given user's account.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Decrement session_count of the user in the database.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_out(conn, user):
    # TODO: Implement this function
    
    sql1 = "SELECT * FROM users WHERE  user_id = %s"
    sql2 = "update users set session_count = session_count - 1 where user_id = %s"

    try:
        if conn == None:
            conn = connect_to_db()
        cursor = conn.cursor()

        cursor.execute(sql1, (user.user_id,))
        data = cursor.fetchone()

        if data == None:
            cursor.close()
            return False, CMD_EXECUTION_FAILED

        if int(data[9]) > 0 :
            cursor.execute(sql2, (user.user_id,))
            conn.commit()
            user.session_count = user.session_count - 1
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        else:
            cursor.close()
            return False, CMD_EXECUTION_FAILED

    except (Exception, psycopg2.DatabaseError) as error:
        cursor.close()
        conn.rollback()
        return None, CMD_EXECUTION_FAILED

    finally:
        if cursor is not None:
            cursor.close()


"""
    Quits from program.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Remember to sign authenticated user out first.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def quit(conn, user):
    # TODO: Implement this function
    try:
        sign_out(conn, user)
        conn.close()
        del user
        return True, CMD_EXECUTION_SUCCESS
    except (Exception, psycopg2.DatabaseError) as error:
        return False, CMD_EXECUTION_FAILED
    


"""
    Retrieves all available memberships and prints them.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print available memberships and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Max Sessions|Monthly Fee
    1|Silver|2|30
    2|Gold|4|50
    3|Platinum|10|90
"""


def show_memberships(conn,user):
    # TODO: Implement this function

    sql = "select * from  membership;"

    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql)
        rows = curser.fetchall()



        if rows == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED
        
        print("#|Name|Max Sessions|Monthly Fee")
        for row in rows:
            print(row[0],'|',row[1],"|",row[2],"|",row[3])

        curser.close()
        return True, CMD_EXECUTION_SUCCESS
    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()


"""
    Retrieves authenticated user's membership and prints it. 
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful; print the authenticated user's membership and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    #|Name|Max Sessions|Monthly Fee
    2|Gold|4|50
"""


def show_subscription(conn, user):
    # TODO: Implement this function
    sql1 = "SELECT m.membership_id, m.membership_name, m.max_parallel_sessions, m.monthly_fee FROM users u, membership m, subscription s WHERE u.user_id = %s AND u.user_id = s.user_id and s.membership_id = m.membership_id;"
    sql2 = "SELECT * FROM users WHERE  user_id = %s"
    sql3 = "select * from  membership where membership_id=1;"
    sql4 = "select * from  subscription where user_id = %s;"
    
    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql2, (user.user_id,))
        data = curser.fetchone()
        if data == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED

        curser.execute(sql4, (user.user_id,))
        row = curser.fetchone()

        if row == None:
            curser.execute(sql3)
            row = curser.fetchone()
            print("#|Name|Max Sessions|Monthly Fee")
            print(row[0],'|',row[1],"|",row[2],"|",row[3])  
            curser.close()
            return True, CMD_EXECUTION_SUCCESS
        else:
            curser.execute(sql1,(user.user_id,))
            row = curser.fetchone()
            print("#|Name|Max Sessions|Monthly Fee")
            print(row[0],'|',row[1],"|",row[2],"|",row[3])
            curser.close()
            return True, CMD_EXECUTION_SUCCESS
    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()

"""
    Insert user-review-business relationship to Review table if not exists in Review table.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If a user-review-business relationship already exists (checking review_id is enough), do nothing on the database and return (True, CMD_EXECUTION_SUCCESS).
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If the business_id is incorrect; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def review(conn, user, review_id, business_id, stars):
    # TODO: Implement this function

    sql1 = "select * from review where review_id = %s;"
    sql2 = "select * from business where business_id = %s;"
    sql3 = "INSERT INTO review(review_id, user_id,business_id,stars,date,useful,funny,cool) VALUES (%s,%s,%s,%s,%s,0,0,0);"
    
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql1, (review_id,))
        data = curser.fetchone()
        if data != None:
            curser.close()
            return True, CMD_EXECUTION_SUCCESS
        
        curser.execute(sql2, (business_id,))
        data = curser.fetchone()
        if data == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED
        curser.execute(sql3, (review_id,user.user_id,business_id,stars,dt_string))
        conn.commit()
        return True, CMD_EXECUTION_SUCCESS
    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        conn.rollback()
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()


"""
    Subscribe authenticated user to new membership.
    - Return type is a tuple, 1st element is a user object and 2nd element is the response message from messages.py.
    - If target membership does not exist on the database, return tuple (None, SUBSCRIBE_MEMBERSHIP_NOT_FOUND).
    - If the new membership's max_parallel_sessions < current membership's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
    - If the operation is successful, commit changes and return tuple (user, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
"""


def subscribe(conn, user, membership_id):
    # TODO: Implement this function
    sql1 = "select * from membership where membership_id = %s;"
    sql2 = "select membership_id from subscription where user_id = %s;"
    sql3 = "SELECT * FROM users WHERE  user_id = %s;"
    sql4 = "select max_parallel_sessions from membership where membership_id = %s;"
    sql5 = "select * from subscription where user_id = %s;"
    sql6 = "INSERT INTO subscription(user_id,membership_id,time_spent) VALUES (%s,%s,0);"
    sql7 = "update subscription set membership_id = %s where user_id = %s;"

    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql1, (membership_id,))
        data = curser.fetchone()
        if data == None:
            curser.close()
            return None, SUBSCRIBE_MEMBERSHIP_NOT_FOUND
        
        curser.execute(sql2, (user.user_id,))
        mem_id = curser.fetchone()
        if mem_id == None:
            mem_id = 1

        curser.execute(sql4, (membership_id,))
        new = curser.fetchone()
        curser.execute(sql4, (mem_id,))
        old = curser.fetchone()

        if new<old:
            return None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE
        else:
            curser.execute(sql5, (user.user_id,))
            data = curser.fetchone()
            if data == None:
                curser.execute(sql6, (user.user_id,membership_id))
                conn.commit()
                return user, CMD_EXECUTION_SUCCESS
            else:
                curser.execute(sql7, (membership_id,user.user_id))
                conn.commit()
                return user, CMD_EXECUTION_SUCCESS

    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        conn.rollback()
        return None, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()

"""
    Searches for businesses with given search_text.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Print all businesses whose names contain given search_text IN CASE-INSENSITIVE MANNER.
    - If the operation is successful; print businesses found and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

    Output should be like:
    Id|Name|State|Is_open|Stars
    1|A4 Coffee Ankara|ANK|1|4
    2|Tetra N Caffeine Coffee Ankara|ANK|1|4
    3|Grano Coffee Ankara|ANK|1|5
"""


def search_for_businesses(conn, user, search_text):
    # TODO: Implement this function
    sql = "select business_id,business_name,state,is_open,stars from business where lower(business_name) like lower('%"+search_text+"%') order by business_id;"
    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql)
        rows = curser.fetchall()

        if rows == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED
        
        print("Id|Name|State|Is_open|Stars")
        for row in rows:
            print(row[0],'|',row[1],"|",row[2],"|",row[3],"|",row[4])

        curser.close()
        return True, CMD_EXECUTION_SUCCESS
        

    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()


"""
    Suggests combination of these businesses:

        1- Gather the reviews of that user.  From these reviews, find the top state by the reviewed business count.  
        Then, from all open businesses find the businesses that is located in the found state.  
        You should collect top 5 businesses by stars.

        2- Perform the same thing on the Tip table instead of Review table.

        3- Again check the review table to find the businesses get top stars from that user.  
        Among them get the latest reviewed one.  Now you need to find open top 3 businesses that is located in the same state 
        and has the most stars (if there is an equality order by name and get top 3).


    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.    
    - Output format and return format are same with search_for_businesses.
    - Order these businesses by their business_id, in ascending order at the end.
    - If the operation is successful; print businesses suggested and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
"""


def suggest_businesses(conn, user):
    # TODO: Implement this function
    sql="select * from users u, subscription s \
        where u.user_id = s.user_id and u.user_id = %s and s.membership_id = '2';"
    sql1="select distinct * from \
        ((select business_id,business_name,state,is_open,stars \
        from business where is_open ='true' and state = \
        (select state \
        from \
        (select state, count( distinct b.business_id) as c \
        from review r, business b \
        where b.business_id = r.business_id and  r.user_id = %s group by state order by c desc limit 1) s) order by stars desc limit 5 \
        ) \
        union \
        \
        (select business_id,business_name,state,is_open,stars \
        from business where is_open ='true' and state = \
        (select state \
        from \
        (select state, count( distinct b.business_id) as c \
        from tip r, business b \
        where b.business_id = r.business_id and  r.user_id = %s group by state order by c desc limit 1) s) order by stars desc limit 5 \
        ) \
        UNION \
        \
        (select business_id,business_name,state,is_open,stars \
        from business \
        where is_open = 'true' and state = \
        (select state \
        from business \
        where business_id = \
        (select business_id \
        from review \
        where stars = \
        (select stars \
        from review \
        where user_id = %s group by stars order by stars desc limit 1) \
        order by date desc limit 1)) \
        order by stars desc limit 3)) res \
        order by business_id;"

    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql,(user.user_id,))
        data = curser.fetchone()

        if data == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED

        curser.execute(sql1,(user.user_id,user.user_id,user.user_id))
        rows = curser.fetchall()

        if rows == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED
        
        print("Id|Name|State|Is_open|Stars")
        for row in rows:
            print(row[0],'|',row[1],"|",row[2],"|",row[3],"|",row[4])

        curser.close()
        return True, CMD_EXECUTION_SUCCESS
        

    except (Exception, psycopg2.DatabaseError) as error:
        curser.close()
        print(error)
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()
    


"""
    Create coupons for given user. Coupons should be created by following these steps:

        1- Calculate the score by using the following formula:
            Score = timespent + 10 * reviewcount

        2- Calculate discount percentage using the following formula (threshold given in messages.py):
            actual_discount_perc = score/threshold * 100

        3- If found percentage in step 2 is lower than 25% print the following:
            You don’t have enough score for coupons.

        4- Else if found percentage in step 2 is between 25-50% print the following:
            Creating X% discount coupon.

        5- Else create 50% coupon and remove extra time from user's time_spent:
            Creating 50% discount coupon.

    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.    
    - If the operation is successful (step 4 or 5); return tuple (True, CMD_EXECUTION_SUCCESS).
    - If the operation is not successful (step 3); return tuple (False, CMD_EXECUTION_FAILED).
    - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).


"""

def get_coupon(conn, user):
    # threshold is defined in messages.py, you can directly use it.
    sql="select * from users u, subscription s \
        where u.user_id = s.user_id and u.user_id = %s and s.membership_id = '2';"
    sql1 = "select s.time_spent \
        from users u, subscription s \
        where u.user_id = s.user_id and u.user_id = %s;"
    sql2 = "select count(*) \
            from users u, review r \
            where u.user_id = r.user_id and u.user_id = %s;"
    
    try:
        if conn == None:
            conn = connect_to_db()
        curser = conn.cursor()

        curser.execute(sql,(user.user_id,))
        data = curser.fetchone()

        if data == None:
            curser.close()
            return False, CMD_EXECUTION_FAILED

        curser.execute(sql1,(user.user_id,))
        time = curser.fetchone()

        curser.execute(sql2,(user.user_id,))
        rew = curser.fetchone()

        score = 10*rew[0]+time[0]
        actual_discount_perc = score/threshold * 100
        if actual_discount_perc < 25:
            curser.close()
            return False, CMD_EXECUTION_FAILED
        elif actual_discount_perc < 50:
            curser.close()
            a = "Creating "+actual_discount_perc+"% discount coupon."
            print(a)
            return True, CMD_EXECUTION_SUCCESS
        else:
            curser.close()
            print("Creating 50% discount coupon.")
            return True, CMD_EXECUTION_SUCCESS
        
    except (Exception, psycopg2.DatabaseError) as error:
        return False, CMD_EXECUTION_FAILED
    finally:
        if curser is not None:
            curser.close()
