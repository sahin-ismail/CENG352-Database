from user import User

import psycopg2

from config import read_config
from messages import *

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


def help(conn, user):
    # TODO: Create behaviour of the application for different type of users: Non Authorized (not signed id), Free and Premium users. 
    print("\n*** Please enter one of the following commands ***")
    print("> help")
    print("> sign_up <user_id> <first_name> <last_name>")
    print("> sign_in <user_id>")
    print("> sign_out")
    print("> show_memberships")
    print("> show_subscription")
    print("> subscribe <membership_id>")
    print("> review <review_id> <business_id> <stars>")
    print("> search_for_businesses <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>")
    print("> suggest_businesses")
    print("> get_coupon")
    print("> quit")



"""
    Saves user with given details.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_up(conn, user_id, user_name):
    # TODO: Implement this function
    return False, CMD_EXECUTION_FAILED


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
    return None, USER_SIGNIN_FAILED


"""
    Signs out from given user's account.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Decrement session_count of the user in the database.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def sign_out(conn, user):
    # TODO: Implement this function
    return False, CMD_EXECUTION_FAILED


"""
    Quits from program.
    - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
    - Remember to sign authenticated user out first.
    - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
    - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
"""


def quit(conn, user):
    # TODO: Implement this function
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
    return False, CMD_EXECUTION_FAILED


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
    return False, CMD_EXECUTION_FAILED

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
    return False, CMD_EXECUTION_FAILED


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
    return None, CMD_EXECUTION_FAILED

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
    return False, CMD_EXECUTION_FAILED


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
    return False, CMD_EXECUTION_FAILED


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
    return False, CMD_EXECUTION_FAILED
