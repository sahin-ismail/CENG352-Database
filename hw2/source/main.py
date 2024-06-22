from trip import *
from validators import *

AUTH_USER = None
ANON_USER = "ANONYMOUS"


def print_success_msg(message):
    print(message)


def print_error_msg(message):
    print("ERROR: %s" % message)


def print_user_info(user):
    global ANON_USER

    if user:
        print(user, end=" > ")
    else:
        print(ANON_USER, end=" > ")


def main():
    global AUTH_USER

    conn = None
    help(conn, AUTH_USER)

    while True:
        # print user information if signed in
        print_user_info(user=AUTH_USER)

        # get new command from user
        cmd_text = input()
        cmd_tokens = tokenize_command(cmd_text)
        cmd = cmd_tokens[0]

        if cmd == "help":
            help(conn, AUTH_USER)

        elif cmd == "sign_up":
            # validate command
            validation_result, validation_message = sign_up_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                conn = connect_to_db()
                _, arg_user_id, arg_first_name, arg_last_name = cmd_tokens

                # sign up
                exec_status, exec_message = sign_up(conn=conn, user_id=arg_user_id, 
                                                    user_name=arg_first_name+" "+arg_last_name)

                conn.close()

                # print message
                if exec_status:
                    print_success_msg(exec_message)
                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "sign_in":
            # validate command
            validation_result, validation_message = sign_in_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                _, arg_user_id = cmd_tokens

                conn = connect_to_db()
                user, exec_message = sign_in(conn=conn, user_id=arg_user_id)

                if user:
                    AUTH_USER = user
                    print_success_msg(exec_message)

                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "sign_out":

            # validate command
            validation_result, validation_message = sign_out_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                exec_status, exec_message = sign_out(conn, user=AUTH_USER)

                if exec_status:
                    AUTH_USER = None
                    conn.close()
                    print_success_msg(exec_message)

                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "quit":

            # validate command
            validation_result, validation_message = quit_validator(cmd_tokens)

            if validation_result:

                exec_status, exec_message = quit(conn, user=AUTH_USER)

                if exec_status:
                    break
                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "show_memberships":
            # validate command
            validation_result, validation_message = show_memberships_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                exec_status, exec_message = show_memberships(conn,AUTH_USER)

                if not exec_status:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "show_subscription":
            # validate command
            validation_result, validation_message = show_subscription_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                exec_status, exec_message = show_subscription(conn, user=AUTH_USER)

                if not exec_status:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "review":

            # validate command

            validation_result, validation_message = review_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                _, arg_review_id, arg_business_id, arg_stars = cmd_tokens
                exec_status, exec_message = review(conn, user=AUTH_USER, review_id=arg_review_id, business_id = arg_business_id, stars=arg_stars)

                if exec_status:
                    print_success_msg(exec_message)
                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "subscribe":
            # validate command
            validation_result, validation_message = subscribe_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                _, arg_membership_id = cmd_tokens

                user, exec_message = subscribe(conn=conn, user=AUTH_USER, membership_id=arg_membership_id)

                if user:
                    AUTH_USER = user
                    print_success_msg(exec_message)

                else:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "search_for_businesses":
            # validate command
            validation_result, validation_message = search_for_businesses_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                arg_search_text = " ".join(cmd_tokens[1:])

                exec_status, exec_message = search_for_businesses(conn=conn, user=AUTH_USER, search_text=arg_search_text)

                if not exec_status:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "suggest_businesses":
            # validate command
            validation_result, validation_message = suggest_businesses_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                exec_status, exec_message = suggest_businesses(conn=conn, user=AUTH_USER)

                if not exec_status:
                    print_error_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "get_coupon":
            # validate command
            validation_result, validation_message = get_coupon_validator(AUTH_USER, cmd_tokens)

            if validation_result:
                exec_status, exec_message = get_coupon(conn=conn, user=AUTH_USER)

                if not exec_status:
                    print_error_msg(exec_message)
                else:
                	print_success_msg(exec_message)

            else:
                print_error_msg(validation_message)

        elif cmd == "":
            pass

        else:
            print_error_msg(messages.CMD_UNDEFINED)


if __name__ == '__main__':
    main()
