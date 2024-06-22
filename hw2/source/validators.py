import messages


def sign_up_validator(auth_user, cmd_tokens):
    # disregard already signed in users
    if auth_user:
        return False, messages.USER_ALREADY_SIGNED_IN
    # sign_up user_id first_name last_name
    elif len(cmd_tokens) == 4:
        return True, None
    else:
        return False, messages.CMD_NOT_ENOUGH_ARGS % 3


def sign_in_validator(auth_user, cmd_tokens):
    # disregard already signed in users
    if auth_user:
        if auth_user.user_id == cmd_tokens[1]:
            return None, messages.USER_ALREADY_SIGNED_IN
        else:
            return None, messages.USER_OTHER_SIGNED_IN

    # sign_in user_id
    elif len(cmd_tokens) == 2:
        return True, None
    else:
        return False, messages.CMD_NOT_ENOUGH_ARGS % 1


"""
    This validator is basic validator that returns (True, None) 
    when a user is authenticated and the number of command tokens is 1.
    Returns (False, <message>) otherwise.
"""


def basic_validator(auth_user, cmd_tokens):
    # only accept signed in users
    if auth_user:
        return True, None
    elif not auth_user and len(cmd_tokens) == 1:
        return False, messages.USER_NOT_AUTHORIZED
    else:
        return False, messages.CMD_INVALID_ARGS


def sign_out_validator(auth_user, cmd_tokens):
    return basic_validator(auth_user, cmd_tokens)


def quit_validator(cmd_tokens):
    if len(cmd_tokens) == 1:
        return True, None
    else:
        return False, messages.CMD_INVALID_ARGS


def show_memberships_validator(auth_user, cmd_tokens):
    return basic_validator(auth_user, cmd_tokens)


def show_subscription_validator(auth_user, cmd_tokens):
    return basic_validator(auth_user, cmd_tokens)


def review_validator(auth_user, cmd_tokens):
    if not auth_user:
        return False, messages.USER_NOT_AUTHORIZED
    elif len(cmd_tokens) == 4:
        return True, None
    else:
        return False, messages.CMD_NOT_ENOUGH_ARGS_AT_LEAST % 3


def subscribe_validator(auth_user, cmd_tokens):
    # only accept signed in users
    if not auth_user:
        return False, messages.USER_NOT_AUTHORIZED
    # subscribe <membership_id>
    elif len(cmd_tokens) == 2:
        return True, None
    else:
        return False, messages.CMD_NOT_ENOUGH_ARGS % 1


def search_for_businesses_validator(auth_user, cmd_tokens):
    # only accept signed in users
    if not auth_user:
        return False, messages.USER_NOT_AUTHORIZED
    # search_for_businesses <keyword_1> <keyword_2> <keyword_3> ... <keyword_n>
    elif len(cmd_tokens) > 1:
        return True, None
    else:
        return False, messages.CMD_NOT_ENOUGH_ARGS_AT_LEAST % 1


def suggest_businesses_validator(auth_user, cmd_tokens):
    return basic_validator(auth_user, cmd_tokens)

def get_coupon_validator(auth_user, cmd_tokens):
    return basic_validator(auth_user, cmd_tokens)
