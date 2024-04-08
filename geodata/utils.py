from fake_useragent import UserAgent

def random_user_agent() -> str:
    return UserAgent().random
