import time
from urllib.parse import urlparse

from functional import seq
from psaw import PushshiftAPI


def url_to_submission_id(url):
    """ Returns the unique submission id from the match thread url """
    parsed_url = urlparse(url)
    return parsed_url.path.split('/')[4]


def comment_to_dict(comment):
    return {
        "text": comment.get("body", ""),
        "score": comment.get("score", 0),
        "url": "https://www.reddit.com" + comment.get("permalink", ""),
    }


api = PushshiftAPI()


def get_comments_from_url_psaw(url: str) -> list:
    start = time.time()
    submission_id = url_to_submission_id(url)
    parsed_url = urlparse(url)
    subreddit = parsed_url.path.split('/')[2]
    comments = api.search_comments(subreddit=subreddit, link_id=submission_id)
    comments = seq(comments).map(comment_to_dict)
    end = time.time()

    print(f"Took {end - start} seconds")
    return comments
