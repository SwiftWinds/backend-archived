import json
import logging
import os
import time
from urllib.parse import urlparse

import praw
import requests
from dotenv import load_dotenv
from functional import seq
from pmaw import PushshiftAPI
from praw.models import MoreComments

api = PushshiftAPI()

load_dotenv(".env")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
USER_AGENT = os.getenv("USER_AGENT")

username = 'auto-reddit-rec'
userAgent = "RecommedditScraper/0.1 by " + username
clientId = 'MSo42pflF3S9Ug'
clientSecret = "c6Vmpn6qCFcvEzjw3C-OU2MsOkInWg"
r = praw.Reddit(user_agent=userAgent, client_id=clientId, client_secret=clientSecret)

ID_LENGTH = 6


def enable_praw_log():
    """
    Enables PRAW HTTP logging
    See https://praw.readthedocs.io/en/latest/getting_started/logging.html
    :return:
    """
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    for logger_name in ("praw", "prawcore"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)


def connect() -> praw.Reddit:
    """
    Code flow: connect to reddit api without an account
    :return: bool
    """
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        user_agent=USER_AGENT,
    )
    # print(reddit.auth.url(["identity"], "...", "permanent"))
    print("User:", reddit.user.me())
    # print("Access to:", reddit.auth.scopes())

    reddit.read_only = True
    return reddit


def comment_to_dict(comment):
    return {
        "text": comment.get("body", ""),
        "score": comment.get("score", 0),
        "url": "https://www.reddit.com" + comment.get("permalink", ""),
    }


def comment_obj_to_dict(comment):
    if isinstance(comment, MoreComments):
        print(f"ERROR: received MoreComments with {comment.count} comments in comment_obj_to_dict")
        return None
    return {
        "text": comment.body,
        "score": comment.score,
        "url": "https://www.reddit.com" + comment.permalink,
    }


# def comments_to_dict(comments):
#     if isinstance(comments, MoreComments) and comments.count == 0:
#         return []
#     comment_list = comments.comments() if isinstance(comments, MoreComments) else [comments]
#     comment_list = seq(comment_list).map(comment_obj_to_dict).filter_not(lambda x: x is None)
#     return comment_list


# def get_comments(reddit, url: str) -> list:
#     """
#     Get all comments from a particular URL.
#     Currently added by BFS order.
#     :param url:
#     """
#     submission = reddit.submission(url=url)
#     submission.comments.replace_more(
#         limit=None
#     )  # removes limit=x amount of MoreComments
#
#     comments = seq(submission.comments.list()).map(comment_to_dict)
#
#     return comments


def get_comments_from_url(url: str) -> list:
    """
    Get all comments from a particular URL.
    Currently added by BFS order.
    :param url:
    """
    start = time.time()
    submission_id = url_to_submission_id(url)
    subreddit = url_to_subreddit(url)
    pushshift_url = f"https://api.pushshift.io/reddit/comment/search/?subreddit={subreddit}&link_id={submission_id}&limit=1000"
    request_start = time.time()
    submission = requests.get(pushshift_url)
    submission_results = []
    try:
        submission_results = json.loads(submission.content)['data']
    except json.decoder.JSONDecodeError:
        print(f"ERROR: JSONDecodeError for {pushshift_url}")
        print(f"response: {submission.content}")
    num_results = len(submission_results)
    while num_results == 100:
        pushshift_url = f"https://api.pushshift.io/reddit/comment/search/?subreddit={subreddit}&link_id={submission_id}&limit=1000&until={submission_results[-1]['created_utc']}"
        request_end = time.time()
        time.sleep(max(0., 0.7 - (request_end - request_start)))
        request_start = time.time()
        submission = requests.get(pushshift_url)
        additional_results = []
        try:
            additional_results = json.loads(submission.content)['data']
        except json.decoder.JSONDecodeError:
            print(f"ERROR: JSONDecodeError for {pushshift_url}")
            print(f"response: {submission.content}")
        num_results = len(additional_results)
        submission_results += additional_results
    request_end = time.time()
    time.sleep(max(0., 0.7 - (request_end - request_start)))

    comments = seq(submission_results).map(comment_to_dict)
    end = time.time()

    print(f"Took {end - start} seconds")
    return comments


def get_comments_from_url_pmaw(url: str) -> list:
    start = time.time()
    submission_id = url_to_submission_id(url)
    parsed_url = urlparse(url)
    subreddit = parsed_url.path.split('/')[2]
    comments = api.search_comments(subreddit=subreddit, link_id=submission_id)
    comments = seq(comments).map(comment_to_dict)
    end = time.time()

    print(f"Took {end - start} seconds")
    return comments


def get_all(r, submission_id):
    submission = r.submission(id=submission_id)
    comments_list = []
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        comments_list.append(comment)
    return comments_list


def get_comments_from_url_praw(url: str) -> list:
    start = time.time()
    submission_id = url_to_submission_id(url)
    comments = seq(get_all(r, submission_id)).map(comment_obj_to_dict)
    end = time.time()
    print(f"Took {end - start} seconds")
    return comments


def url_to_submission_id(url):
    """ Returns the unique submission id from the match thread url """
    parsed_url = urlparse(url)
    return parsed_url.path.split('/')[4]


def url_to_subreddit(url):
    """ Returns the subreddit name from the url """
    parsed_url = urlparse(url)
    return parsed_url.path.split('/')[2]


def get_comments_from_urls(urls: list[str]) -> list:
    start = time.time()
    post_ids = seq(urls).map(url_to_submission_id)
    comment_ids = api.search_submission_comment_ids(ids=post_ids)
    comments = api.search_comments(ids=comment_ids)
    comment_list = [comment_to_dict(comment) for comment in comments]
    end = time.time()
    print(f"Took {end - start} seconds")
    return comment_list


def post_to_dict(post):
    return {
        "text": post.selftext,
        "score": post.score,
        "url": post.url,
    }


def get_post(reddit, url: str):
    """
    Get post content and votes
    :param reddit:
    :param url:
    :return: list: post, upvotes, url
    """
    submission = reddit.submission(url=url)
    return post_to_dict(submission)

# enable_praw_log()
# reddit = connect()
# test_url = "https://www.reddit.com/r/cpp/comments/1rml6l/good_books_on_distributedparallel_systems/"
# post = get_post(reddit, test_url)
# print(post)
