import html
import time

from functional import seq
from unidecode import unidecode

import MonkeyLearnProductSentiment
import comments
import markdown_to_plaintext
import search
from comment import Comment, CommentList


def clean_comment(comment):
    comment["text"] = unidecode(markdown_to_plaintext.unmark(html.unescape(comment["text"])))
    return comment


def get_recommendations(query):
    if not query:
        return {"error_message": "No query", "success": False, "recommendations": []}

    # search google for "<query name> reddit"
    start = time.time()
    reddit_urls = search.return_links(query)
    print(*reddit_urls, sep="\n")
    print("Searching Google took {} seconds".format(time.time() - start))

    # resolve reddit URLs to comments and remove HTML/markdown syntax
    # comments are dictionaries of string text, number score, and string url.
    # reddit = comments.connect()

    # all_comments = dump_comments.load_comments("dump_movies.dumps")

    # chunked_comments = CommentList(
    #     seq(all_comments)
    #         .map(Comment.from_dict)
    #         .to_list()
    # ).chunk()

    start = time.time()
    the_comments = (seq(reddit_urls)
                    .flat_map(comments.get_comments_from_url)
                    .map(clean_comment)
                    .map(Comment.from_dict)
                    .to_list())

    # the_comments = (seq(comments.get_comments_from_urls(reddit_urls))
    #                 .map(clean_comment)
    #                 .map(Comment.from_dict)
    #                 .to_list())

    print(f"Got {len(the_comments)} comments")
    chunked_comments = CommentList(the_comments).chunk()
    end = time.time()
    print("Total time to parse reddit: " + str(end - start))

    start = time.time()
    results = MonkeyLearnProductSentiment.movie_extractor_chunked(chunked_comments)
    recommendations = seq(results).smap(lambda text, score: {"keyword": text, "score": score}).to_list()
    end = time.time()
    print("Total time to run inference and format results: " + str(end - start))

    return {"error_message": "", "success": True, "recommendations": recommendations}
