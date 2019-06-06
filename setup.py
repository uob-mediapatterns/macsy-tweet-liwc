from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="macsy_tweet_liwc",
    version="0.0.1",
    author="UoB Mediapatterns",
    description="Runs LIWC on Macsy tweets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/uob-mediapatterns/macsy-tweet-liwc",
    packages=['macsy_tweet_liwc'],
    install_requires=[
        'numpy',
        'macsy@git+https://github.com/uob-mediapatterns/macsy',
        'liwc@git+https://github.com/uob-mediapatterns/liwc',
        'python-dateutil'
    ]
)
