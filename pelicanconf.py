#!/usr/bin/env python
# -*- coding: utf-8 -*- #
from __future__ import unicode_literals

AUTHOR = "Sergei Lebedev"
SITEURL = "https://superbobry.github.io"
SITENAME = "The blog"
SITETITLE = "Hi."
SITESUBTITLE = " ".join([
    "I am Sergei Lebedev.",
    "I am a machine learning engineer at <a href='https://criteo.com'>Criteo</a>.",
    "This is my blog."
])
SITEDESCRIPTION = "Sergei Lebedev's Thoughts and Writings"
SITELOGO = "static/picture.jpg"
BROWSER_COLOR = "#333"
PYGMENTS_STYLE = "default"

ROBOTS = "index, follow"

CC_LICENSE = {
    "name": "Creative Commons Attribution-ShareAlike",
    "version": "4.0",
    "slug": "by-sa"
}

RELATIVE_URLS = True

MAIN_MENU = False
SOCIAL = [
    ("github", "https://github.com/superbobry"),
    ("twitter", "https://twitter.com/superbobry"),
]

THEME = "Flex"
PATH = "content"

TIMEZONE = "Europe/Paris"

I18N_TEMPLATES_LANG = "en"
DEFAULT_LANG = "en"
OG_LOCALE = "en_US"
LOCALE = "en_US"

DATE_FORMATS = {
    "en": "%B %d, %Y",
}

COPYRIGHT_YEAR = 2018

STATIC_PATHS = ["static"]
CUSTOM_CSS = "static/custom.css"

FEED_ATOM = FEED_RSS = None

SUMMARY_MAX_LENGTH = -1
