#!/usr/bin/env python
# -*- coding: utf-8 -*- #
from __future__ import unicode_literals

AUTHOR = "Sergei Lebedev"
SITEURL = ""
SITENAME = "The blog"
SITETITLE = "Hi."
SITESUBTITLE = " ".join([
    "I am Sergei Lebedev.",
    "I am a machine learning engineer at <a href='https://criteo.com'>Criteo.</a>",
    "This is my blog."
])
SITEDESCRIPTION = "Sergei Lebedev's Thoughts and Writings"
SITELOGO = "https://pbs.twimg.com/profile_images/664519682137960448/LCW2x5sj_400x400.jpg"
BROWSER_COLOR = "#333"
PYGMENTS_STYLE = "default"

MAIN_MENU = False

THEME = "flex"
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

DEFAULT_PAGINATION = 10
