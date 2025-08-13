# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
from urllib.request import urlopen
from pathlib import Path
from datetime import datetime

project = 'Stream DaQ'
copyright = copyright = str(datetime.now().year) + ', DataLab AUTh'
author = 'DataLab AUTh'

master_doc = "index"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',  # For Google-style docstrings
    'sphinx_design',
    'sphinx_copybutton', # For copy button in code blocks
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

numfig = True

myst_enable_extensions = [
    "dollarmath",
    "amsmath",
    "deflist",
    "html_admonition",
    "html_image",
    "colon_fence",
    "smartquotes",
    "replacements",
    "linkify",
    "substitution",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


html_theme = "sphinx_book_theme"
html_logo = "../Stream DaQ logo.png"
html_title = "Stream DaQ Documentation"
html_copy_source = True
html_favicon = "../logo/quacklity/Quacklity_square_transparent.png"
html_last_updated_fmt = ""

html_theme_options = {
    "repository_url": "https://github.com/Bilpapster/Stream-DaQ",
    "use_repository_button": True,
    # for more pygment styles, see: https://pygments.org/styles/
    "pygments_light_style": "tango",
    "pygments_dark_style": "lightbulb",
}

# Add custom CSS
html_css_files = [
    'styles.css',
]

html_static_path = ['_static']

# This allows us to use substitutions in the documentation
rst_prolog = """
.. include:: /_templates/substitutions.rst
"""

