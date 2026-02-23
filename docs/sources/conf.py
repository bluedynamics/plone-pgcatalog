# Configuration file for the Sphinx documentation builder.

# -- Project information -----------------------------------------------------

project = "plone.pgcatalog"
copyright = "2024-2026, BlueDynamics Alliance"  # noqa: A001
author = "Jens Klein and contributors"
release = "1.0"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinx_design",
    "sphinx_copybutton",
]

myst_enable_extensions = [
    "deflist",
    "colon_fence",
    "fieldlist",
]

myst_fence_as_directive = ["mermaid"]

templates_path = ["_templates"]
exclude_patterns = []

# mermaid options
mermaid_output_format = "raw"

# -- Options for HTML output -------------------------------------------------

html_theme = "shibuya"

html_theme_options = {
    "logo_target": "/",
    "accent_color": "cyan",
    "color_mode": "dark",
    "dark_code": True,
    "nav_links": [
        {
            "title": "GitHub",
            "url": "https://github.com/bluedynamics/plone-pgcatalog",
        },
        {
            "title": "PyPI",
            "url": "https://pypi.org/project/plone.pgcatalog/",
        },
    ],
}

html_static_path = ["_static"]
html_logo = "_static/logo-web.png"
html_favicon = "_static/favicon.ico"
