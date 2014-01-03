This project contains utilities for pulling video information from numerous systems.

Everything is stored in a mongo db.

Use the video-settings.json.template file to create your own video-settings.json file as it is required to do anything.

These scripts do have some dependencies.

sitemap.py should be run first
glass.py uses sitemap data to pull information from glass
videoutils.py is a shared utility class
brokenlinks.py is a shared utility class
