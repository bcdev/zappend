from pathlib import Path
from urllib.error import URLError
from urllib.request import urlretrieve

ROBOTO_BASE_URL = "https://raw.githubusercontent.com/google/fonts/main/ofl/roboto"

DOWNLOADS = (
    {
        "name": "Roboto.ttf",
        "url": f"{ROBOTO_BASE_URL}/Roboto%5Bwdth,wght%5D.ttf",
    },
    {
        "name": "Roboto-Italic.ttf",
        "url": f"{ROBOTO_BASE_URL}/Roboto-Italic%5Bwdth,wght%5D.ttf",
    },
    {
        "name": "OFL.txt",
        "url": f"{ROBOTO_BASE_URL}/OFL.txt",
    },
)


def on_config(config):
    """Download self-hosted Roboto assets before MkDocs builds the site."""
    docs_dir = Path(config["docs_dir"])
    font_dir = docs_dir / "assets" / "downloaded" / "fonts"
    font_dir.mkdir(parents=True, exist_ok=True)

    for item in DOWNLOADS:
        target = font_dir / item["name"]
        if target.exists():
            continue

        try:
            print(f"Downloading Roboto asset: {item['name']}")
            urlretrieve(item["url"], target)
        except URLError as exc:
            raise RuntimeError(
                f"Could not download {item['name']} from {item['url']}. "
                "Check your network connection or place the file manually in "
                f"{font_dir}."
            ) from exc

    return config
