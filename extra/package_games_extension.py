"""Create the Chrome Web Store draft ZIP without repository or personal files.

Run from any directory: python3 extra/package_games_extension.py
This does not upload or publish anything. HTTPS setup is still required.
"""
import json
from pathlib import Path
import re
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / 'extensions' / 'linkedin-games'
FILES = '''manifest.json akari.js api.js auto-config.js auto-submit.js
background.js extract.js leaderboard.js linkedin-auto.js linkedin-result.js
options.html options.css options.js personal.js popup.html popup.js style.css'''.split()


def main():
    manifest = json.loads((EXTENSION / 'manifest.json').read_text())
    files = sorted(set(FILES + list(manifest['icons'].values())))
    contents = {}
    for name in files:
        path = EXTENSION / name
        if not path.resolve().is_relative_to(EXTENSION.resolve()) or path.is_symlink():
            raise ValueError(f'Unexpected package path: {name}')
        data = path.read_bytes()
        if re.search(rb'tlegames_[A-Za-z0-9_-]{43}', data):
            raise ValueError(f'Possible credential in {name}; refusing to package')
        contents[name] = data
    assert len(manifest['description']) <= 132
    output = ROOT / 'dist' / f'tle-games-{manifest["version"]}-draft.zip'
    output.parent.mkdir(exist_ok=True)
    with ZipFile(output, 'w', ZIP_DEFLATED) as package:
        for name, data in contents.items():
            package.writestr(name, data)
    with ZipFile(output) as package:
        assert package.testzip() is None
        assert 'manifest.json' in package.namelist()
        assert set(package.namelist()) == set(files)
    print(f'Created {output} ({len(files)} files, {output.stat().st_size} bytes).')
    print('Draft upload only: finish HTTPS, privacy hosting, and reviewer access before submission.')


if __name__ == '__main__':
    main()
