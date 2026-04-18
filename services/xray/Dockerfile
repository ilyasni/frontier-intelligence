FROM python:3.11-slim

WORKDIR /opt/xray

ARG XRAY_DOWNLOAD_URL=https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip

RUN python -c "import stat, urllib.request, zipfile; from pathlib import Path; url='${XRAY_DOWNLOAD_URL}'; archive=Path('/tmp/xray.zip'); archive.write_bytes(urllib.request.urlopen(url, timeout=120).read()); zf=zipfile.ZipFile(archive); member=next(name for name in zf.namelist() if name.endswith('/xray') or name == 'xray'); target=Path('/usr/local/bin/xray'); target.write_bytes(zf.read(member)); target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)" \
 && /usr/local/bin/xray version

ENTRYPOINT ["/bin/sh", "/etc/xray/run-xray.sh"]
