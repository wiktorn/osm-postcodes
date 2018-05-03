import argparse
import pathlib
import urllib.request

import tqdm

from postcodes import postcodes


class UrlRetrieveProgressBar(tqdm.tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--osm-data", dest="osm_data", nargs='?',
                        default="http://download.geofabrik.de/europe/poland-latest.osm.pbf")
    parser.add_argument("--local-data", dest="local_data", nargs='?', default="data.pbf")
    args = parser.parse_args()

    local_data = pathlib.Path(args.local_data)
    if not local_data.exists():
        with UrlRetrieveProgressBar(unit='B', unit_scale=True, miniters=1, desc=args.osm_data) as report:
            urllib.request.urlretrieve(url=args.osm_data, filename=args.local_data, reporthook=report.update_to)
    else:
        print("Skipping download as {} already exists".format(args.local_data))

    ret = postcodes.get_postcodes(local_data)

    for postcode in sorted(ret.keys()):
        pos = ret[postcode]
        print("{} {:09.6f} {:09.6f}".format(postcode, pos.y, pos.x))


if __name__ == '__main__':
    main()
