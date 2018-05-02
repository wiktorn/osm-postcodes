import argparse
import collections
import concurrent.futures
import functools
import json
import pathlib
import urllib.request

import osmium
import shapely.wkb
import shapely.geometry
import tqdm

wkb_factory = osmium.geom.WKBFactory()


def osmium2shapely(o):
    #if isinstance(o, osmium.osm.Way):
    #    wkb = wkb_factory.create_linestring(o)
    #elif isinstance(o, osmium.osm.Area):
    wkb = wkb_factory.create_multipolygon(o)
    return shapely.wkb.loads(wkb, hex=True)


class PostcodeExtractor(osmium.SimpleHandler):
    def __init__(self, reporthook):
        super(PostcodeExtractor, self).__init__()
        self.postcodes = collections.defaultdict(list)
        self.reporthook = reporthook

    def node(self, n):
        self.reporthook(1)
        postcode = n.tags.get('addr:postcode')
        if postcode:
            self.accumulate_postcodes(postcode, shapely.geometry.Point(n.location.lon, n.location.lat))

    def area(self, a):
        self.reporthook(1)
        postcode = a.tags.get('addr:postcode')
        if postcode:
            # noinspection PyBroadException
            try:
                shape = osmium2shapely(a).representative_point()
                self.accumulate_postcodes(postcode, shape)
            except:
                pass

    def accumulate_postcodes(self, postcode: str, point: shapely.geometry.base.BaseGeometry):
        self.postcodes[postcode].append(point)


class UrlRetriveProgressBar(tqdm.tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def union_shapes(lst):
    first = lst[0]
    return functools.reduce(shapely.geometry.Point.union, lst[1:], first)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--osm-data", dest="osm_data", nargs='?',
                        default="http://download.geofabrik.de/europe/poland-latest.osm.pbf")
    parser.add_argument("--local-data", dest="local_data", nargs='?', default="data.pbf")
    args = parser.parse_args()

    if not pathlib.Path(args.local_data).exists():
        with UrlRetriveProgressBar(unit='B', unit_scale=True, miniters=1, desc=args.osm_data) as report:
            urllib.request.urlretrieve(url=args.osm_data, filename=args.local_data, reporthook=report.update_to)
    else:
        print("Skipping download as {} already exists".format(args.local_data))

    with tqdm.tqdm(desc="Extracting postcodes") as pb:
        pe = PostcodeExtractor(pb.update)
        pe.apply_file(args.local_data)

    ret = {}

    def job(_postcode, _positions):
        ret[_postcode] = union_shapes(_positions).centroid

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(job, postcode, positions) for postcode, positions in pe.postcodes.items()]

        # give meaningful progressbar
        for _ in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Generating position"
        ):
            pass

    for postcode in sorted(ret.keys()):
        pos = ret[postcode]
        print("{} {:09.6f} {:09.6f}".format(postcode, pos.y, pos.x))

    print(json.dumps(shapely.geometry.mapping(union_shapes(pe.postcodes['14-200']))))


if __name__ == '__main__':
    main()
