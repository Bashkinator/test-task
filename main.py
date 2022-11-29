import argparse
import csv
import io
import itertools
import logging
import os.path
import pathlib
import random
import sys
import threading
import uuid
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

from lxml import etree


def init_logger(level=logging.INFO):
    logger = logging.getLogger('test-task')
    logger.setLevel(level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger


# not thread/process safe
def get_logger():
    return logging.getLogger('test-task')


class TestObject:

    def __init__(self, object_id=None, level=None, object_names=None):
        self.object_id = object_id if object_id else RandomGenerator.get_unique_random_string()
        self.level = level if level else random.randint(1, 100)
        if object_names is not None:
            self.object_names = object_names
        else:
            self.object_names = [
                RandomGenerator.get_random_string()
                for _ in range(random.randint(1, 10))
            ]

    def __str__(self):
        return f"TestObject ID={self.object_id} level={self.level}, objects={self.object_names}"

    def __repr__(self):
        return str(self)

    @classmethod
    def from_xml_string(cls, xml_string):
        xml_root = etree.XML(xml_string)
        object_id = xml_root[0].get("value")
        level = xml_root[1].get("value")
        xml_objects = xml_root[2]
        object_names = []
        for xml_obj in xml_objects.iter("object"):
            object_names.append(xml_obj.get("name"))
        return cls(object_id=object_id, level=level, object_names=object_names)

    def serialize(self):
        root = etree.Element("root")
        etree.SubElement(root, "var", attrib={"name": "id", "value": self.object_id})
        etree.SubElement(root, "var", attrib={"name": "level", "value": str(self.level)})
        objects = etree.SubElement(root, "objects")
        for name in self.object_names:
            etree.SubElement(objects, "object", attrib={"name": name})
        tree = etree.ElementTree(root)
        return tree


class RandomGenerator:
    unique_strings_generated = set()

    @staticmethod
    def get_random_string():
        # Returns the random string, based on generated UUID4.
        return uuid.uuid4().hex

    @classmethod
    def get_unique_random_string(cls):
        # Returns the random string, which is unique for the script run.
        # Saves generated string in self.unique_strings_generated
        while True:
            rand_str = cls.get_random_string()
            if rand_str not in cls.unique_strings_generated:
                cls.unique_strings_generated.add(rand_str)
                return rand_str

    @classmethod
    def get_unique_random_strings(cls, count):
        # Returns a set of random strings, that are unique for the script run.
        # Saves generated strings in self.unique_strings_generated
        old_strings = set(cls.unique_strings_generated)  # Copy previous strings
        new_length = len(old_strings) + count
        # Generating strings until the target length is reached
        while len(cls.unique_strings_generated) < new_length:
            rand_str = cls.get_random_string()
            cls.unique_strings_generated.add(rand_str)
        return cls.unique_strings_generated - old_strings  # Returns only new strings


def create_testfile(object_id, file, lock):
    obj_tree = TestObject(object_id).serialize()
    obj_str = etree.tostring(obj_tree, xml_declaration=True)
    with lock:
        file.writestr(f"{object_id}.xml", obj_str)
    return object_id


def create_zipfile(name, object_ids, out_dir):
    zip_buffer = io.BytesIO()
    lock = threading.Lock()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with ThreadPoolExecutor(10) as exe:
            futures = [exe.submit(create_testfile, object_id, zf, lock) for object_id in object_ids]
            for future in as_completed(futures):
                _ = future.result()
    filename = os.path.join(out_dir, name)
    with open(filename, "wb") as f:
        f.write(zip_buffer.getvalue())
    return [name, out_dir, object_ids]


def create_files(zip_count, xml_count, out_dir, worker_count):
    all_object_ids = RandomGenerator.get_unique_random_strings(zip_count * xml_count)
    object_ids_iter = iter(all_object_ids)
    with ProcessPoolExecutor(worker_count) as exe:
        futures = list()
        for i in range(0, zip_count):
            objects_ids = list(itertools.islice(object_ids_iter, xml_count))  # get chunk of names from set iterator
            futures.append(exe.submit(create_zipfile, f"{i+1}.zip", objects_ids, out_dir))
        for future in as_completed(futures):
            res = future.result()
            get_logger().debug(res)


def create_levels_file(test_objects, out_dir):
    filename = os.path.join(out_dir, "levels.csv")
    with open(filename, 'w', newline='') as csvfile:
        levels_writer = csv.writer(csvfile)
        for test_object in test_objects:
            levels_writer.writerow([test_object.object_id, test_object.level])


def create_names_file(test_objects, out_dir):
    filename = os.path.join(out_dir, "names.csv")
    with open(filename, 'w', newline='') as csvfile:
        names_writer = csv.writer(csvfile)
        for test_object in test_objects:
            for name in test_object.object_names:
                names_writer.writerow([test_object.object_id, name])


def process_test_object(xml_string):
    test_object = TestObject.from_xml_string(xml_string)
    return test_object


def extract_test_objects(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zf:
        xml_files = [zf.read(name) for name in zf.namelist()]
    test_objects = list()
    with ThreadPoolExecutor(10) as exe:
        futures = list()
        for xml in xml_files:
            futures.append(exe.submit(TestObject.from_xml_string, xml))
        for future in as_completed(futures):
            test_objects.append(future.result())
    return test_objects


def parse_files(src_dir, out_dir, worker_count):
    test_objects = list()
    filepaths = [os.path.join(src_dir, f) for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f)) and f.endswith('.zip') ]
    with ProcessPoolExecutor(worker_count) as exe:
        futures = list()
        for filepath in filepaths:
            futures.append(exe.submit(extract_test_objects, filepath))
        for future in as_completed(futures):
            res = future.result()
            test_objects.extend(res)
            # get_logger().debug(res)
        csv_futures = list()
        csv_futures.append(exe.submit(create_levels_file, test_objects, out_dir))
        csv_futures.append(exe.submit(create_names_file, test_objects, out_dir))
        for future in as_completed(csv_futures):
            _ = future.result()


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create zip archives with xml files or parse directory with zip-archives and create csv files "
                    "from xmls.",
    )

    parser.add_argument('-c', '--create', action='store_true')
    parser.add_argument("-z", "--zip-count", action='store', type=int, default=50)
    parser.add_argument("-x", "--xml-count", action='store', type=int, default=100)

    parser.add_argument('-p', '--parse', action='store_true')

    parser.add_argument('-s', '--source-dir', action='store', default='./out')
    parser.add_argument('-o', '--output-dir', action='store', default='./out')

    parser.add_argument("-w", "--worker-count", action='store', type=int, default=4)

    parser.add_argument('-v', '--verbose', action='store_true')

    return parser


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()

    logger = init_logger(logging.DEBUG if args.verbose else logging.INFO)

    if args.parse:
        src_dir = os.path.abspath(args.source_dir)
        out_dir = os.path.abspath(args.output_dir)
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Parsing zip-files from {src_dir}")
        parse_files(src_dir, out_dir, args.worker_count)  # do parsing
        logger.info(f"Zip-files are parsed, csv-files are created in {out_dir}")
    else:  # creating is default option
        out_dir = os.path.abspath(args.output_dir)
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Creating {args.zip_count} zip-files with {args.xml_count} xml-files in each")
        create_files(args.zip_count, args.xml_count, out_dir, args.worker_count)  # do creating
        logger.info(f"{args.zip_count} zip-files with {args.xml_count} xml-files in each are created in {out_dir}")


if __name__ == "__main__":
    main()
