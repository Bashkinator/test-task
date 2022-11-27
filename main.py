import argparse
import logging
import os.path
import random
import sys
import uuid

from lxml import etree

logger = logging.getLogger('test-task')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)


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
        return uuid.uuid4().hex.encode()

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

    parser.add_argument('-v', '--verbose', action='store_true')

    return parser


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.parse:
        src_dir = os.path.abspath(args.source_dir)
        logger.info(f"Parsing zip-files from {src_dir}")
        pass  # do parsing
        logger.info(f"Zip-files are parsed, csv-files are created in {src_dir}")
    else:  # creating is default option
        out_dir = os.path.abspath(args.output_dir)
        logger.info(f"Creating {args.zip_count} zip-files with {args.xml_count} xml-files in each")
        pass  # do creating
        logger.info(f"{args.zip_count} zip-files with {args.xml_count} xml-files in each are created in {out_dir}")


if __name__ == "__main__":
    main()
