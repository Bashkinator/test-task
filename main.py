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

from xml.etree import ElementTree


#  TODO: logging from multiple processes
#  TODO: exception handling


def init_logger(level=logging.INFO):
    """Init basic logger for app"""
    logger = logging.getLogger('test-task')
    logger.setLevel(level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger


# not thread/process safe
def get_logger():
    """Get basic logger for app"""
    return logging.getLogger('test-task')


class TestObject:
    """
    Object for xml-files

    Attributes
    ----------
    object_id : str
        Unique random string for Object ID in xml. Generate new if None in init function parameters.
    level : int
        Random number from 1 to 100. Generate new if None in init function parameters.
    object_names : list(str)
        List of random strings for object names in xml-file. Generate new if None in init function parameters.
    """

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
        """Construct TestObject from xml string"""
        xml_root = ElementTree.fromstring(xml_string)
        object_id = xml_root[0].get("value")
        level = xml_root[1].get("value")
        xml_objects = xml_root[2]
        object_names = []
        for xml_obj in xml_objects.iter("object"):
            object_names.append(xml_obj.get("name"))
        return cls(object_id=object_id, level=level, object_names=object_names)

    def serialize(self):
        """Convert TestObject to xml string"""
        root = ElementTree.Element("root")
        ElementTree.SubElement(root, "var", attrib={"name": "id", "value": self.object_id})
        ElementTree.SubElement(root, "var", attrib={"name": "level", "value": str(self.level)})
        objects = ElementTree.SubElement(root, "objects")
        for name in self.object_names:
            ElementTree.SubElement(objects, "object", attrib={"name": name})
        return ElementTree.tostring(root, xml_declaration=True)


class RandomGenerator:
    """
    Class for string random generation
    """
    # Set for checking string uniqueness
    unique_strings_generated = set()

    @staticmethod
    def get_random_string():
        """Returns the random string, based on generated UUID4."""
        return uuid.uuid4().hex

    @classmethod
    def get_unique_random_string(cls):
        """
        Returns the random string, which is unique for the script run.
        Saves generated string in self.unique_strings_generated
        """
        while True:
            rand_str = cls.get_random_string()
            if rand_str not in cls.unique_strings_generated:
                cls.unique_strings_generated.add(rand_str)
                return rand_str

    @classmethod
    def get_unique_random_strings(cls, count):
        """
        Returns a set of random strings, that are unique for the script run.
        Saves generated strings in self.unique_strings_generated
        """
        old_strings = set(cls.unique_strings_generated)  # Copy previous strings
        new_length = len(old_strings) + count
        # Generating strings until the target length is reached
        while len(cls.unique_strings_generated) < new_length:
            rand_str = cls.get_random_string()
            cls.unique_strings_generated.add(rand_str)
        return cls.unique_strings_generated - old_strings  # Returns only new strings


def create_testfile(object_id, file, lock):
    """
    Create TestObject for object_id, serialize it to xml and save to zip-file

    Parameters
    ----------
    object_id : str
        Random generated unique string for Object ID
    file : zipfile.ZipFile
        Zip-file for saving
    lock : threading.Lock
        Lock for ensuring access to zip-file
    """
    obj_xml_str = TestObject(object_id).serialize()
    with lock:
        file.writestr(f"{object_id}.xml", obj_xml_str)
    return object_id


def create_zipfile(name, object_ids, out_dir):
    """
    Create zip-file and fill it with xml-files

    Parameters
    ----------
    name : str
        Filename for zipfile
    object_ids : list(str)
        Random generated IDs for xml-files.
    out_dir : str
        Absolute path to output directory

    Returns
    -------
    Returns parameters back in dict
    """
    # Creating zip-file in memory
    zip_buffer = io.BytesIO()
    lock = threading.Lock()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with ThreadPoolExecutor() as exe:  # using threads for memory copy
            futures = [exe.submit(create_testfile, object_id, zf, lock) for object_id in object_ids]
            # ThreadPoolExecutor wait for futures before shutdown
    # Write zip buffer to file
    # No need in multithreading. We are already in separate process
    filename = os.path.join(out_dir, name)
    with open(filename, "wb") as f:
        f.write(zip_buffer.getvalue())
    return {
        "name": name,
        "out_dir": out_dir,
        "object_ids": object_ids
    }


def create_files(zip_count, xml_count, out_dir):
    """
    Create files for task

    Parameters
    ----------
    zip_count : int
        Required number of zip-files.
    xml_count : int
        Required number of xml-files in each zip-file.
    out_dir : str
        Absolute path to output directory.

    Returns
    -------
    Returns back parameters in list
    """
    # Generating objects ids beforehand
    all_object_ids = RandomGenerator.get_unique_random_strings(zip_count * xml_count)
    object_ids_iter = iter(all_object_ids)
    files_created = 0
    # Compressing is CPU-bound, so using processes
    with ProcessPoolExecutor() as exe:
        futures = list()
        for i in range(0, zip_count):
            objects_ids = list(itertools.islice(object_ids_iter, xml_count))  # get chunk of ids from set iterator
            futures.append(exe.submit(create_zipfile, f"{i + 1}.zip", objects_ids, out_dir))
        for future in as_completed(futures):
            try:
                res = future.result()
                files_created += 1
                get_logger().debug(f"{res['name']} is created")
            except Exception as e:  # very basic exception handling
                get_logger().exception("Zip-file creation failed", exc_info=e)
    return files_created


def create_levels_file(test_objects, out_dir):
    """
    Create levels.csv

    Parameters
    ----------
    test_objects : list(TestObject)
        List with parsed objects for processing
    out_dir : str
        Absolute path to output directory.

    Returns
    -------
    Returns filename of created csv-file
    """
    filename = os.path.join(out_dir, "levels.csv")
    with open(filename, 'w', newline='') as csvfile:
        levels_writer = csv.writer(csvfile)
        for test_object in test_objects:
            levels_writer.writerow([test_object.object_id, test_object.level])
    return filename


def create_names_file(test_objects, out_dir):
    """
    Create names.csv

    Parameters
    ----------
    test_objects : list(TestObject)
        List with parsed objects for processing
    out_dir : str
        Absolute path to output directory.

    Returns
    -------
    Returns filename of created csv-file
    """
    filename = os.path.join(out_dir, "names.csv")
    with open(filename, 'w', newline='') as csvfile:
        names_writer = csv.writer(csvfile)
        for test_object in test_objects:
            for name in test_object.object_names:
                names_writer.writerow([test_object.object_id, name])


def extract_test_objects(zip_path):
    """
    Parse files and create csv-files for task

    Parameters
    ----------
    zip_path : str
        Absolute path to zip-file.

    Returns
    -------
    Dict with:
        filename : str
            Absolute path to processed zip-file.
        test_objects : list(TestObject)
            List with unpacked objects for processing
    """
    with zipfile.ZipFile(zip_path, 'r') as zf:
        xml_files = [zf.read(name) for name in zf.namelist()]
    test_objects = list()
    with ThreadPoolExecutor() as exe:
        futures = list()
        for xml in xml_files:
            futures.append(exe.submit(TestObject.from_xml_string, xml))
        for future in as_completed(futures):
            test_objects.append(future.result())
    return {
        "filename": zip_path,
        "test_objects": test_objects
    }


def parse_files(src_dir, out_dir):
    """
    Parse files and create csv-files for task

    Parameters
    ----------
    src_dir : str
        Absolute path to output directory.
    out_dir : str
        Absolute path to output directory.

    Returns
    -------
    Tuple of file_processed and file_created counters
    """
    test_objects = list()
    # collect filepaths of zip-files
    filepaths = [os.path.join(src_dir, f) for f in os.listdir(src_dir) if
                 os.path.isfile(os.path.join(src_dir, f)) and f.endswith('.zip')]
    files_processed = 0
    # Decompressing is CPU-bound, so using processes
    with ProcessPoolExecutor() as exe:
        futures = list()
        for filepath in filepaths:
            futures.append(exe.submit(extract_test_objects, filepath))
        for future in as_completed(futures):
            try:
                res = future.result()
                # save parsed objects for future processing
                test_objects.extend(res["test_objects"])
                files_processed += 1
                get_logger().debug(f"{res['filename']} is parsed")
            except Exception as e:  # very basic exception handling
                get_logger().exception("Zip-file creation failed", exc_info=e)
    # Processing parsed objects and creating csv-files
    get_logger().info("Processing objects and creating csv-files")
    files_created = 0
    # Writing to file is IO-bound, so using threads
    with ThreadPoolExecutor(2) as exe:
        csv_futures = list()
        csv_futures.append(exe.submit(create_levels_file, test_objects, out_dir))
        csv_futures.append(exe.submit(create_names_file, test_objects, out_dir))
        for future in as_completed(csv_futures):
            try:
                future.result()
                files_created += 1
            except Exception as e:  # very basic exception handling
                get_logger().exception("Zip-file creation failed", exc_info=e)
    return files_processed, files_created


def init_argparse():
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


def main():
    # Parsing arguments
    parser = init_argparse()
    args = parser.parse_args()

    # Initializing logger
    logger = init_logger(logging.DEBUG if args.verbose else logging.INFO)

    if args.parse:
        # Construct absolute paths
        src_dir = os.path.abspath(args.source_dir)
        if not os.path.exists(src_dir) or not os.path.isdir(src_dir):
            logger.error(f"Source directory {src_dir} is invalid")
            return
        out_dir = os.path.abspath(args.output_dir)
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)  # make output directory
        logger.info(f"Parsing zip-files from {src_dir}")
        file_processed, file_created = parse_files(src_dir, out_dir)  # do parsing
        logger.info(f"{file_processed} zip-files are parsed, {file_created} csv-files are created in {out_dir}")
    else:  # creating is default option
        # Construct absolute path
        out_dir = os.path.abspath(args.output_dir)
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)  # make output directory
        logger.info(f"Creating {args.zip_count} zip-files with {args.xml_count} xml-files in each")
        files_created = create_files(args.zip_count, args.xml_count, out_dir)  # do creating
        logger.info(f"{files_created} zip-files with {args.xml_count} xml-files in each are created in {out_dir}")


if __name__ == "__main__":
    main()
