import argparse
import uuid


class RandomGenerator:

    def __init__(self):
        self.unique_strings_generated = set()

    @staticmethod
    def get_random_string():
        # Returns the random string, based on generated UUID4.
        return uuid.uuid4().hex.encode()

    def get_unique_random_string(self):
        # Returns the random string, which is unique for that RandomGenerator instance.
        # Saves generated string in self.unique_strings_generated
        while True:
            rand_str = self.get_random_string()
            if rand_str not in self.unique_strings_generated:
                self.unique_strings_generated.add(rand_str)
                break

    def get_unique_random_strings(self, count):
        # Returns a set of random strings, that are unique for that RandomGenerator instance.
        # Saves generated strings in self.unique_strings_generated
        old_strings = set(self.unique_strings_generated)  # Copy previous strings
        new_length = len(old_strings) + count
        # Generating strings until the target length is reached
        while len(self.unique_strings_generated) < new_length:
            rand_str = self.get_random_string()
            self.unique_strings_generated.add(rand_str)
        return self.unique_strings_generated - old_strings  # Returns only new strings


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

    return parser


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    if args.parse:
        pass  # do parsing
    else:  # creating is default option
        pass  # do creating


if __name__ == "__main__":
    main()
