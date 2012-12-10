import re


class ParseException(Exception):
    pass


class LineConfigParser(object):
    def __init__(self, path):
        self.path = path
        self.sections = {}

    def _get_includes(self, section_key, includes=None):
        if includes is None:
            includes = set()

        if section_key in includes:
            return includes

        includes.add(section_key)

        for include_key in self.sections[section_key]['includes']:
            self._get_includes(include_key, includes)

        return includes

    def _get_section(self, section_key):
        if not section_key in self.sections:
            self.sections[section_key] = {
                'includes': [],
                'lines': [],
            }
        return self.sections[section_key]

    @classmethod
    def get_regular_expressions(cls, path, section_key):
        if path is None:
            return []

        exps = []
        parser = cls(path)
        parser.read()
        for line in parser.get_section_lines(section_key):
            exps.append(re.compile(line))
        return exps

    def get_section_lines(self, section_key):
        if section_key not in self.sections:
            return []

        lines = list()
        for include_key in self._get_includes(section_key):
            lines += self.sections[include_key]['lines']
        return lines

    def read(self):
        fd = open(self.path)
        current_section = None
        for line in fd:
            line = line.strip()

            # skip empty lines
            if len(line) == 0:
                continue

            # skip comments
            if line.startswith('#'):
                continue

            # look for bracket line
            if line.startswith('[') and line.endswith(']'):
                line = line[1:-1].strip()

                if len(line) == 0:
                    raise ParseException('Empty bracket line')

                colon_count = line.count(':')

                if colon_count > 1:
                    raise ParseException('Invalid bracket line')

                # new section
                if colon_count == 0:
                    current_section = self._get_section(line)
                    continue

                # include section
                section_key, include_key = line.split(':')
                section = self._get_section(section_key)
                section['includes'].append(include_key)
                continue

            # strip escape character
            if line.startswith('\\'):
                line = line[1:]

            # no current section
            if current_section is None:
                raise ParseException('No current section')

            # add line to current section
            current_section['lines'].append(line)

        # check if all referenced sections exist
        for section_key, section in self.sections.iteritems():
            for include_key in section['includes']:
                if not include_key in self.sections:
                    raise ParseException("Included section '%s' not found" %
                        (include_key))
