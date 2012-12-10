from StringIO import StringIO

import utils as test_utils
import slogging.line_config_parser as lcp


class LineConfigParser(lcp.LineConfigParser):
    def __init__(self, sections):
        self.sections = sections


class Open(object):
    def __init__(self, test, path, fobj):
        self.test = test
        self.path = path
        self.fobj = fobj

    def method(self, path, *a, **kw):
        self.test.assertEquals(self.path, path)
        return self.fobj


class TestLineConfigParser(test_utils.MockerTestCase):

    def test_init(self):
        path = 'some_path'
        parser = lcp.LineConfigParser(path)
        self.assertEquals(path, parser.path)
        self.assertEquals({}, parser.sections)

    def test_get_includes_no_section_includes(self):
        sections = {
            'section1': {'includes': []},
            'section2': {'includes': []},
            'section3': {'includes': []},
            'section4': {'includes': []},
        }
        parser = LineConfigParser(sections)
        includes = parser._get_includes('section1')
        self.assertEquals(set(['section1',]), includes)

    def test_get_includes_section_includes(self):
        sections = {
            'section1': {'includes': 'section2 section3'.split()},
            'section2': {'includes': []},
            'section3': {'includes': []},
            'section4': {'includes': []},
        }
        parser = LineConfigParser(sections)
        includes = parser._get_includes('section1')
        self.assertEquals(set('section1 section2 section3'.split()), includes)

    def test_get_includes_section_recursive_includes(self):
        sections = {
            'section1': {'includes': ['section2',]},
            'section2': {'includes': ['section3',]},
            'section3': {'includes': []},
            'section4': {'includes': []},
        }
        parser = LineConfigParser(sections)
        includes = parser._get_includes('section1')
        self.assertEquals(set('section1 section2 section3'.split()), includes)

    def test_get_includes_section_circular_recursive_includes(self):
        sections = {
            'section1': {'includes': ['section2',]},
            'section2': {'includes': ['section3',]},
            'section3': {'includes': ['section1',]},
            'section4': {'includes': []},
        }
        parser = LineConfigParser(sections)
        includes = parser._get_includes('section1')
        self.assertEquals(set('section1 section2 section3'.split()), includes)

    def test_get_includes_included_already(self):
        section_key = 'section1'
        init_includes = set('section1 section2'.split())

        parser = LineConfigParser({})
        ret_includes = parser._get_includes(section_key, set(init_includes))

        self.assertEquals(init_includes, ret_includes)

    def test_get_section(self):
        parser = LineConfigParser({})
        section_key = 'some_section_key'
        section = parser._get_section(section_key)

        self.assert_(section_key in parser.sections)
        self.assertEquals(section, parser.sections[section_key])
        self.assertEquals([], section['includes'])
        self.assertEquals([], section['lines'])

    def test_get_section_lines_for_unknown_section(self):
        parser = LineConfigParser({})
        lines = parser.get_section_lines('some_section_key')
        self.assertEquals([], lines)

    def test_get_section_lines(self):
        class LineConfigParser(lcp.LineConfigParser):
            def __init__(self, sections, includes):
                self.sections = sections
                self.includes = includes

            def _get_includes(self, *a, **kw):
                return self.includes

        sections = {
            'section1': {'lines': 'one two three'.split()},
            'section2': {'lines': 'four five'.split()},
            'section3': {'lines': 'six'.split()},
            'section4': {'lines': 'seven'.split()},
        }

        includes = 'section1 section2 section3'.split()

        lines = []
        for section in includes:
            lines.extend(sections[section]['lines'])

        parser = LineConfigParser(sections, includes)
        ret_lines = parser.get_section_lines('section1')
        self.assertEquals(lines, ret_lines)

    def test_read_empty_file(self):
        path = 'some_path'
        fobj = StringIO('')

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()

    def test_read_one_section(self):
        lines = [
            '[section1]',
            'one',
            'two',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()

        self.assertEquals(1, len(parser.sections))

        section = parser.sections['section1']

        self.assertEquals(0, len(section['includes']))
        self.assertEquals(lines[1:], section['lines'])

    def test_read_two_section(self):
        lines = [
            '[section1]',
            'one',
            'two',
            '',
            '[section2]',
            'three',
            'four',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()

        self.assertEquals(2, len(parser.sections))

        section1 = parser.sections['section1']
        self.assertEquals(0, len(section1['includes']))
        self.assertEquals(lines[1:3], section1['lines'])

        section2 = parser.sections['section2']
        self.assertEquals(0, len(section2['includes']))
        self.assertEquals(lines[-2:], section2['lines'])

    def test_read_two_section_one_include(self):
        lines = [
            '[section1]',
            'one',
            'two',
            '',
            '[section1:section2]',
            '',
            '[section2]',
            'three',
            'four',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()

        self.assertEquals(2, len(parser.sections))

        section1 = parser.sections['section1']
        self.assertEquals(['section2',], section1['includes'])
        self.assertEquals(lines[1:3], section1['lines'])

        section2 = parser.sections['section2']
        self.assertEquals(0, len(section2['includes']))
        self.assertEquals(lines[-2:], section2['lines'])

    def test_read_one_section_with_comment(self):
        lines = [
            '[section1]',
            '# comment',
            'one',
            'two',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()

        self.assertEquals(1, len(parser.sections))

        section = parser.sections['section1']

        self.assertEquals(0, len(section['includes']))
        self.assertEquals(lines[-2:], section['lines'])

    def test_read_empty_bracket_line(self):
        lines = [
            '[]',
            'one',
            'two',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        self.assertRaises(lcp.ParseException, parser.read)

    def test_read_tow_many_colons_in_bracket_line(self):
        lines = [
            '[a:b:c]',
            'one',
            'two',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        self.assertRaises(lcp.ParseException, parser.read)

    def test_read_escape_slash(self):
        lines = [
            '[section1]',
            '\one',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        parser.read()
        section = parser.sections['section1']
        self.assertEquals(['one',], section['lines'])

    def test_read_no_start_section(self):
        lines = [
            'one',
            '[section]',
            'two',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        self.assertRaises(lcp.ParseException, parser.read)

    def test_read_invalid_include(self):
        lines = [
            '[section1]',
            '[section1:section2]',
        ]

        path = 'some_path'
        fobj = StringIO('\n'.join(lines))

        open_ = Open(self, path, fobj)
        self.mock(lcp, 'open', open_.method)

        parser = lcp.LineConfigParser(path)
        self.assertRaises(lcp.ParseException, parser.read)
