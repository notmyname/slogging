import gettext

#: Version information (major, minor, revision[, 'dev']).
version_info = (1, 1, 4)
#: Version string 'major.minor.revision'.
version = __version__ = ".".join(map(str, version_info))

gettext.install('slogging')
