# Copyright(c)2014 NTT corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def get_format_type(conf, logger, key, default):
    """
    Get and check format_type value.
    """
    format_type = conf.get(key, default).lower()
    if format_type not in ('json', 'csv'):
        logger.warning(
            _("Invalid Parameter %s: %s, " % (key, format_type) +
              "use default %s.") % default)
        format_type = default
    return format_type
