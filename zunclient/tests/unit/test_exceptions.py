#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import testtools

from zunclient import exceptions


class CommandFailedTest(testtools.TestCase):

    def message(self, stderr):
        return str(exceptions.CommandFailed('1234', ['tar'], 2, b'', stderr))

    def test_message_includes_stderr(self):
        self.assertEqual(
            "Command ['tar'] in container 1234 returned non-zero exit "
            "status 2: tar: /x: Cannot open: No such file or directory",
            self.message(b'tar: /x: Cannot open: No such file or directory\n'))

    def test_message_without_stderr(self):
        expect = ("Command ['tar'] in container 1234 returned non-zero exit "
                  "status 2")
        self.assertEqual(expect, self.message(b''))
        self.assertEqual(expect, self.message(b' \n'))

    def test_message_replaces_undecodable_bytes(self):
        self.assertTrue(self.message(b'bad \xff\n').endswith(': bad �'))
