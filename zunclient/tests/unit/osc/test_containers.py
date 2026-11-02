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


from unittest import mock

import testtools

from zunclient.osc.v1 import containers


class ContainerCommandTest(testtools.TestCase):

    def run_command(self, cls, *argv):
        app = mock.Mock()
        cmd = cls(app, None)
        parsed_args = cmd.get_parser('openstack').parse_args(argv)
        return app.client_manager.container, cmd.take_action(parsed_args)

    @mock.patch('zunclient.common.utils.exec_command', return_value=3)
    def test_exec_returns_exit_code(self, exec_command):
        client, result = self.run_command(
            containers.ExecContainer, 'c', 'sh', '-c', 'exit 3')
        self.assertEqual(3, result)
        exec_command.assert_called_once_with(client, 'c',
                                             ['sh', '-c', 'exit 3'])

    @mock.patch('zunclient.common.websocketclient.terminal.exec_interactive',
                return_value=130)
    def test_exec_interactive(self, exec_interactive):
        client, result = self.run_command(
            containers.ExecContainer, '--interactive', 'c', 'sh')
        self.assertEqual(130, result)
        exec_interactive.assert_called_once_with(client, 'c', ['sh'])

    def test_cp_from_container(self):
        client, _ = self.run_command(containers.CopyContainer,
                                     'c:/etc/hosts', '/tmp')
        client.containers.copy_from.assert_called_once_with(
            'c', '/etc/hosts', '/tmp')

    def test_cp_to_container(self):
        client, _ = self.run_command(containers.CopyContainer,
                                     '/tmp/a', 'c:/srv')
        client.containers.copy_to.assert_called_once_with(
            'c', '/tmp/a', '/srv')
