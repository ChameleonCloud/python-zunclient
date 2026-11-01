================================
 The :mod:`zunclient` Python API
================================

.. module:: zunclient
   :synopsis: A client for the OpenStack Zun API.

.. currentmodule:: zunclient

Usage
-----

First create a client instance with your credentials::

    >>> from zunclient import client
    >>> zun = client.Client(VERSION, auth_url=AUTH_URL, username=USERNAME,
    ...                     password=PASSWORD, project_name=PROJECT_NAME,
    ...                     user_domain_name='default',
    ...                     project_domain_name='default')

Here ``VERSION`` can be a string or ``zunclient.api_versions.APIVersion`` obj.
If you prefer string value, you can use ``1`` or
``1.X`` (where X is a microversion).

Alternatively, you can create a client instance using the keystoneauth
session API::

    >>> from keystoneauth1 import loading
    >>> from keystoneauth1 import session
    >>> from zunclient import client
    >>> loader = loading.get_plugin_loader('password')
    >>> auth = loader.load_from_options(auth_url=AUTH_URL,
    ...                                 username=USERNAME,
    ...                                 password=PASSWORD,
    ...                                 project_name=PROJECT_NAME,
    ...                                 user_domain_name='default',
    ...                                 project_domain_name='default')
    >>> sess = session.Session(auth=auth)
    >>> zun = client.Client(VERSION, session=sess)

If you have PROJECT_NAME instead of a PROJECT_ID, use the project_name
parameter. Similarly, if your cloud uses keystone v3 and you have a DOMAIN_NAME
or DOMAIN_ID, provide it as `user_domain_(name|id)` and if you are using a
PROJECT_NAME also provide the domain information as `project_domain_(name|id)`.

Then call methods on its managers::

    >>> zun.containers.list()
    [<Container {...}>]

    >>> zun.containers.run(name="my-container", image='nginx')
    <Container {...}>

Running commands in a container
-------------------------------

These methods run a command over a websocket, through Zun's websocket proxy,
so a command can run for as long as it needs and pass any amount of data.
They need Zun's Kubernetes driver, as CHI@Edge runs, whose exec sessions
speak Kubernetes' remotecommand protocol.

A command is a list of arguments, or a string that is split into arguments
as a shell would split it. Either way no shell runs it; for pipes or
redirection, run one: ``['sh', '-c', 'dmesg | tail']``.

Run a command and wait for it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``execute_run`` runs a command, waits for it to exit, and returns its exit
code and output, like ``subprocess.run``::

    >>> result = zun.containers.execute_run(container, ['ls', '/srv'])
    >>> result.returncode
    0
    >>> result.stdout
    b'data\n'

* ``check=True`` raises ``zunclient.exceptions.CommandFailed`` for a non-zero
  exit code. Its ``returncode``, ``stdout`` and ``stderr`` say what happened.
* ``input`` is bytes, or a binary file, for the command's stdin. The command
  sees the end of its input after the last byte.
* ``timeout`` is how many seconds to wait. After that, ``execute_run`` raises
  ``k8s_exec.ExecTimeout`` with the output so far. The command keeps running,
  as when ``docker exec`` is interrupted.

Copy files
~~~~~~~~~~

``copy_to`` copies a local file or directory into a directory in the
container, and ``copy_from`` copies one out, like ``docker cp``::

    >>> zun.containers.copy_to(container, 'results/', '/srv')
    >>> zun.containers.copy_from(container, '/srv/results', 'downloads')

Both stream a tar archive through ``tar`` in the container, so files of any
size work, and the image needs ``tar``; BusyBox's is enough. As with
``docker cp``:

* Copied files belong to the user the command runs as in the container, not
  to their local owner.
* Symlinks are copied as links.
* Files copied before an error stay.

If ``tar`` in the container fails, for example because a path doesn't exist,
both raise ``CommandFailed``, with tar's message in ``stderr``.
``copy_from`` raises ``tarfile.FilterError`` for a file that would land
outside the destination, and ``tarfile.ReadError`` if the archive is cut off.

Exec sessions
~~~~~~~~~~~~~

``execute_session`` starts a command and returns an ``ExecSession`` right
away, to read its output as it arrives, give it input as it runs, or run it
in a terminal::

    >>> from zunclient.v1.containers import PIPE
    >>> count = ['sh', '-c', 'for i in 1 2 3; do echo $i; sleep 1; done']
    >>> session = zun.containers.execute_session(container, count, stdout=PIPE)
    >>> for line in session.stdout:
    ...     print(line)
    b'1\n'
    b'2\n'
    b'3\n'
    >>> session.wait()
    0
    >>> session.close()

Where ``stdout`` goes, and likewise ``stderr``:

``None``, the default
    Read it with ``session.recv()``, which returns ``(channel, data)`` for
    each piece of output that arrives, or with ``session.communicate()``.

a function that takes bytes, or a binary file
    The session calls or writes to it as output arrives.

``PIPE``
    ``session.stdout`` is a binary file to read until the command exits, as
    with ``subprocess.PIPE``.

``stdin=PIPE`` gives the command a stdin. Write to it through
``session.stdin``, a buffered binary file: ``flush()`` sends what has been
written, and ``close()`` sends the rest and ends the input. ``send(data)``
and ``close_stdin()`` do the same without a buffer::

    >>> session = zun.containers.execute_session(container, ['wc', '-c'],
    ...                                          stdin=PIPE)
    >>> session.communicate(b'hello')
    (b'5\n', b'')

``tty=True`` runs the command in a terminal, as ``docker exec -t`` does, for
programs such as ``vim`` or ``htop``. Output to stderr then comes as stdout.
Set the terminal's size with ``session.resize(columns, rows)``, and send
keystrokes with ``send()``; ``send(b'\x03')`` is Ctrl-C.

``session.wait()`` waits for the command to exit and returns its exit code.
``session.close()`` ends the session; like ``docker exec``'s client going
away, it leaves a command that is still running to run on. To stop a
command, run it in a terminal and send Ctrl-C, or have it exit at the end of
its input.

How a session reads and writes:

* The session reads on its own thread from the start, so output keeps
  arriving while you write input. A function given as ``stdout`` or
  ``stderr`` runs on that thread; if it raises, the session stops reading
  and ``wait()`` raises the error.
* Any thread may write.
* Output that isn't read holds the command back, as with any pipe, rather
  than filling memory. As with ``subprocess``, if both ``stdout`` and
  ``stderr`` are pipes, read both, or use a function for one.
* The session sends a websocket ping every 30 seconds, so that proxies don't
  close the connection while the command is quiet.

Errors:

``k8s_exec.SessionError``
    The session ended without the command's exit code, for example because
    the program doesn't exist or the connection was lost.

``k8s_exec.ExecTimeout``
    ``wait()``, ``recv()`` or ``communicate()`` ran out of time. The session
    stays open.

``zunclient.common.websocketclient.exceptions.Disconnected``
    A write after the session ended.

``zunclient.exceptions.UnsupportedVersion``
    Before Zun API 1.42, a command has stdin only in a terminal, so stdin
    without ``tty=True``, or ``tty=True`` without stdin, needs 1.42 or later.
    A client created with the default version, ``1.latest``, uses the
    server's newest.
