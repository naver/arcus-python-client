#
# arcus-python-client - Arcus client event poller
# Copyright 2017 MaybeS.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import select
import sys

def get_poller(def_ev_mask=None):
    """
    This return poller for each system platform.
    Unfortunately, only linux and darwin(macOS) are supported at this time
    Inherit the Poller class and implement a poller for the other platform.

    @see also https://docs.python.org/2/library/sys.html#sys.platform
    """
    return {
        'darwin': darwinPoller(),
        'linux': linuxPoller(),
        'win32': winPoller(),
        'cygwin': cygwinPoller(),
    }[sys.platform]

class Poller(object):
    """
    This define base poller class

    modify, close and other methods no need to implement because it is not used by arcus

    @see also https://docs.python.org/3/library/select.html#edge-and-level-trigger-polling-epoll-objects
    """
    def __init__(self):
        pass

    def poll(self, timeout=None, max_events=None):
        """
        Wait for events. timeout in seconds (float)
        """
        pass

    def register(self, fd, event_mask=None):
        """
        Register a fd descriptor with the epoll object.
        """
        pass

    def unregister(self, fd):
        """
        Remove a registered file descriptor from the epoll object
        """
        pass


class linuxPoller(Poller):
    def __init__(self):
        Poller.__init__(self)
        self._poll = select.poll()

    def poll(self, timeout=None, max_events=None):
        return self._poll(timeout=timeout)

    def register(self, fd, event_mask=None):
        self._poll.register(fd , event_mask)

    def unregister(self, fd):
        self._poll.unregister(sock)

class darwinPoller(Poller):
    def __init__(self):
        Poller.__init__(self)
        self._kev_table = {}
        self._event_map = {
            select.POLLIN: select.KQ_FILTER_READ ,
            select.POLLOUT: select.KQ_FILTER_WRITE ,
        }
        self._rev_event_map = {}
        for ev , kev in self._event_map.items():
            self._rev_event_map[kev] = ev
        self._poll = select.kqueue()

    def poll(self, timeout=None, max_events=1):
        return [(self._sock_map[event.ident], self._rev_event_map[event.filter]) 
                for event in self._poll.control(self._kev_table.values(), max_events, timeout)]


    def register(self, fd, event_mask=None):
        ke = self._get_kevent(sock , event_mask)
        self._kev_table[fd.fileno()] = ke
        self._sock_map[fd.fileno()] = fd

    def unregister(self, fd):
        del self._kev_table[fd.fileno()]
        del self._sock_map[fd.fileno()]

# I have no idea for this
class winPoller(Poller):
    pass

# It can be implemented similarly to darwin
# because it looks like it supports kqueue.
class cygwinPoller(Poller):
    pass
