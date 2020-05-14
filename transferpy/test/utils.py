"""Utils for testing wmfmariadbpy."""

import sys


class hide_stderr:
    """Class used to hide the stderr."""

    class FakeStderr:
        """Class used as a fake stderr."""

        def write(self, s):
            """Just do nothing."""
            pass

    def __enter__(self):
        """Store the real stderr and place the fake one."""
        self.real_stderr = sys.stderr
        sys.stderr = self.FakeStderr()

    def __exit__(self, type, value, traceback):
        """Restore the real stderr."""
        sys.stderr = self.real_stderr
