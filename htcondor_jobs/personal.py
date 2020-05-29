# Copyright 2020 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Mapping, List

import logging
import atexit
import os
import shlex
import subprocess
import functools
import textwrap
import time
from pathlib import Path

import htcondor

from . import scheduler, utils

logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    "LOCAL_CONFIG_FILE": "",
    "MASTER_ADDRESS_FILE": "$(LOG)/.master_address",
    "COLLECTOR_ADDRESS_FILE": "$(LOG)/.collector_address",
    "SCHEDD_ADDRESS_FILE": "$(LOG)/.schedd_address",
    "JOB_QUEUE_LOG": "$(SPOOL)/job_queue.log",
    "UPDATE_INTERVAL": "2",
    "POLLING_INTERVAL": "2",
    "NEGOTIATOR_INTERVAL": "2",
    "STARTER_UPDATE_INTERVAL": "2",
    "STARTER_INITIAL_UPDATE_INTERVAL": "2",
    "NEGOTIATOR_CYCLE_DELAY": "2",
    "RUNBENCHMARKS": "0",
    "MAX_JOB_QUEUE_LOG_ROTATIONS": "0",
}


def skip_if(*states):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.state in states:
                logger.debug(
                    "Skipping call to {} for {} because its state is {}".format(
                        func.__name__, self, self.state
                    )
                )
                return

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


class PersonalCondorState(utils.StrEnum):
    UNINITIALIZED = "uninitialized"
    INITIALIZED = "initialized"
    STARTED = "started"
    READY = "ready"
    STOPPING = "stopping"
    STOPPED = "stopped"


class PersonalCondor:
    """
    A :class:`PersonalCondor` is responsible for managing the lifecycle of a
    personal HTCondor pool.
    """

    def __init__(
        self,
        local_dir: Optional[Path] = None,
        config: Mapping[str, str] = None,
        raw_config: str = None,
    ):
        """
        Parameters
        ----------
        local_dir
            The local directory for the HTCondor pool. All HTCondor state will
            be stored in this directory.
        config
            HTCondor configuration parameters to inject, as a mapping of key-value pairs.
        raw_config
            Raw HTCondor configuration language to inject, as a string.
        """
        self._state = PersonalCondorState.UNINITIALIZED
        atexit.register(self._atexit)

        if local_dir is None:
            local_dir = Path.cwd() / ".condor-personal"
        self.local_dir = local_dir

        self.execute_dir = self.local_dir / "execute"
        self.lock_dir = self.local_dir / "lock"
        self.log_dir = self.local_dir / "log"
        self.run_dir = self.local_dir / "run"
        self.spool_dir = self.local_dir / "spool"
        self.passwords_dir = self.local_dir / "passwords.d"
        self.tokens_dir = self.local_dir / "tokens.d"

        self.config_file = self.local_dir / "condor_config"

        if config is None:
            config = {}
        self.config = {k: v if v is not None else "" for k, v in config.items()}
        self.raw_config = raw_config or ""

        self.condor_master = None

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        old_state = self._state
        self._state = state
        logger.debug("State of {} changed from {} to {}".format(self, old_state, state))

    def __repr__(self):
        return "{}(local_dir = {}, state = {})".format(
            type(self).__name__, self.local_dir, self.state
        )

    def use_config(self):
        """
        Returns a context manager that sets ``CONDOR_CONFIG`` to point to the
        config file for this HTCondor pool.
        """
        return SetCondorConfig(self.config_file)

    def schedd(self):
        """Return the :class:`htcondor.Schedd` for this pool's schedd."""
        with self.use_config():
            return htcondor.Schedd()

    def collector(self):
        """Return the :class:`htcondor.Collector` for this pool's collector."""
        with self.use_config():
            return htcondor.Collector()

    def scheduler(self):
        return scheduler.Scheduler.from_personal(self)

    @property
    def master_is_alive(self):
        return self.condor_master is not None and self.condor_master.poll() is None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Stop triggered for {} by context exit.".format(self))
        self.stop()

    def __del__(self):
        logger.debug("Stop triggered for {} by object deletion.".format(self))
        self.stop()

    def _atexit(self):
        logger.debug("Stop triggered for {} by interpreter shutdown.".format(self))
        self.stop()

    @skip_if(PersonalCondorState.READY)
    def start(self):
        logger.info("Starting {}".format(self))

        try:
            self._initialize()
            self._ready()
        except BaseException:
            logger.exception(
                "Encountered error during setup of {}, cleaning up!".format(self)
            )
            self.stop()
            raise

        logger.info("Started {}".format(self))

    @skip_if(PersonalCondorState.INITIALIZED)
    def _initialize(self):
        self._setup_local_dirs()
        self._write_config()
        self.state = PersonalCondorState.INITIALIZED

    def _setup_local_dirs(self):
        for dir in (
            self.local_dir,
            self.execute_dir,
            self.lock_dir,
            self.log_dir,
            self.run_dir,
            self.spool_dir,
            self.passwords_dir,
            self.tokens_dir,
        ):
            dir.mkdir(parents=True, exist_ok=True)
            logger.debug("Created dir {}".format(dir))

    def _write_config(self):
        # TODO: switch to -summary instead of -write:up
        write = run_command(
            ["condor_config_val", "-write:up", self.config_file.as_posix()],
        )
        if write.returncode != 0:
            raise Exception("Failed to copy base OS config: {}".format(write.stderr))

        param_lines = []

        param_lines += ["#", "# ROLES", "#"]
        param_lines += [
            "use ROLE: CentralManager",
            "use ROLE: Submit",
            "use ROLE: Execute",
        ]

        base_config = {
            "LOCAL_DIR": self.local_dir.as_posix(),
            "EXECUTE": self.execute_dir.as_posix(),
            "LOCK": self.lock_dir.as_posix(),
            "LOG": self.log_dir.as_posix(),
            "RUN": self.run_dir.as_posix(),
            "SPOOL": self.spool_dir.as_posix(),
            "SEC_PASSWORD_DIRECTORY": self.passwords_dir.as_posix(),
            "SEC_TOKEN_SYSTEM_DIRECTORY": self.tokens_dir.as_posix(),
            "STARTD_DEBUG": "D_FULLDEBUG D_COMMAND",
        }

        param_lines += ["#", "# BASE PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in base_config.items()]

        param_lines += ["#", "# DEFAULT PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in DEFAULT_PARAMS.items()]

        param_lines += ["#", "# CUSTOM PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in self.config.items()]

        param_lines += ["#", "# RAW PARAMS", "#"]
        param_lines += textwrap.dedent(self.raw_config).splitlines()

        with self.config_file.open(mode="a") as f:
            f.write("\n".join(param_lines))

        logger.debug("Wrote config file for {} to {}".format(self, self.config_file))

    def _ready(self):
        # TODO: check for existing running condor using address files and condor_who -quick?
        self._start_condor()
        self._wait_for_ready()

    @property
    def _has_master(self):
        return self.condor_master is not None

    @skip_if(PersonalCondorState.STARTED)
    def _start_condor(self):
        with SetCondorConfig(self.config_file):
            self.condor_master = subprocess.Popen(
                ["condor_master", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            logger.debug(
                "Started condor_master (pid {})".format(self.condor_master.pid)
            )

    def _daemons(self):
        return set(
            self.run_command(["condor_config_val", "DAEMON_LIST"],).stdout.split(" ")
        )

    @skip_if(PersonalCondorState.READY)
    def _wait_for_ready(self, timeout=120):
        daemons = self._daemons()
        master_log_path = self.master_log

        logger.debug(
            "Starting up daemons for {}, waiting for: {}".format(
                self, " ".join(sorted(daemons))
            )
        )

        start = time.time()
        while time.time() - start < timeout:
            time_to_give_up = int(timeout - (time.time() - start))

            # if the master log does not exist yet, we can't use condor_who
            if not master_log_path.exists():
                logger.debug(
                    "MASTER_LOG at {} does not yet exist for {}, retrying in 1 seconds (giving up in {} seconds).".format(
                        master_log_path, self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who = self.run_command(
                shlex.split(
                    "condor_who -wait:10 'IsReady && STARTD_State =?= \"Ready\"'"
                ),
            )
            print(who.stdout)
            if who.stdout.strip() == "":
                logger.debug(
                    "condor_who stdout was unexpectedly blank for {}, retrying in 1 second (giving up in {} seconds).".format(
                        self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who_ad = dict(kv.split(" = ") for kv in who.stdout.splitlines())

            if (
                who_ad.get("IsReady") == "true"
                and who_ad.get("STARTD_State") == '"Ready"'
                and all(who_ad.get(d) == '"Alive"' for d in daemons)
            ):
                self.state = PersonalCondorState.READY
                return

            logger.debug(
                "{} is waiting for daemons to be ready (giving up in {} seconds)".format(
                    self, time_to_give_up
                )
            )

        self.run_command(["condor_who", "-quick"])
        raise TimeoutError("Standup for {} failed".format(self))

    @skip_if(
        PersonalCondorState.STOPPED,
        PersonalCondorState.UNINITIALIZED,
        PersonalCondorState.INITIALIZED,
    )
    def stop(self):
        logger.info("Stopping {}".format(self))

        self.state = PersonalCondorState.STOPPING

        self._condor_off()
        self._wait_for_master_to_terminate()

        self.state = PersonalCondorState.STOPPED

        logger.info("Stopped {}".format(self))

    def _condor_off(self):
        if not self.master_is_alive:
            return

        off = self.run_command(["condor_off", "-daemon", "master"], timeout=30)

        if not off.returncode == 0:
            logger.error(
                "condor_off failed, exit code: {}, stderr: {}".format(
                    off.returncode, off.stderr
                )
            )
            self._terminate_condor_master()
            return

        logger.debug("condor_off succeeded: {}".format(off.stdout))

    def _terminate_condor_master(self):
        if not self.master_is_alive:
            return

        self.condor_master.terminate()
        logger.debug(
            "Sent terminate signal to condor_master (pid {})".format(
                self.condor_master.pid
            )
        )

    def _kill_condor_master(self):
        self.condor_master.kill()
        logger.debug(
            "Sent kill signal to condor_master (pid {})".format(self.condor_master.pid)
        )

    def _wait_for_master_to_terminate(self, kill_after=60, timeout=120):
        logger.debug(
            "Waiting for condor_master (pid {}) for {} to terminate".format(
                self.condor_master.pid, self
            )
        )

        start = time.time()
        killed = False
        while True:
            try:
                self.condor_master.communicate(timeout=5)
                break
            except TimeoutError:
                pass

            elapsed = time.time() - start

            if not killed:
                logger.debug(
                    "condor_master has not terminated yet, will kill in {} seconds".format(
                        int(kill_after - elapsed)
                    )
                )

            if elapsed > kill_after and not killed:
                # TODO: in this path, we should also kill the other daemons
                # TODO: we can find their pids by reading the master log
                self._kill_condor_master()
                killed = True

            if elapsed > timeout:
                raise TimeoutError(
                    "Timed out while waiting for condor_master to terminate"
                )

        logger.debug(
            "condor_master (pid {}) has terminated with exit code {}".format(
                self.condor_master.pid, self.condor_master.returncode
            )
        )

    def read_config(self):
        return self.config_file.read_text()

    def run_command(self, *args, **kwargs):
        """
        Execute a command with ``CONDOR_CONFIG`` set to point to this HTCondor pool.
        Arguments and keyword arguments are passed through to :func:`~run_command`.
        """
        with self.use_config():
            return run_command(*args, **kwargs)

    @property
    def master_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's master."""
        return self._get_log_path("MASTER")

    @property
    def collector_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's collector."""
        return self._get_log_path("COLLECTOR")

    @property
    def negotiator_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's negotiator."""
        return self._get_log_path("NEGOTIATOR")

    @property
    def schedd_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's schedd."""
        return self._get_log_path("SCHEDD")

    @property
    def startd_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's startd."""
        return self._get_log_path("STARTD")

    @property
    def shadow_log(self) -> Path:
        """A :class:`DaemonLog` for the pool's shadows."""
        return self._get_log_path("SHADOW")

    @property
    def job_queue_log(self) -> Path:
        """The path to the pool's job queue log."""
        return self._get_log_path("JOB_QUEUE")

    @property
    def startd_address(self):
        """The address of the pool's startd."""
        return self._get_address_file("STARTD").read_text().splitlines()[0]

    def _get_log_path(self, subsystem):
        return self._get_path_from_condor_config_val("{}_LOG".format(subsystem))

    def _get_address_file(self, subsystem):
        return self._get_path_from_condor_config_val(
            "{}_ADDRESS_FILE".format(subsystem)
        )

    def _get_path_from_condor_config_val(self, attr):
        return Path(self.run_command(["condor_config_val", attr],).stdout)


class SetEnv:
    """
    A context manager. Inside the block, the Condor config file is the one given
    to the constructor. After the block, it is reset to whatever it was before
    the block was entered.

    If you need to change the ``CONDOR_CONFIG``, use the specialized
    :func:`SetCondorConfig`.
    """

    _was_not_set = object()

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get(self.key, self._was_not_set)
        set_env_var(self.key, self.value)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not self._was_not_set:
            set_env_var(self.key, self.previous_value)
        else:
            unset_env_var(self.key)


def set_env_var(key: str, value: str):
    os.environ[key] = value


def unset_env_var(key: str):
    value = os.environ.get(key, None)

    if value is not None:
        del os.environ[key]


class SetCondorConfig:
    """
    A context manager. Inside the block, the Condor config file is the one given
    to the constructor. After the block, it is reset to whatever it was before
    the block was entered.
    """

    def __init__(self, config_file: Path):
        self.config_file = Path(config_file)
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get("CONDOR_CONFIG", None)
        set_env_var("CONDOR_CONFIG", self.config_file.as_posix())

        htcondor.reload_config()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not None:
            set_env_var("CONDOR_CONFIG", self.previous_value)
        else:
            unset_env_var("CONDOR_CONFIG")

        htcondor.reload_config()


def run_command(
    args: List[str], stdin=None, timeout: int = 60, log: bool = True,
):
    """
    Execute a command.

    Parameters
    ----------
    args
    stdin
    timeout
    log

    Returns
    -------

    """
    if timeout is None:
        raise TypeError("run_command timeout cannot be None")

    args = list(map(str, args))
    p = subprocess.run(
        args,
        timeout=timeout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=stdin,
        universal_newlines=True,
    )
    p.stdout = p.stdout.rstrip()
    p.stderr = p.stderr.rstrip()

    msg_lines = [
        "Ran command: {}".format(" ".join(p.args)),
        "CONDOR_CONFIG = {}".format(os.environ.get("CONDOR_CONFIG", "<not set>")),
        "exit code: {}".format(p.returncode),
        "stdout:{}{}".format("\n" if "\n" in p.stdout else " ", p.stdout),
        "stderr:{}{}".format("\n" if "\n" in p.stderr else " ", p.stderr),
    ]
    msg = "\n".join(msg_lines)

    if log and p.returncode != 0:
        logger.debug(msg)

    return p
