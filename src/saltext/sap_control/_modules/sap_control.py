"""
SaltStack extension for sapcontrol
Copyright (C) 2022 SAP UCC Magdeburg

sapcontrol execution module
===========================
SaltStack execution module that wraps sapcontrol functions.

:codeauthor:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       zeep, requests
:platform:      Linux

This module wraps different functions of the sapcontrol by calling the corresponding SOAP services.
For controlling the state of the sapcontrol, you **should** create a custom systemd service and
use the service module.

By default, the functions will try to connect to the SAP Host Agent over HTTPS on port 5##14 and can
optionally fall back to HTTP communication on port 5##13.

.. note::
    Because functions are called over SOAP, only authenticated requests are accepted.

Currently, only basic authentication (username/password) is implemented.

.. note::
    This module was only tested on linux platforms.
"""
import logging
import time
from datetime import datetime as dt

import salt.utils.http
import salt.utils.path
import salt.utils.platform

# Third Party libs
ZEEPLIB = True
try:
    from zeep import Client
    from zeep.transports import Transport
    from zeep.exceptions import Fault
except ImportError:
    ZEEPLIB = None
REQUESTSLIB = True
try:
    from requests.exceptions import SSLError
    from requests.auth import HTTPBasicAuth
    from requests import Session
except ImportError:
    REQUESTSLIB = None

# Globals
log = logging.getLogger(__name__)
logging.getLogger("zeep").setLevel(logging.WARNING)  # data from here is not really required

SAPCONTROL_GRAY = 1
SAPCONTROL_GREEN = 2
SAPCONTROL_YELLOW = 3
SAPCONTROL_RED = 4

SAPCONTROL_FALLBACK_PATH = "/usr/sap/hostctrl/exe/sapcontrol"

__virtualname__ = "sap_control"


def __virtual__():
    """
    Only load this module if all libraries are available. Only work on POSIX-like systems.
    """
    if not ZEEPLIB:
        return False, "Could not load sap_control module, zeep unavailable"
    if not REQUESTSLIB:
        return False, "Could not load sap_control module, requests unavailable"
    if salt.utils.platform.is_windows():
        return False, "This module doesn't work on Windows."
    return __virtualname__


def _which(executable, runas=None):
    """
    Similar to salt.utils.path.which(), but:
     - Only works on Linux
     - Allows runas

    If not runas is given, the salt minion user is used
    """
    ret = __salt__["cmd.run_all"](cmd=f"which {executable}", runas=runas)
    if ret["retcode"]:
        return None
    return ret["stdout"]


def _get_client(instance_number, username, password, fallback=True, fqdn=None, timeout=300):
    """
    Creates and returns a SOAP client.

    This is **not** identical to sap_hostctrl._get_client()
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    session = Session()
    session.verify = salt.utils.http.get_ca_bundle()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session, timeout=timeout, operation_timeout=timeout)
    url = f"https://{fqdn}:5{instance_number}14/?wsdl"
    log.debug(f"Retrieving services from {url}")
    client = None
    try:
        client = Client(url, transport=transport)
    except SSLError as ssl_ex:
        log.debug(f"Got an exception:\n{ssl_ex}")
        if "certificate verify failed" in ssl_ex.__str__():
            log.error(f"Could not verify SSL certificate of {fqdn}")
        else:
            log.error(f"Cannot setup connection to sapcontrol on {fqdn}")
        client = False
    except Exception as exc:  # pylint: disable=broad-except
        log.debug(f"Got an exception:\n{exc}")
        log.error(f"Cannot setup connection to sapcontrol on {fqdn}")

    if fallback and not client:
        log.warning("HTTPS connection failed, trying  over an unsecure HTTP connection!")
        session.verify = False
        url = f"http://{fqdn}:5{instance_number}13/?wsdl"
        try:
            client = Client(url, transport=transport)
        except Exception as exc:  # pylint: disable=broad-except
            # possible exceptions unclear / undocumented
            log.debug(f"Got an exception:\n{exc}")
            log.error(f"Cannot setup connection to sapcontrol on {fqdn}")
            return False

    return client


# pylint: disable=too-many-leading-hastag-for-block-comment
### CONTROL ########################################################################################################


# pylint: disable=unused-argument
def status(instance_number, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Retrieve the current status of sapcontrol.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.status instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        log.debug(f"sapcontrol not running on {fqdn}")
        return False
    log.debug(f"sapcontrol running on {fqdn}")
    return True


# pylint: disable=unused-argument
def start(sid, instance_number, username, password, timeout=60, **kwargs):
    """
    Starts sapcontrol for a given SID and instance number.

    sid
        SID of the SAP system for which sapcontrol should be started.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    timeout
        Timeout for sapcontrol to start. Default is ``60``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.start sid="S4H" instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug("running function")
    sapcontrol_path = _which("sapcontrol", runas=username)
    if not sapcontrol_path:
        sapcontrol_path = _which(SAPCONTROL_FALLBACK_PATH, runas=username)
        if not sapcontrol_path:
            log.error(f"User {username} does not have access to any sapcontrol executables")
            return False

    cmd = f"{sapcontrol_path} -nr {instance_number} -function StartService {sid}"
    log.debug(f"Running '{cmd}' as user {username}")
    cmd_ret = __salt__["cmd.run_all"](cmd, python_shell=True, timeout=timeout, runas=username)
    log.debug(f"Result: {cmd_ret}")
    if cmd_ret.get("retcode"):
        out = cmd_ret.get("stderr").strip()
        log.error(f"Could not start sapcontrol:\n{out}")
        return False

    running = False
    now = time.time()
    while time.time() < now + timeout:
        if status(instance_number, username, password):
            running = True
            break
        time.sleep(1)

    return running


# pylint: disable=unused-argument
def stop(instance_number, username, timeout=60, **kwargs):
    """
    Stops sapcontrol for a given instance number.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    timeout
        Timeout for sapcontrol to stop. Default is ``60``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.stop sid="S4H" instance_number="00" username="sapadm"
    """
    log.debug("running function")
    sapcontrol_path = _which("sapcontrol", runas=username)
    if not sapcontrol_path:
        sapcontrol_path = _which(SAPCONTROL_FALLBACK_PATH, runas=username)
        if not sapcontrol_path:
            log.error(f"User {username} does not have access to any sapcontrol executables")
            return False

    cmd = f"{sapcontrol_path} -nr {instance_number} -function StopService"
    log.debug(f"Running '{cmd}' as user {username}")
    cmd_ret = __salt__["cmd.run_all"](cmd, python_shell=True, timeout=timeout, runas=username)
    log.debug(f"Result: {cmd_ret}")
    if cmd_ret.get("retcode"):
        out = cmd_ret.get("stderr").strip()
        log.error(f"Could not stop sapcontrol:\n{out}")
        return False
    return True


# pylint: disable=unused-argument
def restart(sid, instance_number, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Restarts sapcontrol for a given SID and instance number.

    sid
        SID of the SAP system for which sapcontrol should be stopped.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.restart sid="S4H" instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")

    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        log.debug("sapcontrol is not running, starting")
        return start(sid, instance_number, username, password)
    else:
        log.debug("sapcontrol is running, restarting")
        result = client.service.RestartService()
        if result:
            log.error(f"Could not restart sapcontrol:\n{result}")
            return False
        else:
            return True


# pylint: disable=too-many-leading-hastag-for-block-comment
### FUNCTIONS ########################################################################################################


def instance_status(instance_number, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Retrieves the status of an SAP instance based on the instance number.

    Returns one of the following status:
        SAPCONTROL_GRAY     = 1     => instance stopped
        SAPCONTROL_GREEN    = 2     => instance running
        SAPCONTROL_YELLOW   = 3     => instance starting / stopping
        SAPCONTROL_RED      = 4     => instance error

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.instance_status instance_number="00" username="sapadm" password="Abcd1234"
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug(f"Running function for instance {instance_number} on {fqdn} with fallback={fallback}")
    tgt_domain = "." + fqdn.split(".", 1)[1]

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return SAPCONTROL_RED

    log.debug("Retrieving all instances")
    instances = client.service.GetSystemInstanceList()
    for instance in instances:
        hostname = instance["hostname"] + tgt_domain
        if hostname != fqdn:
            continue
        inst_number = format(instance["instanceNr"], "02")
        if inst_number != instance_number:
            continue
        if instance["dispstatus"] == "SAPControl-GREEN":
            log.debug(f"Instance {instance_number} on {fqdn} is running")
            return SAPCONTROL_GREEN
        elif instance["dispstatus"] == "SAPControl-YELLOW":
            log.debug(f"Instance {instance_number} on {fqdn} is starting / stopping")
            return SAPCONTROL_YELLOW
        elif instance["dispstatus"] == "SAPControl-RED":
            log.debug(f"Instance {instance_number} on {fqdn} is error")
            return SAPCONTROL_RED
        elif instance["dispstatus"] == "SAPControl-GRAY":
            log.debug(f"instance_status:Instance {instance_number} on {fqdn} is stopped")
            return SAPCONTROL_GRAY
        else:
            log.error(
                f"Unknown status {instance['dispstatus']} for instance {instance_number} on {fqdn}"
            )
            raise Exception(
                f"Unknown status {instance['dispstatus']} for instance {instance_number} on {fqdn}"
            )

    log.warning(f"Cannot determine status of instance {instance_number} on {fqdn}")
    return SAPCONTROL_RED


def instance_start(
    instance_number, username, password, fallback=True, fqdn=None, timeout=300, **kwargs
):
    """
    Starts an SAP instance based on the instance number.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    timeout
        Timeout for the instance to start. Default is ``300``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.instance_start instance_number="00" username="sapadm" password="Abcd1234"
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug(
        f"Running function for instance {instance_number:02d} on {fqdn} with fallback={fallback}"
    )
    tgt_host = fqdn.split(".", 1)[0]

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return SAPCONTROL_RED

    log.debug(f"Starting instance {instance_number} on {fqdn}")
    client.service.InstanceStart(host=tgt_host, nr=int(instance_number))

    log.debug(f"Waiting for status == Running up to {timeout} seconds")
    start_timeout = time.time() + timeout
    while True:
        log.debug(f"Checking instance status for {instance_number}")
        inst_status = instance_status(
            instance_number=instance_number,
            fqdn=fqdn,
            username=username,
            password=password,
            fallback=fallback,
        )
        if inst_status == SAPCONTROL_RED:
            log.error(f"Cannot determine status of instance {instance_number}")
        elif inst_status == SAPCONTROL_GREEN:
            log.debug(f"Instance {instance_number} is running, exiting")
            break
        elif inst_status == SAPCONTROL_YELLOW:
            log.debug(f"Instance {instance_number} is starting")
        elif inst_status == SAPCONTROL_GRAY:
            log.debug(f"Instance {instance_number} is still stopped")
        else:
            log.error(f"Unknown instance status {inst_status}")
            raise Exception(f"Unknown instance status {inst_status}")
        if time.time() > start_timeout:
            log.error(
                f"Could not start instance {instance_number}, timeout of {timeout} seconds reached"
            )
            return False
        time.sleep(1)

    return True


def instance_stop(
    instance_number, username, password, fallback=True, fqdn=None, timeout=300, **kwargs
):
    """
    Stops an SAP instance based on the instance number.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    timeout
        Timeout for the instance to stop. Default is ``300``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.instance_stop instance_number="00" username="sapadm" password="Abcd1234"
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug(
        f"Running function for instance {instance_number:02d} on {fqdn} with fallback={fallback}"
    )
    tgt_host = fqdn.split(".", 1)[0]

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return SAPCONTROL_RED

    log.debug(f"Stopping instance {instance_number} on {fqdn}")
    # this will only return something on error
    result = client.service.InstanceStop(host=tgt_host, nr=int(instance_number), softtimeout=300)
    if result:
        log.error(f"Something went wrong:\n{result}")
        return False

    log.debug(f"Waiting for status == Stopped up to {timeout} seconds")
    stop_timeout = time.time() + timeout
    while True:
        log.debug(f"Checking instance status for {instance_number}")
        inst_status = instance_status(
            instance_number=instance_number,
            username=username,
            password=password,
            fqdn=fqdn,
            fallback=fallback,
        )
        if inst_status == SAPCONTROL_RED:
            log.error(f"Cannot determine status of instance {instance_number}")
        elif inst_status == SAPCONTROL_GREEN:
            log.debug(f"Instance {instance_number} is still running")
        elif inst_status == SAPCONTROL_YELLOW:
            log.debug(f"Instance {instance_number} is stopping")
        elif inst_status == SAPCONTROL_GRAY:
            log.debug(f"Instance {instance_number} is stopped, exiting")
            break
        else:
            log.error(f"Unknown instance status {inst_status}")
            raise Exception(f"Unknown instance status {inst_status}")
        if time.time() > stop_timeout:
            log.error(
                f"Could not stop instance {instance_number}, timeout of {timeout} seconds reached"
            )
            return False
        time.sleep(1)

    return True


def system_start(
    instance_number,
    username,
    password,
    level="ALL",
    fallback=True,
    fqdn=None,
    timeout=300,
    **kwargs,
):
    """
    Starts an SAP system with a certain level.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    level
        Configuration of the system to start, can be on of: ``ALL|SCS|DIALOG|ABAP|J2EE|TREX|ENQREP|HDB|ALLNOHDB``.
        Default is ``ALL``.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    timeout
        Timeout for the system to start. Default is ``300``.

    .. note ::
        There is no implementation of WaitForStarted as a sapcontrol webservice.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.system_start instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Calling StartSystem on instance {instance_number} on {fqdn}")
    result = client.service.StartSystem(
        options=f"SAPControl-{level}-INSTANCES", waittimeout=timeout
    )
    # on success, the function doesn't return anything, on error / timeout the error is returned
    if not result:
        return True
    else:
        log.error(f"Could not start {instance_number}:\n{result}")
        return False


def system_stop(
    instance_number,
    username,
    password,
    level="ALL",
    fallback=True,
    fqdn=None,
    timeout=300,
    **kwargs,
):
    """
    Stops an SAP system with a certain level.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    level
        Configuration of the system to stop, can be on of: ``ALL|SCS|DIALOG|ABAP|J2EE|TREX|ENQREP|HDB|ALLNOHDB``.
        Default is ``ALL``.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    timeout
        Timeout for the system to stop. Default is ``300``.

    .. note ::
        There is no implementation of WaitForStarted as a sapcontrol webservice.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.system_stop instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Calling StopSystem on instance {instance_number} on {fqdn}")
    # the function will only return something on error / timeout
    result = client.service.StopSystem(
        options=f"SAPControl-{level}-INSTANCES", waittimeout=timeout, softtimeout=timeout
    )
    if result:
        log.error(f"Could not stop {instance_number}:\n{result}")
        return False
    return True


def get_system_instance_list(
    instance_number, username, password, fallback=True, fqdn=None, timeout=300, **kwargs
):
    """
    Retrieve a list of system instances on the host.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    timeout
        Timeout to retrieve the list of system instances. Default is ``300``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_system_instance_list instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Retrieving list of instances for instance {instance_number} on {fqdn}")
    result = client.service.GetSystemInstanceList(timeout=timeout)
    if not result:
        log.error(f"Something went wrong:\n{result}")
        return False
    ret = []
    for instance in result:
        ret.append(
            {
                "hostname": instance["hostname"],
                "instance": instance["instanceNr"],
                "start_priority": float(instance["startPriority"]),
                "features": instance["features"].split("|"),
            }
        )
    return ret


def get_instance_properties(
    instance_number, username, password, fallback=True, fqdn=None, **kwargs
):
    """
    Retrieve the properties for an SAP instance.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_instance_properties instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Retrieving properties of instance {instance_number} on {fqdn}")
    result = client.service.GetInstanceProperties()
    if not result:
        log.error(f"Something went wrong:\n{result}")
        return False
    ret = {}
    for prop in result:
        ret[prop["property"]] = prop["value"]
    return ret


def parameter_value(
    instance_number, parameter, username, password, fallback=True, fqdn=None, **kwargs
):
    """
    Retrieve a parameter value from an SAP instance. Will return ``(Success, Data)``,
    e.g. ``(<True|False>, <some_value>)``.

    instance_number
        Instance number for the sapcontrol instance.

    parameter
        Parameter name to retrieve.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.parameter_value instance_number="00" parameter="icm/host_name_full" username="sapadm" password="Abcd1234"
    """  # pylint: disable=line-too-long
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False, None

    log.debug(f"Retrieving parameter {parameter} of instance {instance_number} on {fqdn}")
    result = None
    try:
        result = client.service.ParameterValue(parameter=parameter)
    except Fault as fault:
        if fault.message == "Invalid parameter":
            log.warning(f"Parameter {parameter} does not exist")
            return True, None
        if fault.message == "Permission denied":
            log.warning(f"Cannot access parameter {parameter}, permission denied")
            return False, None
        else:
            raise
    if not result:
        log.error(f"Something went wrong:\n{result}")
        return False, None
    log.debug(f"Got Result:\n{result}")
    return True, result


def get_abap_component_list(
    instance_number, username, password, fallback=True, fqdn=None, **kwargs
):
    """
    Retrieve a list of ABAP components of a system.

    .. note::
        This of only works for SAP NetWeaver AS ABAP instances.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_abap_component_list instance_number="00" username="sapadm" password="Abcd1234"
    """
    log.debug(f"Running function for instance {instance_number} on {fqdn}")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False, None

    log.debug(f"Retrieving ABAP components of instance {instance_number} on {fqdn}")
    result = None
    try:
        result = client.service.ABAPGetComponentList()
    except Fault as fault:
        if fault.message == "DpAttachStartService failed":
            log.warning(f"Instance {instance_number} is not running or is not of type DIALOG")
            return False, None
    if not result:
        log.error(f"Something went wrong:\n{result}")
        return False, None
    log.trace(f"Got result:\n{result}")
    data = {}
    for comp in result:
        data[comp["component"]] = {
            "version": comp["release"],
            "support_packages": comp["patchlevel"],
            "patch_level": "N/A",
            "vendor": "N/A",
            "type": comp["componenttype"],
            "description": comp["description"],
        }
    return True, data


def process_status(
    instance_number, process_name, username, password, fallback=True, fqdn=None, **kwargs
):
    """
    Retrieves the status of a process of an SAP instance.

    Returns one of the following status:
        SAPCONTROL_GRAY     = 1     => process stopped
        SAPCONTROL_GREEN    = 2     => process running
        SAPCONTROL_YELLOW   = 3     => process starting / stopping
        SAPCONTROL_RED      = 4     => process error

    instance_number
        Instance number for the sapcontrol instance.

    process_name
        Name of the process for which the status should be retrieved.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.process_status instance_number="00" process_name="webdisp" username="sapadm" password="Abcd1234"
    """  # pylint: disable=line-too-long
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug(
        f"Running function for process {process_name} of instance {instance_number} on {fqdn} with fallback={fallback}"
    )

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return SAPCONTROL_RED

    log.debug(f"Retrieving all processes of instance {instance_number}")
    processes = client.service.GetProcessList()
    for process in processes:
        if process["name"] != process_name:
            continue
        if process["dispstatus"] == "SAPControl-GREEN":
            log.debug(f"Process {process_name} of instance {instance_number} on {fqdn} is running")
            return SAPCONTROL_GREEN
        elif process["dispstatus"] == "SAPControl-YELLOW":
            log.debug(
                f"Process {process_name} of instance {instance_number} on {fqdn} is starting / stopping"
            )
            return SAPCONTROL_YELLOW
        elif process["dispstatus"] == "SAPControl-RED":
            log.debug(f"Process {process_name} of instance {instance_number} on {fqdn} is error")
            return SAPCONTROL_RED
        elif process["dispstatus"] == "SAPControl-GRAY":
            log.debug(f"Process {process_name} of instance {instance_number} on {fqdn} is stopped")
            return SAPCONTROL_GRAY
        else:
            msg = (
                f"Unknown status {process['dispstatus']} for process {process_name} "
                f"of instance {instance_number} on {fqdn}"
            )
            log.error(msg)
            raise Exception(msg)

    log.warning(
        f"Cannot determine status of process {process_name} of instance {instance_number} on {fqdn}"
    )
    return SAPCONTROL_RED


def get_pid(
    instance_number,
    process_name,
    username,
    password,
    fallback=True,
    fqdn=None,
    timeout=300,
    **kwargs,
):
    """
    Retrieves the PID of an Process of an SAP instance.

    instance_number
        Instance number for the sapcontrol instance.

    process_name
        Name of the process for which the pid should be retrieved.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_pid instance_number="00" process_name="webdisp" username="sapadm" password="Abcd1234"
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug(
        f"Running function for process {process_name} of instance {instance_number} on {fqdn} with fallback={fallback}"
    )

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Retrieving all processes of instance {instance_number}")
    processes = client.service.GetProcessList()
    for process in processes:
        if process["name"] == process_name:
            log.debug(f"PID of process {process_name} is {process['pid']}")
            return process["pid"]
        else:
            continue

    log.warning(
        f"Cannot determine the PID of process {process_name} of instance {instance_number} on {fqdn}"
    )
    return False


# pylint: disable=dangerous-default-value
def get_syslog_errors(
    timestamp_from,
    instance_number,
    username,
    password,
    severities=["SAPControl-RED"],
    fallback=True,
    fqdn=None,
    **kwargs,
):
    """
    Retrieves syslog entries for the system.

    .. note::
        This of only works for SAP NetWeaver AS ABAP instances.

    timestamp_from
        Timestamp from which entries should be retrieved. Must be a datetime object or a string in the format
        ``%Y-%m-%d %H:%M:%S``.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    severities
        List of severities for which entries should be retrieved. By default, this list only contains ``SAPControl-RED``

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_syslog_errors timestamp_from="2022-12-31 14:59:38" instance_number="00" username="sapadm" password="Abcd1234"
    """  # pylint: disable=line-too-long
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug("Running function")

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    if isinstance(timestamp_from, str):
        timestamp_from = dt.strptime(timestamp_from, "%Y-%m-%d %H:%M:%S")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Retrieving all processes of instance {instance_number}")
    syslog = client.service.ABAPReadSyslog()
    relevant_syslog = []
    for entry in syslog:
        syslog_timestamp = dt.strptime(entry.Time, "%Y %m %d %H:%M:%S")
        if syslog_timestamp > timestamp_from and entry.Severity in severities:
            relevant_syslog.append(entry)
    log.trace(f"Retrieved the following relevant syslog entries: {relevant_syslog}")
    return relevant_syslog


# pylint: disable=dangerous-default-value
def get_workprocess_table(instance_number, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Retrieves the current workprocess table for a given instance.

    .. note::
        This of only works for SAP NetWeaver AS ABAP instances.

    instance_number
        Instance number for the sapcontrol instance.

    username
        Username to use for connecting to sapcontrol.

    password
        Password to use for connecting to sapcontrol.

    fallback
        If set to ``True``, a HTTP connection will be opened in case of HTTPS connection failures.
        Default is ``True``.

    fqdn
        The fully qualified domain name on which the sapcontrol instance is running.
        If none is given, the FQDN of the current host is used. Default is ``None``.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_control.get_workprocess_table instance_number="00" username="sapadm" password="Abcd1234"
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]
    log.debug("Running function")

    if isinstance(instance_number, int):
        instance_number = format(instance_number, "02")

    log.debug("Setting up connection to sapcontrol")
    client = _get_client(
        instance_number=instance_number,
        username=username,
        password=password,
        fqdn=fqdn,
        fallback=fallback,
    )
    if not client:
        return False

    log.debug(f"Retrieving all processes of instance {instance_number}")
    return client.service.ABAPGetSystemWPTable() or []
