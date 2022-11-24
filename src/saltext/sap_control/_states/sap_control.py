"""
SaltStack extension for sapcontrol
Copyright (C) 2022 SAP UCC Magdeburg

sapcontrol state module
=======================
SaltStack module that implements states based on sapcontrol functionality.

:codeauthor:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       N/A
:platform:      Linux

This module implements states that utilize sapcontrol functionality.

.. note::
    This module can only run on linux platforms.
"""
import logging
import os
import re
import time
from datetime import datetime as dt


# Globals
log = logging.getLogger(__name__)

# the following list contains syslog errors that are to be ignored.
NON_CRITICAL_SYSLOG_ERRORS = [
    'Monitoring: Program RSUSR003 Reports "Security check passed" ',
]

__virtualname__ = "sap_control"


def __virtual__():
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


# pylint: disable=unused-argument
def running(name, instance, username, password, restart=False, **kwargs):
    """
    Ensure that sapcontrol is started for an SID / instance.

    name
        The SID for which sapcontrol should be running.

    instance
        The instance for which sapcontrol should be running.

    username
        User with which to run all operations.

    password
        Passwort for the user.

    restart
        Boolean if sapcontrol should be restarted if it is already running, defualt is ``False``.

    Example:

    .. code-block:: jinja

        sapcontrol for S4H / instance 00 is running:
          sap_control.running:
            - name: S4H
            - instance: '00'
            - username: sapadm
            - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")

    .. note::
        This should not be used. Instead, a proper systemd service should be created that handles sapcontrol.
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": "",
    }

    result = __salt__["sap_control.status"](
        instance_number=instance, username=username, password=password
    )
    if not isinstance(result, bool):
        msg = f"Cannot retrieve status for sapcontrol / instance {instance}"
        log.error(msg)
        ret["comment"] = msg
        return ret
    if result:
        log.debug("sapcontrol is running")
        if not restart:
            ret["comment"] = "sapcontrol is already running"
        else:
            log.debug("Restarting sapcontrol")
            if __opts__["test"]:
                ret["comment"] = "sapcontrol would have been restarted"
                ret["changes"] = {
                    "old": f"sapcontrol for {name} / {instance} was running",
                    "new": f"sapcontrol for {name} / {instance} would have been restarted",
                }
            else:
                result = __salt__["sap_control.restart"](
                    sid=name, instance_number=instance, username=username, password=password
                )
                if not isinstance(result, bool) or not result:
                    log.error(f"Cannot start sapcontrol for {name} / {instance} was running")
                    ret["comment"] = f"Cannot start sapcontrol {name} / {instance}"
                    ret["result"] = False
                    return ret
                else:
                    ret["comment"] = "sapcontrol was restarted"
                    ret["changes"] = {
                        "old": f"sapcontrol for {name} / {instance} was running",
                        "new": f"sapcontrol for {name} / {instance} was restarted",
                    }
    else:
        log.debug("sapcontrol is not running, starting")
        if __opts__["test"]:
            ret["comment"] = "sapcontrol would have been started"
            ret["changes"] = {
                "old": f"sapcontrol for {name} / {instance} was not running",
                "new": f"sapcontrol for {name} / {instance} would have been started",
            }
        else:
            result = __salt__["sap_control.start"](
                sid=name, instance_number=instance, username=username, password=password
            )
            if not isinstance(result, bool) or not result:
                log.error(f"Cannot start sapcontrol for {name} / {instance}")
                ret["comment"] = f"Cannot start sapcontrol {name} / {instance}"
                return ret
            log.debug("sapcontrol was started")
            ret["comment"] = "sapcontrol was started"
            ret["changes"] = {
                "old": f"sapcontrol for {name} / {instance} was not running",
                "new": f"sapcontrol for {name} / {instance} was started",
            }
    ret["result"] = True if (not __opts__["test"] or ret["changes"]) else None
    return ret


# pylint: disable=unused-argument
def dead(name, instance, username, password, **kwargs):
    """
    Ensure that sapcontrol is stopped for an SID / instance.

    name
        The SID for which sapcontrol should be stopped.

    instance
        The instance for which sapcontrol should be stopped.

    username
        User with which to run all operations.

    password
        Passwort for the user.

    Example:

    .. code-block:: jinja

        sapcontrol for S4h / instance 00 is stopped:
          sap_control.dead:
            - name: S4H
            - instance: '00'
            - username: sapadm
            - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")

    .. note::
        This should not be used. Instead, a proper systemd service should be created that handles sapcontrol.
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": "",
    }

    result = __salt__["sap_control.status"](
        instance_number=instance, username=username, password=password
    )
    if not isinstance(result, bool):
        msg = f"Cannot retrieve status for sapcontrol / instance {instance}"
        log.error(msg)
        ret["comment"] = msg
        return ret
    if not result:
        log.debug("sapcontrol is already stopped")
        ret["comment"] = "sapcontrol is already stopped"
        ret["result"] = True
        return ret

    log.debug("sapcontrol is running, stopping")
    if __opts__["test"]:
        ret["comment"] = "sapcontrol would have been stopped"
        ret["changes"] = {
            "old": f"sapcontrol for {name} / {instance} is running",
            "new": f"sapcontrol for {name} / {instance} would have been stopped",
        }
    else:
        result = __salt__["sap_control.stop"](
            instance_number=instance, username=username, password=password
        )
        if not isinstance(result, bool) or not result:
            log.error("Cannot stop sapcontrol")
            ret["comment"] = "Cannot stop sapcontrol"
            return ret

        log.debug("sapcontrol was stopped")
        ret["comment"] = "sapcontrol was stopped"
        ret["changes"] = {
            "old": f"sapcontrol for {name} / {instance} was running",
            "new": f"sapcontrol for {name} / {instance} is not running",
        }

    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None

    return ret


def sld_registered(
    name,
    sid,
    instance_number,
    username,
    password,
    sld_user,
    sld_password,
    sld_host,
    sld_port,
    log_files=None,
    remove_logs=True,
    overwrite=False,
    sld_check_timeout=60,
    **kwargs,
):
    """
    Ensure that a sapcontrol instance is registered at an SLD / LMDB. If log files are defined (see argument
    ``log_files``), then each file will be checked for a correct HTTP return code.

    name
        Target slddest.cfg file.

    sid
        SID of the system.

    instance_number
        Instance number for which the SLD registration should take place.

    username
        Username for the sapcontrol connection.

    password
        Password for the sapcontrol connection.

    sld_user
        SLD connection username.

    sld_password
        SLD connection password.

    sld_host
        SLD connection fqdn.

    sld_port
        SLD connection port.

    log_files
        List of log files to check for success (full path).

    remove_logs
        Remove the logs before restarting the service. Default is ``True``.

    overwrite
        Configuration will not be checked but overwritten. Default is ``False``.

    sld_check_timeout
        How long the system will wait for a positive HTTP return code from the SLD in the defined logs.
        Default is ``60``.

    .. warning::
        In order to trigger the data transfer, sapcontrol will be restarted!

    .. note::
        No password check will be performed if all other configuration parameters fit.
        To circumvent this, set overwrite=True.

    Example:

    .. code-block:: jinja

        SLD is configured and data is transfered for S4H / 00:
          sap_control.sld_registered:
            - name: /usr/sap/S4H/SYS/global/slddest.cfg
            - sid: S4H
            - instance_number: '00'
            - username: s4hadm
            - password: __slot__:salt:vault.read_secret(path="os", key="s4hadm")
            - sld_user: SLD_DS_USER
            - sld_password: __slot__:salt:vault.read_secret(path="sld", key="SLD_DS_USER")
            - sld_host: sol.my.domain
            - sld_port: 50000
            - log_files:
              - /usr/sap/S4H/D00/work/dev_sldregs
              - /usr/sap/S4h/D00/work/dev_sldregk
              - /usr/sap/S4H/D00/work/dev_krnlreg
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": "",
    }
    if not log_files:
        log_files = []

    sldreg_bin = _which("sldreg", runas=username)
    if not sldreg_bin:
        msg = f"Could not determine path of sldreg for user {username}"
        log.error(msg)
        ret["comment"] = msg
        ret["result"] = False
        return ret

    sldreg_dir = os.path.dirname(sldreg_bin)

    log.debug("Checking for existing config")
    update_cfg = True
    if __salt__["file.file_exists"](name):
        log.debug("Getting existing config")
        cmd = " ".join([sldreg_bin, "-showconnect", name])
        result = __salt__["cmd.run_all"](
            cmd=cmd, runas=username, env={"LD_LIBRARY_PATH": sldreg_dir}
        )
        if result["retcode"]:
            return False
        log.debug("Parse output")
        existing_config = {}
        for line in result["stdout"]:
            for param in ["host_param", "https_param", "port_param", "user_param"]:
                if param in line:
                    line_idx = line.find(param)
                    key, value = line[line_idx:].split("=", 1)
                    existing_config[key] = value.strip("'")
        if (
            sld_user == existing_config.get("user_param", None)
            and sld_host == existing_config.get("host_param", None)
            and sld_port == existing_config.get("port_param", None)
            and "y" == existing_config.get("https_param", None)
        ):
            update_cfg = False

    if not update_cfg and not overwrite:
        ret["comment"] = "No changes required"
        ret["result"] = True
        return ret

    log.debug("Updating configuration")
    if __opts__["test"]:
        ret["changes"]["config"] = f"Configuration {name} would have been updated"
    else:
        cmd = " ".join(
            [
                sldreg_bin,
                "-configure",
                name,
                "-usekeyfile",
                "-noninteractive",
                "-user",
                sld_user,
                "-pass",
                sld_password,
                "-host",
                sld_host,
                "-port",
                str(sld_port),
                "-usehttps",
            ]
        )
        result = __salt__["cmd.run_all"](
            cmd=cmd, runas=username, env={"LD_LIBRARY_PATH": sldreg_dir}
        )
        if result["retcode"]:
            msg = "Could not update configuration"
            log.error(msg)
            ret["comment"] = msg
            ret["result"] = False
            return ret
        ret["changes"]["config"] = f"Configuration {name} updated"

    if remove_logs:
        log.debug("Removing log files")
        for log_file in log_files:
            result = __salt__["file.remove"](log_file)
            if result:
                if "log_files" not in ret["changes"]:
                    ret["changes"]["log_files"] = []
                ret["changes"]["log_files"].append(f"Removed {log_file}")

    log.debug("Restarting sapcontrol to trigger SLD data transfer")
    if __opts__["test"]:
        ret["changes"]["sapcontrol"] = "Would have been restarted"
    else:
        result = __salt__["sap_control.restart"](sid, instance_number, username, password)
        if not result:
            ret["comment"] = "Could not restart sapcontrol"
            ret["result"] = False
            return ret
        ret["changes"]["sapcontrol"] = "Restarted"

    all_success = True
    if not __opts__["test"]:
        if log_files:
            log.debug("Checking log files for success")
            # wait max. n seconds for the registration to happen
            timeout = time.time() + sld_check_timeout
            re_rc = re.compile(r"Return code: ([0-9]{3})")
            while time.time() < timeout:
                all_success = True
                for log_file in log_files:
                    log.debug(f"Checking {log_file}")
                    try:
                        log_file_data = __salt__["file.read"](log_file)
                    except FileNotFoundError:
                        log.debug(f"{log_file} does not (yet?) exist")
                        all_success = False
                        break
                    return_codes = re_rc.findall(log_file_data)
                    log.debug(f"Got result from checkup: {return_codes}")
                    if not return_codes or int(return_codes[-1]) != 200:
                        all_success = False
                if all_success:
                    break
                time.sleep(0.5)

    if all_success:
        if log_files:
            if __opts__["test"]:
                ret["comment"] = "SLD registration and data transfer would have been successful"
            else:
                ret["comment"] = "SLD registration and data transfer successful"
        else:
            if __opts__["test"]:
                ret["comment"] = "SLD registration would have been successful"
            else:
                ret["comment"] = "SLD registration successful"
        ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None
    else:
        ret["comment"] = "SLD data transfer not successful"
        ret["result"] = False
    return ret


# pylint: disable=unused-argument
def system_health_ok(name, check_from, instance_number, username, password, **kwargs):
    """
    This state checks the system health by looking for Critical Syslog Entries and
    Work Process Errors. If errors are present in the system, the state will return
    ``False`` as result.

    name
        SID of the SAP system.

    check_from
        Date from which on the system health should be checked (e.g. for log entries)
        in the format 31129999 or 01012000.

    instance_number
        Instance number for which syslog errors should be retrieved.

    username
        Username for the sapcontrol connection.

    password
        Password for the sapcontrol connection.

    .. note::
        This state does not implement ``__opts__["test"]`` since no data is changed.

    Example:

    .. code-block:: jinja

        System healh is OK for SAP NetWeaver AS ABAP system S4H (SM50 / SM21):
          sap_control.system_health_ok:
            - name: 'S4H'
            - check_from: {{ None | strftime("%d%m%Y") }}  {# renders to current date, e.g. 31082002 #}
            - instance_number: '00'
            - username: s4hadm
            - password: __slot__:salt:vault.read_secret(path="os", key="s4hadm")
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": [],
    }
    from_datetime = dt.strptime(f"{check_from}000000", "%d%m%Y%H%M%S")

    log.debug("Checking system log")
    syslog_errors = __salt__["sap_control.get_syslog_errors"](
        timestamp_from=from_datetime,
        instance_number=instance_number,
        username=username,
        password=password,
    )
    processed_errors = []
    if syslog_errors:
        log.error("Syslog errors:")
        for err in syslog_errors:
            # skip non-critical errors
            if err.Text in NON_CRITICAL_SYSLOG_ERRORS:
                continue
            log.error(err)
            processed_errors.append(re.sub(" +", " ", f"SM21: {err.Text}"))
    ret["comment"] += list(set(processed_errors))

    log.debug("Checking for work process errors")
    wp_table = __salt__["sap_control.get_workprocess_table"](
        instance_number=instance_number,
        username=username,
        password=password,
    )
    if not isinstance(wp_table, list):
        msg = "Cannot retrieve workprocess table"
        log.error(msg)
        ret["comment"].append(msg)
    for wproc in wp_table:
        if wproc.Status == "Ended" or wproc.Err:
            reason = f" (reason: {wproc.Reason})" if wproc.Reason else ""
            msg = (
                f"SM50: {wproc.Typ} work process {wproc.No} (PID: {wproc.Pid}) is in "
                f"status {wproc.Status}{reason} with error '{wproc.Err}'"
            )
            log.error(msg)
            ret["comment"].append(msg)

    if ret["comment"]:
        ret["result"] = False
    else:
        ret["comment"] = "System health OK"
        ret["result"] = True
    return ret
