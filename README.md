# SaltStack sapcontrol extension
This SaltStack extensions allows interfacing sapcontrol running on minions.

**THIS PROJECT IS NOT ASSOCIATED WITH SAP IN ANY WAY**

## Installation
Run the following to install the SaltStack sapcontrol extension:
```bash
salt-call pip.install saltext.sap-control
```
Keep in mind that this package must be installed on every minion that should utilize the states and execution modules.

Alternatively, you can add this repository directly over gitfs
```yaml
gitfs_remotes:
  - https://github.com/SAPUCC/saltext-sap_control.git:
    - root: src/saltext/sap_control
```
In order to enable this, logical links under `src/saltext/sap_control/` from `_<dir_type>` (where the code lives) to `<dir_type>` have been placed, e.g. `_modules` -> `modules`. This will double the source data during build, but:
 * `_modules` is required for integrating the repo over gitfs
 * `modules` is required for the salt loader to find the modules / states

## Usage
A state using the sapcontrol extension looks like this:
```jinja
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
```

## Docs
See https://saltext-sap-control.readthedocs.io/ for the documentation.

## Contributing
We would love to see your contribution to this project. Please refer to `CONTRIBUTING.md` for further details.

## License
This project is licensed under GPLv3. See `LICENSE.md` for the license text and `COPYRIGHT.md` for the general copyright notice.
