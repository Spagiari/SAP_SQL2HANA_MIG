# MIG SQL 2 HANA
Tool for Z table migration SqlServer to Hana Database 

## Configuration file
src/config.json
```{
    "orig": {
        "User": "",
        "Pwd": "",
        "Server": "",
        "Database": "",
        "Schema": ""
    },
    "dest":{
        "UserSystemPrivileges": "SYSTEM",
        "PwdSystemPrivileges": "",
        "User": "",
        "Pwd": "",
        "Server": "",
        "Port": 31041,
        "Schema": ""
    }
}
```

## Usage

### Create from to script
```
    cd src
    $ python mig.py FROMTO -o TABLE_ORIGIN -d TABLE_DESTINATION
```

#### Example from to script

### Executing Scrip
```
    cd src
    $ python mig.py EXECUTE -s fromto_TABLE_ORIGIN-TABLE_DESTINATION.json
```
