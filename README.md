# MIG SQL 2 HANA
Tool for Z table migration SqlServer to Hana Database.
This tool make end run a customizable ETL script from origin Microsift SQL table to destination SAP Hana tables. 

## Configuration file
src/config.json
```
{
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
```
{
    "DestinationTable": "ZTFI00016",
    "FromTo": {
        "CONDICAO": {
            "ColumnDestination": "CONDICAO",
            "ColumnOrigin": "CONDICAO",
            "Transform": "'009' as CONDICAO"
        },
        "DT_CADASTRO": {
            "ColumnDestination": "DT_CADASTRO",
            "ColumnOrigin": "DT_CADASTRO",
            "Transform": "isnull(DT_CADASTRO, '') as DT_CADASTRO"
        },
        "KUNNR": {
            "ColumnDestination": "KUNNR",
            "ColumnOrigin": "KUNNR"
        },
        "MANDT": {
            "ColumnDestination": "MANDT",
            "ColumnOrigin": "MANDT"
        }
    },
    "Observations": [
        "CHECK ORIGIN ['USUARIO']",
        "CHECK DESTINATION: ['UNAME']"
    ],
    "OriginTable": "ZECCFIT0044",
    "Ready": true,
    "Where": "MANDT = 400"
}

```

### Executing Scrip
```
    cd src
    $ python mig.py EXECUTE -s fromto_TABLE_ORIGIN-TABLE_DESTINATION.json
```
