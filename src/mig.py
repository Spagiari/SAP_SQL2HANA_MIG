import pyodbc as db
import pyhdb
import json
import argparse

SQLCharTypes = ['CHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'NTEXT', 'BINARY',
                'VARBINARY', 'VARBINARY']
SQLNumbTypes = ['BIT', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'DECIMAL',
                'DEC', 'NUMERIC', 'FLOAT', 'REAL', 'SMALLMONEY', 'MONEY']
SQLDateTypes = ['DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'TIME',
                'DATETIMEOFFSET']
SQLBlobTypes = ['VARCHAR', 'TEXT', 'NVARCHAR', 'VARBINARY', 'IMAGE']

HanaCharTypes = ['VARCHAR', 'NVARCHAR', 'ALPHANUM', 'SHORTTEXT', 'VARBINARY']
HanaNumbTypes = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'SMALLDECIMAL',
                 'DECIMAL', 'REAL', 'DOUBLE']
HanaDateTypes = ['DATE', 'TIME', 'SECONDDATE', 'TIMESTAMP', 'TIME']
HanaBlobTypes = ['BLOB', 'CLOB', 'NCLOB', 'TEXT']

class mig:
    def __init__(self):
        #Create connection string to connect DBTest database with windows authentication
        self.oColumnMetaData = dict()
        self.dColumnMetaData = dict()

        try:
            with open('config.json', 'r') as fin:
                content = fin.read()
        except IOError:
            print("Error opening file")
            return

        try:
            cfg = json.loads(content)
            self.orig = cfg['orig']
            self.dest = cfg['dest']
        except:
            print("Invalid config.json format")
            return

        self.con = db.connect('Driver={4};' \
                              'Server={0}; Database={1};' \
                              'Uid={2};Pwd={3};'.format(self.orig['Server'],
                                                        self.orig['Database'],
                                                        self.orig['User'],
                                                        self.orig['Pwd'],
                                                       '{ODBC Driver 13 for SQL Server}'))

        self.hconn = pyhdb.connect(
            host=self.dest['Server'],
            port=self.dest['Port'],
            user=self.dest['UserSystemPrivileges'],
            password=self.dest['PwdSystemPrivileges'])

    def getColumns(self, tablename):
        cur = self.con.cursor()
        qry = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " \
                "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE " \
                "FROM {0}.INFORMATION_SCHEMA.COLUMNS " \
                "WHERE TABLE_NAME = N'{1}'".format(self.orig['Database'],
                                                   tablename)
        #print(qry)
        cur.execute(qry)
        row = cur.fetchone() #Fetch first row

        if (not row):
            print("Origin table not found")
            return False

        col = []

        while row: #Fetch all rows using a while loop
            col.append(row[0])
            self.oColumnMetaData[row[0]] = dict()
            self.oColumnMetaData[row[0]]['Type'] = row[1].upper()
            if self.oColumnMetaData[row[0]]['Type'] in SQLCharTypes:
                self.oColumnMetaData[row[0]]['Length'] = row[2]
            elif self.oColumnMetaData[row[0]]['Type'] in SQLNumbTypes:
                self.oColumnMetaData[row[0]]['Length'] = row[3]
                self.oColumnMetaData[row[0]]['Scale'] = row[4]
            elif self.oColumnMetaData[row[0]]['Type'] in SQLDateTypes:
                raise ValueError('Migration of datetime types not implemented')
            else:
                print ('Unexpected data type {0}'.format(row[1]))
                raise ValueError('Unexpected data type')
            self.oColumnMetaData[row[0]]['IsNullable'] = True \
                    if row[5] == 'YES' else False
            if self.oColumnMetaData[row[0]]['Type'] in SQLBlobTypes and \
               self.oColumnMetaData[row[0]]['Length'] == -1:
                self.oColumnMetaData[row[0]]['IsBlob'] = True
            else:
                self.oColumnMetaData[row[0]]['IsBlob'] = False

            row = cur.fetchone()
        cur.close() #Close the cursor and connection objects
        return col

    def hGetColumns(self, tablename):
        cur = self.hconn.cursor()
        qry = "SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE "\
                "FROM SYS.COLUMNS "\
                "WHERE SCHEMA_NAME = '{0}' "\
                "and TABLE_NAME = '{1}'".format(self.dest['Schema'],
                                                tablename)
        cur.execute(qry)
        row = cur.fetchone()

        if (not row):
            print("Destination table not found")
            return False

        col = []

        while row:
            col.append(row[0])
            self.dColumnMetaData[row[0]] = dict()
            self.dColumnMetaData[row[0]]['Type'] = row[1].upper()
            self.dColumnMetaData[row[0]]['IsBlob'] = False
            if self.dColumnMetaData[row[0]]['Type'] in HanaCharTypes:
                self.dColumnMetaData[row[0]]['Length'] = row[2]
            elif self.dColumnMetaData[row[0]]['Type'] in HanaBlobTypes:
                self.dColumnMetaData[row[0]]['Length'] = row[2]
                self.dColumnMetaData[row[0]]['IsBlob'] = True
            elif self.dColumnMetaData[row[0]]['Type'] in HanaNumbTypes:
                self.dColumnMetaData[row[0]]['Length'] = row[2]
                self.dColumnMetaData[row[0]]['Scale'] = row[3]
            elif self.dColumnMetaData[row[0]]['Type'] in HanaDateTypes:
                raise ValueError('Migration of datetime types not implemented')
            else:
                print('Unexpected data type {0}'.format(row[1]))
                raise ValueError('Unexpected data type')
            self.dColumnMetaData[row[0]]['IsNullable'] = True \
                    if row[4] == 'X' else False
            row = cur.fetchone()

        return col

    def getIntersection(self, scol, hcol):
        lst3 = [value for value in scol if value in hcol]
        return lst3

    def getDiferences(self, scol, hcol):
        snh = [value for value in scol if value not in hcol]
        hns = [value for value in hcol if value not in scol]
        return snh, hns

    def wrongtype(self, c):
            return "Wrong types destiny "\
                    "{0}({1}), origin" \
                    "{2}({3})".format(self.dColumnMetaData[c]['Type'],
                                      self.dColumnMetaData[c]['Length'],
                                      self.oColumnMetaData[c]['Type'],
                                      self.oColumnMetaData[c]['Length'])
    def wronglengthstr(self, c):
            return "Wrong lenth destiny "\
                    "{0}({1}), origin" \
                    "{2}({3})".format(self.dColumnMetaData[c]['Type'],
                                      self.dColumnMetaData[c]['Length'],
                                      self.oColumnMetaData[c]['Type'],
                                      self.oColumnMetaData[c]['Length'])
    def wronglengthnum(self, c):
            return "Wrong lenth possible data lost destiny "\
                    "{0}({1},{2}), origin" \
                    "{3}({4},{5})".format(self.dColumnMetaData[c]['Type'],
                                          self.dColumnMetaData[c]['Length'],
                                          self.dColumnMetaData[c]['Scale'],
                                          self.oColumnMetaData[c]['Type'],
                                          self.oColumnMetaData[c]['Length'],
                                          self.oColumnMetaData[c]['Scale'])


    def makecopyscript(self, stable, htable):
        if stable[0].upper() != 'Z' or htable[0].upper() != 'Z':
            print('Ambas as tabelas precisam ser Z')
            return

        script = dict()
        script['Ready'] = True
        script['Observations'] = list()
        script['FromTo'] = dict()
        script['OriginTable'] = stable
        script['DestinationTable'] = htable
        script['Where'] = 'MANDT = 400'

        ocol = self.getColumns(stable)
        hcol = self.hGetColumns(htable)

        if not ocol or not hcol:
            return

        icol = self.getIntersection(ocol, hcol)
        if len(icol) == len(ocol):
            print("-- Perfeito! todas as colunas do ECC tem referência no Hana.")
        else:
            print("-- OPS! ATENÇÃO algumas colunas precisam ser verificadas")
            snh, hns = self.getDiferences(ocol, hcol)
            snhf = list()
            hnsf = list()

            for s in snh:
                snhf.append("{0} {1}({2}{3}{4})".format(s,
                                                    self.oColumnMetaData[s]['Type'],
                                                    self.oColumnMetaData[s]['Length'],
                                                    ', ' if 'Scale' in
                                                        self.oColumnMetaData[s]
                                                        else '',
                                                    self.oColumnMetaData[s].get('Scale',
                                                                               '')))
            for h in hns:
                hnsf.append("{0} {1}({2}{3}{4})".format(h,
                                                    self.dColumnMetaData[h]['Type'],
                                                    self.dColumnMetaData[h]['Length'],
                                                    ', ' if 'Scale' in
                                                        self.dColumnMetaData[h]
                                                        else '',
                                                    self.dColumnMetaData[h].get('Scale',
                                                                               '')))

            script['Observations'].append("VERIFIQUE no ECC: {0}".format(snhf))
            script['Observations'].append("VERIFIQUE no HANA: {0}".format(hnsf))
            script['Ready'] = False

        if len(icol) == 0:
            script['Observations'].append("NADA pode ser feito")
            return

        #check icol data types
        for c in icol:
            script['FromTo'][c] = dict()
            script['FromTo'][c]['ColumnOrigin'] = c
            script['FromTo'][c]['ColumnDestination'] = c
            script['FromTo'][c]['Notation'] = list()

            if self.dColumnMetaData[c]['Type'] in HanaCharTypes:
                if self.oColumnMetaData[c]['Type'] in SQLCharTypes:
                    if self.dColumnMetaData[c]['Length'] >= \
                       self.oColumnMetaData[c]['Length'] and \
                       not self.oColumnMetaData[c]['IsBlob']:
                        if self.dColumnMetaData[c]['IsNullable'] or \
                           (self.dColumnMetaData[c]['IsNullable'] == \
                            self.oColumnMetaData[c]['IsNullable']):
                            script['FromTo'][c].pop('Notation')
                        else:
                            script['FromTo'][c].pop('Notation')
                            script['FromTo'][c]['Transform'] = \
                                    "isnull({0}, '') as {0}".format(c)
                    else:
                        script['FromTo'][c]['Notation'].append(self.wronglengthstr(c))
                elif self.oColumnMetaData[c]['Type'] in SQLNumbTypes:
                    script['FromTo'][c]['Notation'].append(self.wrongtype(c))
                    script['FromTo'][c]['Notation'].append("Check Transformation script")
                    script['FromTo'][c]['Transform'] = \
                            "cast(isnull({0}, '0') as varchar) as {0}".format(c)
                else:
                    script['FromTo'][c]['Notation'].append(self.wrongtype(c))
                    script['Ready'] = False

            elif self.dColumnMetaData[c]['Type'] in HanaNumbTypes:
                if self.oColumnMetaData[c]['Type'] in SQLNumbTypes:
                    if self.dColumnMetaData[c]['Length'] >= \
                       self.oColumnMetaData[c]['Length']:
                        if self.dColumnMetaData[c]['IsNullable'] or \
                           (self.dColumnMetaData[c]['IsNullable'] == \
                            self.oColumnMetaData[c]['IsNullable']):
                            script['FromTo'][c].pop('Notation')
                        else:
                            script['FromTo'][c].pop('Notation')
                            script['FromTo'][c]['Transform'] = \
                                    "isnull({0}, 0) as {0}".format(c)
                    else:
                        script['FromTo'][c]['Notation'].append(self.wronglengthstr(c))
                        script['Ready'] = False
                else:
                    script['FromTo'][c]['Notation'].append(self.wrongtype(c))
                    script['Ready'] = False

            elif self.dColumnMetaData[c]['Type'] in HanaBlobTypes:
                if self.oColumnMetaData[c]['IsBlob'] or \
                   self.oColumnMetaData[c]['Type'] in SQLCharTypes:
                    if self.dColumnMetaData[c]['IsNullable'] or \
                       (self.dColumnMetaData[c]['IsNullable'] == \
                        self.oColumnMetaData[c]['IsNullable']):
                        script['FromTo'][c]['IsBlob'] = True
                        script['FromTo'][c].pop('Notation')
                    else:
                        script['FromTo'][c].pop('Notation')
                        script['FromTo'][c]['Transform'] = \
                                "isnull({0}, '') as {0}".format(c)
                else:
                    script['FromTo'][c]['Notation'].append(self.wrongtype(c))
                    script['Ready'] = False

        content = json.dumps(script, indent=4, sort_keys=True)
        with open('fromto_{0}-{1}.json'.format(stable, htable), 'w') as out:
            out.write(content)

    def mig(self, cfg_file, mandt):
        try:
            with open(cfg_file, 'r') as fin:
                content = fin.read()
        except IOError:
            print("Error opening file")
            return

        try:
            cfg = json.loads(content)
        except:
            print("Invalid format")
            return

        if not cfg.get('Ready', False):
            print ('Script not ready')
            return

        ocol = []
        dcol = []
        col = []
        hp = []
        hseq = 1

        for i in cfg['FromTo']:
            col.append(i)
            hp.append(':{0}'.format(str(hseq)))
            ocol.append("[{0}]".format(cfg['FromTo'][i]['ColumnOrigin']))
            dcol.append(cfg['FromTo'][i]['ColumnDestination'])
            hseq += 1

        fields = ",".join(ocol)

        for i in cfg['FromTo']:
            if 'Transform' in cfg['FromTo'][i]:
                fields = fields.replace('[{0}]'.format(cfg['FromTo'][i]['ColumnOrigin']),
                               cfg['FromTo'][i]['Transform'])

        where = ''
        if 'Where' in cfg:
            where = " WHERE {0}".format(cfg['Where'])

        qry = "SELECT {0} FROM {1}.{2} {3}".format(fields, self.orig['Schema'],
                                                   cfg['OriginTable'], where)


        hfields = ",".join(dcol)
        hvalues = ",".join(hp)

        hqry = "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(self.dest['Schema'],
                                                               cfg['DestinationTable'],
                                                               hfields,
                                                               hvalues)
        #print (hqry)
        #print (qry)

        hconn = pyhdb.connect(
            host=self.dest['Server'],
            port=self.dest['Port'],
            user=self.dest['User'],
            password=self.dest['Pwd'])

        hcur = hconn.cursor()

        cur = self.con.cursor()
        cur.execute(qry)
        row = cur.fetchone() #Fetch first row
        toInsert = list()
        affrows = 0

        while row:
            ivalues = []
            i=0
            for c in col:
                if cfg['FromTo'][c].get('IsBlob', False):
                    ivalues.append(pyhdb.NClob(row[i]))
                else:
                    if c == 'MANDT' and mandt:
                        ivalues.append(mandt)
                    else:
                        ivalues.append(row[i])
                i+=1

            toInsert.append(ivalues)
            row = cur.fetchone()

            if (len(toInsert) == 10000):
                hcur.executemany(hqry, toInsert)
                affrows += len(toInsert)
                print('Pushed {0} rows!'.format(affrows))
                toInsert = list()

        if (len(toInsert) > 0):
            hcur.executemany(hqry, toInsert)
            affrows += len(toInsert)
            print('Pushed {0} rows!'.format(affrows))
            toInsert = list()

        print('Commiting')
        hconn.commit()
        print('Done!')
        print('Sucess')
        cur.close()
        hcur.close()
        hconn.close()


def main():
    args = parser()
    m = mig()

    if args.Option == 'FROMTO' and args.origin and args.destination:
        m.makecopyscript(args.origin,args.destination)
    elif args.Option == 'EXECUTE' and args.script:
        m.mig(args.script, args.force_mandt)

    #m = mig()
    #m.makecopyscript("ZTBFI_FOLLOW_COB","ZTFI00038")
    #print (m.dColumnMetaData)
    #print (m.oColumnMetaData)
    #m.mig("fromto_ZTBFI_FOLLOW_COB-ZTFI00038.json", True)

def parser():
    parser = argparse.ArgumentParser(description="""Migration toolbox \n
                                     copy data from SQL Server from Hana""")

    parser.add_argument("Option", help="""FROMTO: Make from to migration
                        script. \n\tUse with -o and -d
                        EXECUTE: Execute migration script. \n\tUse with -s""")

    parser.add_argument('-o', '--origin',
                        action="store", dest="origin",
                        default= None,
                        help= "Table name on origin server")
    parser.add_argument('-d', '--destination',
                        action="store", dest="destination",
                        default= None,
                        help= "Table name on destiny server")
    parser.add_argument('-s', '--script',
                        action="store", dest="script",
                        default= None,
                        help= "Script file path")
    parser.add_argument('-f', '--force',
                        action="store", dest="force_mandt",
                        default= None,
                        help= "Force mandant number")

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    main()
