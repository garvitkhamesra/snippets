import sys
import MySQLdb
import csv

def main(host, user, passwd, db, table, csvfile):

    try:
        conn = getconn(host, user, db, passwd)
    except MySQLdb.Error as e:
        print ("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    cursor = conn.cursor()

    loadcsv(conn, cursor, table, csvfile)

    cursor.close()
    conn.close()

def getconn(host, user, db, passwd):
    print (host + "," + user + "," + passwd + "," + db)
    conn = MySQLdb.connect(host = host,
                           user = user,
                           passwd = passwd,
                           db = db)
    return conn

def nullify(L):
    def f(x):
        if(x == ""):
            return None
        else:
            return x

    return [f(x) for x in L]

def loadcsv(conn, cursor, table, filename):
    f = csv.reader(open(filename))

    header = next(f)
    numfields = len(header)

    query = buildInsertCmd(table, numfields)

    print ("query -> " + query)
    for line in f:
        vals = nullify(line)
        print (vals)
        try:
            cursor.execute(query, vals)
        except MySQLdb.Error as e:
            print ("Error %d: %s" % (e.args[0], e.args[1]))
            sys.exit (1)
        conn.commit()
    return

def buildInsertCmd(table, numfields):
    assert(numfields > 0)
    placeholders = (numfields-1) * "%s, " + "%s"
    query = ("insert into %s" % table) + (" values (%s)" % placeholders)
    return query

if __name__ == '__main__':
    # commandline execution

    args = sys.argv[1:]
    if(len(args) < 4):
        print ("error: arguments: host user password db table csvfile")
        sys.exit(1)

    main(*args)