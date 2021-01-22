import psycopg2
import sys

def getConn():
    connStr = "host='localhost' \
               dbname= 'postgres' user='postgres' password = '12345'"
    conn=psycopg2.connect(connStr)      
    return  conn

        
def writeOutput(output):
    with open("output.txt", "a") as myfile:
        myfile.write(output)
         
try:
    conn=None   
    conn=getConn()
    cur = conn.cursor()
    f = open("input.txt", "r")
    for x in f:
        try:
            if(x[0] == 'Z'): # Empty database tables
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = "DELETE FROM cancel; DELETE FROM ticket; DELETE FROM event; DELETE FROM spectator;"
                cur.execute(sql)
                conn.commit()
                print("Z. All tables cleared")
                writeOutput("\nZ. All tables cleared\n")
            elif(x[0] == 'B'): # Insert new event
                raw = x[2:-1]
                data = raw.split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = "INSERT INTO event VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')".format(data[0], data[1], data[2], data[3], data[4], data[5])
                cur.execute(sql)
                conn.commit()
                writeOutput("\nB. Insert event Successful\n")
                print("B. Insert event successful")
            elif(x[0] == 'A'): # Insert new spectator
                raw = x[2:-1]
                data = raw.split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = "INSERT INTO spectator VALUES ('{0}', '{1}', '{2}')".format(data[0], data[1], data[2])
                cur.execute(sql)
                conn.commit()
                writeOutput("\nA. Insert spectator successful\n")
                print("A. Insert spectator successful")        
            elif(x[0] == 'E'): # Issue ticket (ticket_restrict constraint (ecode + sno must be unique) on ticket table limits spectator to one ticket per event)
                raw = x[2:-1]
                data = raw.split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = "INSERT INTO ticket VALUES ('{0}', '{1}', '{2}')".format(data[0], data[1], data[2])
                cur.execute(sql)
                conn.commit()
                writeOutput("\nE. Issue ticket successful\n")
                print("E. Issue ticket successful")
            elif(x[0] == 'P'): # Travel query
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = """
                    SELECT e.edate, e.elocation, COUNT(t.tno) AS potentialspectators 
                    FROM event AS e 
                    LEFT JOIN ticket as t on e.ecode = t.ecode 
                    GROUP BY e.elocation, e.edate;"""
                cur.execute(sql)
                rows = cur.fetchall()   
                print('P. ')
                writeOutput('\nP.\n')
                for row in rows:
                    for item in row:
                        print (item, ", ", end='')
                        s = str(item) + ", "
                        writeOutput(s)
                    writeOutput('\n')
                    print('')
            elif(x[0] == 'Q'): # Total tickets query
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = """
                    SELECT event.edesc, event.ecode, COUNT(ticket.tno) AS TicketsIssued
                    FROM event 
                    LEFT JOIN ticket ON ticket.ecode = event.ecode
                    GROUP BY event.edesc, event.ecode
                    ORDER BY event.edesc;"""
                cur.execute(sql)
                rows = cur.fetchall() 
                print('Q. ')
                writeOutput('\nQ. \n')
                for row in rows:
                    for item in row:
                        print (item, ", ", end='')
                        s = str(item) + ", "
                        writeOutput(s)
                    writeOutput('\n')
                    print('')
            elif(x[0] == 'R'): # Total tickets for an event
                raw = x[2:-1]
                data = raw.split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = """
                    SELECT event.edesc, event.ecode, COUNT(DISTINCT ticket.tno) AS TicketsIssued
                    FROM ticket JOIN event 
                    ON ticket.ecode = event.ecode
                    WHERE event.ecode = '{0}'
                    GROUP BY event.edesc, event.ecode
                    ORDER BY event.edesc;""".format(data[0])
                cur.execute(sql)
                rows = cur.fetchall()
                print('R. ')
                writeOutput('\nR. \n')
                for row in rows:
                    for item in row:
                        print (item, ", ", end='')
                        s = str(item) + ", "
                        writeOutput(s)
                    writeOutput('\n')
                    print('')
            elif(x[0] == 'S'): # Itinerary for spectator
                    raw = x.split(" ")
                    data = raw[1].split(",")
                    cur.execute("SET SEARCH_PATH to pirean;")
                    sql = """
                        select spectator.sname, event.edate, event.elocation, event.etime, event.edesc
                        FROM spectator
                        JOIN ticket ON ticket.sno = spectator.sno
                        JOIN event ON event.ecode = ticket.ecode
                        WHERE spectator.sno = {0};""".format(data[0])
                    cur.execute(sql)
                    rows = cur.fetchall()  
                    print('S. ')
                    writeOutput('\nS. \n')
                    for row in rows:
                        for item in row:
                            print (item, ", ", end='')
                            s = str(item) + ", "
                            writeOutput(s)
                        writeOutput('\n')
                        print('')
            elif(x[0] == 'T'): # Ticket details query
                    raw = x.split(" ")
                    data = raw[1].split(",")
                    cur.execute("SET SEARCH_PATH to pirean;")
                    sql ="""
                        SELECT s.sname, t.ecode, CASE WHEN t.tno = c.tno THEN 'cancelled' ELSE 'active' END as status
                        FROM ticket as t
                        LEFT JOIN spectator as s on t.sno = s.sno 
                        LEFT join cancel as c on t.tno = c.tno
                        where t.tno = {0};""".format(data[0])
                    cur.execute(sql)
                    rows = cur.fetchall()  
                    print('T. ')
                    writeOutput('\nT. \n')
                    for row in rows:
                        for item in row:
                            print (item, ", ", end='')
                            s = str(item) + ", "
                            writeOutput(s)
                        writeOutput('\n')
                        print('')
            elif(x[0] == 'V'): # View cancelled tickets for event
                    raw = x[2:-1]
                    data = raw.split(",")
                    cur.execute("SET SEARCH_PATH to pirean;")
                    sql = "SELECT * FROM cancel WHERE ecode = '{0}'".format(data[0])
                    cur.execute(sql)
                    rows = cur.fetchall()
                    print('V. ')
                    writeOutput('\nV. \n')
                    for row in rows:
                        for item in row:
                            print (item, ", ", end='')
                            s = str(item) + ", "
                            writeOutput(s)
                        writeOutput('\n')
                        print('')
            elif(x[0] == 'D'): # Delete event
                raw = x[2:-1]
                data = raw.split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = """
                INSERT INTO cancel (tno, ecode, sno)
                SELECT tno, ecode, sno FROM ticket WHERE ecode = '{0}';
                DELETE FROM event WHERE ecode = '{0}';""".format(data[0])
                cur.execute(sql)
                conn.commit()
                print("D. Event deleted")
                writeOutput("\nD. Event deleted\n")
            elif(x[0] == 'C'): # Delete spectator
                raw = x.split(" ")
                data = raw[1].split(",")
                cur.execute("SET SEARCH_PATH to pirean;")
                sql = "DELETE FROM spectator WHERE sno = {0}".format(data[0])
                cur.execute(sql)
                conn.commit()
                print("C. Spectator deleted")
                writeOutput("\nC. Spectator deleted\n")
            elif(x[0] == 'X'): # End. 
                    writeOutput("\nX. Exit")  
                    conn.close()  
                    sys.exit("X. Application closing down")
        except Exception as e:
            print("")
            print (e)
            writeOutput("\n")
            writeOutput (str(e))
            conn.rollback()

except Exception as e:
    print (e)
    writeOutput (str(e))

               