import warnings
import pymysql


# To connect DB from console:
# C:\Windows\System32> mysql -u root -p --host s.snu.ac.kr --user DB2015_12740 --port 3306 DB2015_12740
# To test:
# python main.py < test.txt > test_result.txt
def connect():
    global db
    try:
        db = pymysql.connect(
            host='s.snu.ac.kr',
            port=3306,
            user='DB2015_12740',
            passwd='0422',
            db='DB2015_12740',
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor)
    except:
        print("Cannot connect to DB. Closing System . . .")
        exit()


def welcome():
    # Welcome message
    print("============================================================\n\
1. print all buildings\n\
2. print all performances\n\
3. print all audiences\n\
4. insert a new building\n\
5. remove a building\n\
6. insert a new performance\n\
7. remove a performance\n\
8. insert a new audience\n\
9. remove an audience\n\
10. assign a performance to a building\n\
11. book a performance\n\
12. print all performances which assigned at a building\n\
13. print all audiences who booked for a performance\n\
14. print ticket booking status of a performance\n\
15. exit\n\
16. reset database\n\
============================================================")
    return

def struct():
    # Check basic schema and if not exist, build up new basic schema
    # Make 4 relations : Audience, Performance, Building, Book

    cursor = db.cursor()
    with warnings.catch_warnings():  # When tables already Exist, suppress warning message
        warnings.simplefilter('ignore')
        try:
            # Constructing Audience Table
            # Stores information of audience id(hidden), name, gender, age
            audience = '''
                CREATE TABLE IF NOT EXISTS  AUDIENCE (
                    id int UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name varchar(200) NOT NULL, 
                    gender char(1) NOT NULL,
                    age int NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            '''
            cursor.execute(audience)
            # Constructing Building Table
            # Stores information of Building id(hidden), name, location, capacity.
            building = '''
                CREATE TABLE IF NOT EXISTS  BUILDING (
                    id int UNSIGNED NOT NULL AUTO_INCREMENT,
                    name varchar(200) NOT NULL,
                    location varchar(200) NOT NULL,
                    capacity int UNSIGNED NOT NULL,
                    PRIMARY KEY(id, capacity)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            '''
            cursor.execute(building)
            # Constructing Performance Table
            # Stores performance id, name, genre, price information.
            # This table also has Performance to Building information. (building id, capacity columns)
            # When a building is deleted, corresponding columns will be set to null
            performance = '''
                CREATE TABLE IF NOT EXISTS PERFORMANCE ( 
                    id int UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name varchar(200) NOT NULL,
                    genre varchar(200) NOT NULL,
                    price int UNSIGNED NOT NULL,
                    buildingID int UNSIGNED,
                    capacity int UNSIGNED,
                    FOREIGN KEY (buildingID, capacity) REFERENCES BUILDING(id , capacity) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            '''
            cursor.execute(performance)
            # Construct book(reservation) table
            # Stores Audience to Performance information : audience id, performance id, seat number(only one)
            # When customer books multiple seat, each seat will be stored as one row.
            # When audience or performance or building deleted, the related row also deleted.
            book = '''
                CREATE TABLE IF NOT EXISTS BOOK (
                    aud_id int UNSIGNED NOT NULL,
                    per_id int UNSIGNED NOT NULL,
                    seat int UNSIGNED NOT NULL,
                    buildingID int UNSIGNED NOT NULL,
                    FOREIGN KEY (aud_id) references AUDIENCE(id) ON DELETE CASCADE ,
                    FOREIGN KEY (per_id) references PERFORMANCE(id) ON DELETE CASCADE ,
                    FOREIGN KEY (buildingID) references BUILDING(id) ON DELETE CASCADE ,
                    PRIMARY KEY (aud_id, per_id, seat, buildingID)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            '''
            cursor.execute(book)
            return True
        except Exception as e:
            #print(e)
            return False


def dispatch(n):
    # Get action number and return corresponding action(Succeed/Fail).
    if n == '1':
        return printB()
    elif n == '2':
        return printP()
    elif n == '3':
        return printA()
    elif n == '4':
        return insertB()
    elif n == '5':
        return removeB()
    elif n == '6':
        return insertP()
    elif n == '7':
        return removeP()
    elif n == '8':
        return insertA()
    elif n == '9':
        return removeA()
    elif n == '10':
        return setP2B()
    elif n == '11':
        return book()
    elif n == '12':
        return printP2B()
    elif n == '13':
        return printA2P()
    elif n == '14':
        return printPstat()
    elif n == '16':
        return clear()
    else:
        print("Invalid action")
        return False


def printB():  # 1 : Print All Buildings in Building Table
    cursor = db.cursor()
    # SQL for retrieving all row from building table
    sql = 'SELECT * FROM BUILDING'
    # SQL for finding assignment number of building
    sql2 = 'SELECT COUNT(*) FROM PERFORMANCE WHERE buildingid = %s'
    try:
        # Execute SQL
        cursor.execute(sql)
        result = cursor.fetchall()
        # Print result
        #   Print header
        strFormat = '%-10s%-35s%-15s%-10s%-10s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('id', 'name', 'location', 'capacity', 'assigned')
        strOut += '-'*80 + '\n'
        # When table is empty, return
        if cursor.rowcount == 0:
            print(strOut + "There is no building")
            return True
        # Else, get number of assignment and print each row
        for r in result:
            cursor.execute(sql2, r['id'])
            assigned = cursor.fetchone()['COUNT(*)']
            strOut += strFormat%(r['id'], r['name'], r['location'], r['capacity'], assigned)
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Building Failed.")
        #print(e)
        return False
    return True


def printP():  # 2 : Print All Performances in Performance Table
    cursor = db.cursor()
    # SQL for retrieving all row from performance table
    sql = 'SELECT * FROM PERFORMANCE'
    # SQL for getting number of distinct audiences who booked
    sql2 = 'SELECT COUNT(DISTINCT aud_id) as COUNT FROM BOOK WHERE per_id = %s'
    try:
        # Execute SQL
        cursor.execute(sql)
        result = cursor.fetchall()
        # Print result
        #   Print header
        strFormat = '%-10s%-35s%-15s%-10s%-10s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('id', 'name', 'type', 'price', 'booked')
        strOut += '-'*80 + '\n'
        # When table is empty, return
        if cursor.rowcount == 0:
            print(strOut + "There is no performance")
            return True
        # Else, get number of book and print each row
        for r in result:
            cursor.execute(sql2, r['id'])
            booked = cursor.fetchone()['COUNT']
            strOut += strFormat%(r['id'], r['name'], r['genre'], r['price'], booked)
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Performance Failed.")
        #print(e)
        return False
    return True


def printA():  # 3: Print All Audience in Audience Table
    cursor = db.cursor()
    # SQL for retrieving all row from audience table
    sql = 'SELECT * FROM AUDIENCE'
    try:
        # Execute SQL
        cursor.execute(sql)
        result = cursor.fetchall()
        # Print result
        #   Print header
        strFormat = '%-10s%-40s%-15s%-15s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('id', 'name', 'gender', 'age')
        strOut += '-'*80 + '\n'
        # When table is empty, return
        if cursor.rowcount == 0:
            print(strOut + "There is no audience")
            return True
        # Else, print each row
        for r in result:
            strOut += strFormat%(r['id'], r['name'], r['gender'], r['age'])
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Audience Failed.")
        print(e)
        return False
    return True


def insertB():  # 4: Insert one row to Building Table
    cursor = db.cursor()
    # Get input :Name, Location, Capacity
    print("Building name: ", end='')
    bname = input().strip()  # If user puts unnecessary blank character before or after, delete
    if len(bname) > 200:  # If input is too long, truncate
        bname = bname[:200]
    print("Building location: ", end='')
    loc = input().strip()
    if len(loc) > 200:
        loc = loc[:200]
    print("Building capacity: ", end='')
    cap = input().strip()
    if int(cap) <= 0:  # When capacity range invalid, return
        print("Capacity should be more than 0")
        return False

    # Execute SQL
    sql = 'INSERT INTO BUILDING (name, location, capacity) VALUES (%s, %s, %s)'
    try:
        cursor.execute(sql, (bname, loc, int(cap)))
        print("A building is successfully inserted")
    except Exception as e:
        print("Insert Building Failed")
        print(e)
        return False
    return True


def removeB():  # 5: Remove row from Building Table
    cursor = db.cursor()
    # SQL for deleting row
    sql = 'DELETE FROM BUILDING WHERE id = %s'
    # Get input
    print("Building ID: ", end='')
    bID = input().strip()
    try:
        cursor.execute(sql, int(bID))
        # If no row selected, ID is invalid.
        if cursor.rowcount == 0:
            print("Building ", bID, " doesn't exist")
            return False
        print("A building is successfully removed")
    except Exception as e:
        print('Remove Building Failed.')
        #print(e)
        return False
    return True


def insertP():  # 6: Insert one row to Performance Table
    cursor = db.cursor()
    # Get input : Name, Genre, Price
    print("Performance name: ", end='')  # Get name
    pname = input().strip()
    if len(pname) > 200:
        pname = pname[:200]

    print("Performance type: ", end='')  # Get type
    gen = input().strip()
    if len(gen) > 200:
        gen = gen[:200]

    print("Performance price: ", end='')  # Get price
    pri = input().strip()
    if int(pri) < 0:  # if price is out of range, return
        print("Price should be 0 or more")
        return False

    # Execute insert SQL
    sql = 'INSERT INTO PERFORMANCE (name, genre, price) VALUES (%s, %s, %s)'
    try:
        cursor.execute(sql, (pname, gen, int(pri)))
        print("A performance is successfully inserted")
    except Exception as e:
        print("Insert Performance Failed.")
        #print(e)
        return False
    return True


def removeP():  # 7: Remove from Performance Table
    cursor = db.cursor()
    # Get performance ID
    print("Performance ID: ", end='')
    pID = input().strip()

    # Make & Execute delete SQL
    sql = 'DELETE FROM PERFORMANCE WHERE id = %s'
    try:
        cursor.execute(sql, int(pID))
        # If no rows selected, (i.e. invalid performance id) return
        if cursor.rowcount == 0:
            print("Performance ", pID, " doesn't exist")
            return False
        print("A performance is successfully removed")
    except Exception as e:
        print("Remove Performance Failed.")
        #print(e)
        return False
    return True


def insertA():  # 8: insert one row to Audience Table
    cursor = db.cursor()
    # Get input : Name, Gender, Age
    print("Audience name: ", end='')  # Get name
    aname = input().strip()
    if len(aname) > 200:
        aname = aname[:200]

    print("Audience gender: ", end='')  # Get gender
    gen = input().strip()
    if gen not in ['M', 'F', 'm', 'f']:  # if gender input is invalid, return
        print("Gender should be 'M' or 'F'")
        return False

    print("Audience Age : ", end='')  # Get age
    age = input().strip()
    if int(age) <= 0:  # if age input out of range, return
        print("Age should be more than 0")
        return False

    # Make & Execute insert SQL
    sql = 'INSERT INTO AUDIENCE (name, gender, age) VALUES (%s, %s, %s)'
    try:
        cursor.execute(sql, (aname, gen, age))
        print("An audience is successfully inserted")
    except Exception as e:
        print("Insert Audience Failed.")
        #print(e)
        return False
    return True


def removeA():  # 9: Remove from Audience Table
    cursor = db.cursor()
    # Get Audience ID
    print("Audience ID: ", end='')
    aID = input().strip()
    # Make delete SQL
    sql = 'DELETE FROM AUDIENCE WHERE id = %s'
    # Execute delete SQL
    try:
        cursor.execute(sql, aID)
        # If no rows selected, (i.e. invalid audience id) return.
        if cursor.rowcount == 0:
            print("Audience ", aID, " doesn't exist")
            return False
        print("An audience is successfully removed")
    except Exception as e:
        print('Remove Audience Failed.')
        #print(e)
        return False
    return True


def setP2B():  # 10: set Performance to Building
    cursor = db.cursor()
    # Get Building ID & Performance ID
    print("Building ID: ", end='')
    bID = input().strip()
    print("Performance ID: ", end='')
    pID = input().strip()
    # SQL for getting capacity of Building ID
    sql = 'SELECT capacity FROM BUILDING WHERE id = %s'
    # SQL for inserting building assignment info into performance table's foreign key columns
    sql2 = 'UPDATE PERFORMANCE SET buildingID = %s , capacity = %s WHERE id = %s and buildingID is null'
    # SQL for checking if pID is valid
    sql3 = 'SELECT * FROM PERFORMANCE WHERE id = %s'
    try:
        # Get the capacity of building
        cursor.execute(sql, bID)
        if cursor.rowcount == 0:
            print("Building ", bID, " doesn't exist")
            return False
        cap = cursor.fetchone()['capacity']

        # Update 'buildingID, capacity' columns in performance table
        cursor.execute(sql2, (bID, cap, pID))
        if cursor.rowcount == 0:  # If nothing executed, check if performance ID is valid
            cursor.execute(sql3, pID)
            if cursor.rowcount == 0:  # If performance ID invalid, return
                print("Performance ", pID, " doesn't exist")
                return False
            else:  # If Building is already assigned to the performance, return
                print("Performance ", pID, " is already assigned")
                return False
        else:
            print("Successfully assign a performance")
    except Exception as e:
        print("Performance Setting Failed.")
        #print(e)
        return False
    return True


def book():  # 11: Book a reservation to performance
    cursor = db.cursor()
    # Get Input Performance ID, Audience ID, Seat list
    print("Performance ID: ", end='')
    pID = input().strip()
    print("Audience ID: ", end='')
    aID = input().strip()
    print("Seat number: ", end='')
    slist = input().strip().split(",")

    # SQLs for getting maximum seat number, price and age information
    sql = 'SELECT capacity, price, buildingID FROM PERFORMANCE WHERE id = %s'
    sql4 = 'SELECT age FROM AUDIENCE WHERE id = %s'
    # SQL for checking if the seat is already reserved
    sql2 = 'SELECT * FROM BOOK WHERE per_id = %s and seat = %s'
    # SQL for inserting information into book table
    sql3 = 'INSERT INTO BOOK (aud_id, per_id, seat, buildingID) VALUES (%s, %s, %s, %s)'
    try:
        # Get maximum seat number, price and age information
        cursor.execute(sql, pID)
        if cursor.rowcount == 0:  # If no result, (i.e. performance ID is invalid) return.
            print("Performance ", pID, " doesn't exist")
            return False
        info = cursor.fetchone()
        maxseat = info['capacity']
        price = info['price']
        bID = info['buildingID']
        if maxseat is None:  # When capacity field is null (i.e. Performance is not assigned)
            print("Performance ", pID, " isn't assigned")
            return False
        cursor.execute(sql4, aID)
        if cursor.rowcount == 0:  # If no result, (i.e. audience ID is invalid) return
            print("Audience ", aID, " doesn't exist")
            return False
        info = cursor.fetchone()
        age = info['age']

        # Check seat number range exception & seat already reserved exception
        for s in slist:
            if int(s.strip()) > maxseat or int(s.strip()) < 1:
                print("Seat number out of range")
                return False
            cursor.execute(sql2, (pID, s.strip()))
            if cursor.rowcount > 0:
                print("The seat is already taken")
                return False
        # If there's no exception, insert each seat to the book table
        for s in slist:
            cursor.execute(sql3, (aID, pID, s.strip(), bID))
        # Check age and print total price
        price = calprice(price, age, len(slist))
        print("Successfully book a performance")
        print("Total ticket price is", '{:,d}'.format(price))

    except Exception as e:
        print("Book Failed.")
        #print(e)
        return False
    return True


def calprice(price, age, n):  # support method of no.11(book)
    # Calculate total price of ticket according to audience age
    if age <= 7:
        return 0
    elif age <= 12:
        return round(price * 0.5 * n)
    elif age <= 18:
        return round(price * 0.8 * n)
    else:
        return round(price * n)


def printP2B():  # 12: Print all performances assigned to certain building
    cursor = db.cursor()
    # Get Building ID
    print("Building ID: ", end='')
    bID = input().strip()
    # SQL for getting number of book of performances
    sql = 'SELECT COUNT(*) FROM BOOK WHERE per_id =%s'
    # SQL for selecting all performances assigned to building ID
    sql3 = 'SELECT id, name, genre, price FROM PERFORMANCE WHERE buildingID = %s'
    # SQL for checking the building ID is valid
    sql2 = 'SELECT * FROM BUILDING WHERE id = %s'
    try:
        # Execute checkSQL
        cursor.execute(sql2, bID)
        # If no rows are selected, invalid building ID
        if cursor.rowcount == 0:
            print("Building ", bID, " doesn't exist")
            return False
        # Select all performances assigned to building ID
        cursor.execute(sql3, bID)
        result = cursor.fetchall()
        #   Print header
        strFormat = '%-10s%-35s%-15s%-10s%-10s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('id', 'name', 'type', 'price', 'booked')
        strOut += '-'*80 + '\n'
        # When table is empty, return
        if cursor.rowcount == 0:
            print(strOut + "There is no performance")
            return True
        # Else, get booked number and print each row
        for r in result:
            cursor.execute(sql, r['id'])
            booked = cursor.fetchone()['COUNT(*)']
            strOut += strFormat % (r['id'], r['name'], r['genre'], r['price'], booked)
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Performance in the Building Failed")
        #print(e)
        return False
    return True


def printA2P():  # 13: Print all audiences who booked for certain performance
    cursor = db.cursor()
    # Get input
    print("Performance ID: ", end='')
    pID = int(input().strip())
    # Make SQL for selecting rows
    sql = '''SELECT A.id as id, A.name as name, A.gender as gender, A.age as age
             FROM AUDIENCE as A, BOOK as B 
             WHERE A.id = B.aud_id and B.per_id = %s
             GROUP BY (A.id)
    '''
    # Make SQL for checking performance id valid
    sql2 = 'SELECT * FROM PERFORMANCE WHERE id = %s'
    try:
        # Execute check SQL
        cursor.execute(sql2, pID)
        if cursor.rowcount == 0:
            print("Performance ", pID, " doesn't exist")
            return False
        # Execute select SQL
        cursor.execute(sql, pID)
        result = cursor.fetchall()
        # Print result
        #   Print header
        strFormat = '%-10s%-40s%-15s%-15s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('id', 'name', 'gender', 'age')
        strOut += '-'*80 + '\n'
        # When table is empty, return
        if cursor.rowcount == 0:
            print(strOut + "There is no audience")
            return True
        # Else, print each row
        for r in result:
            strOut += strFormat%(r['id'], r['name'], r['gender'], r['age'])
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Audience of the Performance Failed")
        print(e)
    return


def printPstat():  # 14: print all seat number & Audience ID(if applicabe) of a performance
    cursor = db.cursor()
    # Get the performance ID
    print("Performance ID: ", end='')
    pID = int(input().strip())
    # SQL for getting the list of booked seat
    sql = 'SELECT seat, aud_id FROM BOOK WHERE per_id = %s'
    # SQL for checking valid performance id & getting max seat number
    sql2 = 'SELECT capacity FROM PERFORMANCE WHERE id = %s'
    try:
        # Execute check SQL & get capacity value
        cursor.execute(sql2, pID)
        if cursor.rowcount == 0:  # If performance ID invalid, return
            print("Performance ", pID, " doesn't exist")
            return False
        maxseat = cursor.fetchone()['capacity']
        if maxseat is None:  # If performance doesn't have building info, return
            print("Performance ", pID, " isn't assigned")
            return False
        # Execute SQL
        cursor.execute(sql, pID)
        result = cursor.fetchall()
        # Print result
        #   Print header
        strFormat = '%-40s%-40s\n'
        strOut = '-'*80 + '\n'
        strOut += strFormat % ('seat_number', 'audience_id')
        strOut += '-'*80 + '\n'
        #   Iterate maxseat number and print corresponding information if exist
        for i in range(1, maxseat+1):
            noresult = True  # set flag
            for r in result:
                if i == r['seat']:
                    strOut += strFormat % (r['seat'], r['aud_id'])
                    noresult = False
                    break
            if noresult:  # If the seat has no booking information, print blank
                strOut += strFormat % (i, '')
        strOut += '-'*80
        print(strOut)
    except Exception as e:
        print("Printing Status Failed")
        #print(e)
        return False
    return True


def clear():  # 16: Clear all table and rows in DB and reset schema
    # Confirm Clearance
    print("Do you want to reset Database? [y/n]")
    ans = input()
    if ans == 'n':
        print("Canceling . . .")
        return
    elif ans != 'y':
        print("Invalid input! Back to prompt . . .")
        return
    # Drop DB, Create DB, Build schema
    cursor = db.cursor()
    sql = ['DROP DATABASE IF EXISTS DB2015_12740', 'CREATE DATABASE DB2015_12740']
    try:
        for s in sql:
            cursor.execute(s)
        connect()  # re-connect to new DB
        struct()
        db.commit()
    except Exception as e:
        print("Clearing DB Failed.")
        print(e)
        return False
    return True


def main():
    # Connect to DB
    connect()
    # Make up basic table
    struct()
    db.commit()
    # Print selection menu
    welcome()
    # Execute action until user wants to exit
    while True:
        # Print prompt message & get input
        print("Select your action: ", end='')
        n = input()
        # If selection is exit, exit.
        if n == '15':
            break
        # else, execute the action
        else:
            if dispatch(n):
                db.commit()
            print()
    db.commit()
    db.close()
    print("Bye!")
    return


main()
