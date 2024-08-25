import csv
import sqlite3


def databaseing(theproducts, theshipments) -> None:
    con = sqlite3.connect("shipment_database.db")
    cur = con.cursor()

    # DEBUGGING INFO

    # cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # print(cur.fetchall())

    # a = con.execute("PRAGMA table_info('shipment')")

    # for b in a:
    #    print(b)

    # THIS CODE IS WHAT INSERTS THE TABLES INTO THE DATABASE
    # SINCE WE RAN THESE TO TEST INSERTING, WE CAN'T RUN THEM AGAIN OR
    # WE TRIGGER ERRORS FROM INSERTING THE SAME DATA TWICE.

    cur.executemany("INSERT INTO product VALUES(?, ?)", theproducts)
    con.commit()
    cur.executemany("INSERT INTO shipment VALUES(?, ?, ?, ?, ?)", theshipments)
    con.commit()

    tester = cur.execute("SELECT * FROM shipment")
    print(tester.fetchall())
    tester = cur.execute("SELECT * FROM product")
    print(tester.fetchall())
    con.close()


def getproduct(productlist, toget) -> int:
    found = -1
    count = 0
    for item in productlist:
        if item == toget:
            found = count
        count = count + 1
    return found


rows = []
rows1 = []
rows2 = []
products = []
shipments = []
ids = 0
with open('data/shipping_data_0.csv',newline='')as csvfile:
    reader = csv.reader(csvfile,delimiter=',', quotechar='|')
    for row in reader:
        rows.append(row)
    # get products list
    num = 0
    for i in rows:
        if num != 0:
            if not (products.__contains__(i.__getitem__(2))):
                # print("wow, it's " + i.__getitem__(2))
                products.append(i.__getitem__(2))
        num = num + 1
    # trim unneeded datapoints from the rows
    for row in rows:
        if ids != 0:
            id = ids
            prodid = getproduct(products, row.__getitem__(2))
            quantity = row.__getitem__(4)
            origin = row.__getitem__(0)
            destination = row.__getitem__(1)
            temp = (id, prodid, quantity, origin, destination)
            shipments.append(temp)
        ids = ids + 1
    # print(shipments)

with open('data/shipping_data_1.csv',newline='')as csvfile:
    reader1 = csv.reader(csvfile,delimiter=',', quotechar='|')
    for row1 in reader1:
        rows1.append(row1)
    num = 0
    for i in rows1:
        if num != 0:
            if not (products.__contains__(i.__getitem__(1))):
                # print("wow, it's " + i.__getitem__(2))
                products.append(i.__getitem__(1))
        num = num + 1

with open('data/shipping_data_2.csv',newline='')as csvfile:
    reader2 = csv.reader(csvfile,delimiter=',', quotechar='|')
    for row2 in reader2:
        rows2.append(row2)


# need to find the row in row1 that corresponds to the row in row2
ids2 = 0
for row1 in rows1:
    if ids2 != 0:
        for row2 in rows2:
            if row1.__getitem__(0) == row2.__getitem__(0):
                # Combine the data
                id1 = ids
                prodid = getproduct(products, row1.__getitem__(1))
                quantity = 1
                origin = row2.__getitem__(1)
                destination = row2.__getitem__(2)
                temp = (id1, prodid, quantity, origin, destination)
                # print(temp)
                shipments.append(temp)
    ids2 = ids2 + 1
    ids = ids + 1

# print(products)
# we need to format out data into tuples to insert into the database.
# to do with we need to surround out existing data with () and join the data points together
# should look like: [(1, 2, 3), (1, 2, 3),]

goodproducts = []
number = 0
for product in products:
    temps = (number, product)
    # print(temps)
    goodproducts.append(temps)
    number = number + 1
# print(shipments)
databaseing(goodproducts, shipments)
