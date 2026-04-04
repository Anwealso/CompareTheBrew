from scripts import classItem
from scripts.databaseHandler import *


def func():
    drinks = list()
    common_kwargs = {
        "link": "https://google.com",
        "ml": 700,
        "percent": 50,
        "pack_qty": 1,
        "score": None,
        "image": "https://google.com/image.png",
        "promotion": False,
        "old_price": 0,
    }

    a = classItem.Item('bws', 'brand1', 'a', 'vodka', 50, **common_kwargs, std_drinks=22)
    b = classItem.Item('bws', 'brand2', 'b', 'whiskey', 234, **common_kwargs, std_drinks=123)
    c = classItem.Item('bws', 'brand3', 'c', 'vodka', 1, **common_kwargs, std_drinks=50)
    d = classItem.Item('bws', 'brand4', 'd', 'vodka', 788764, **common_kwargs, std_drinks=32)
    e = classItem.Item('bws', 'brand5', 'e', 'vodka', 3234, **common_kwargs, std_drinks=2)
    f = classItem.Item('123', 'brand5', 'e', 'vodka', 3234, **common_kwargs, std_drinks=2)

    drinks.append(a)
    drinks.append(b)
    drinks.append(c)
    drinks.append(d)
    drinks.append(e)

    conn = None
    try:
        conn = sqlite3.connect("database.db")
        print(sqlite3.version)
    except Error as e:
        print(e)

    dbhandler(conn, drinks, 'p')

    select_all_drinks(conn)

    print("---")

    update_drink(conn, drinks[0], 999999999)

    select_all_drinks_by_score(conn)

    print(is_drink_in_table(conn, b))
    print(is_drink_in_table(conn, f))

    newB = classItem.Item('bws', 'brand2', 'b', 'whiskey', -10, 'https://google.com', 700, 50, 123, 123/50)

    update_drink(conn, newB, newB.price)

    print("---")

    select_all_drinks(conn)

    delete_all(conn)


if __name__ == "__main__":
    func()
